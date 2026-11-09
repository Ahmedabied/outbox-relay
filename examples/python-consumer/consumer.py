"""A Python consumer that applies each outbox event once, even though Kafka may deliver it twice.

It reads with isolation.level=read_committed, so it sees only batches the relay committed. For each batch it opens
one Postgres transaction: every record's outbox-id is claimed in outbox_inbox (insert ... on conflict do nothing),
and only a new claim applies the event. Postgres commits first, then the Kafka offsets. A crash between the two
makes Kafka deliver the batch again; its ids are already claimed, so it is skipped instead of applied twice.

    pip install -r requirements.txt
    python consumer.py --db postgresql://app@localhost/app --kafka localhost:9092 --topic orders --name billing

The event applied here is a running total per order (order_totals), so a double count would show in the sums.
Outbox ids are unique per outbox table: give each source outbox its own --name (billing-orders, billing-payments).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, KafkaError

ID_HEADER = "outbox-id"
INBOX_SQL = Path(__file__).resolve().parents[2] / "src" / "main" / "resources" / "inbox.sql"

TOTALS_SQL = """
create table if not exists order_totals (
    consumer text    not null,
    order_id text    not null,
    events   integer not null,
    amount   numeric not null,
    primary key (consumer, order_id)
)"""

APPLY = """
insert into order_totals (consumer, order_id, events, amount) values (%s, %s, 1, %s)
on conflict (consumer, order_id) do update
   set events = order_totals.events + 1, amount = order_totals.amount + excluded.amount"""


def outbox_id(headers: list[tuple[str, bytes]] | None) -> int:
    for key, value in headers or []:
        if key == ID_HEADER:
            return int(value)
    raise ValueError(f"record without an {ID_HEADER} header: not from the relay")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--db", required=True, help="Postgres connection string")
    p.add_argument("--kafka", required=True, help="bootstrap servers")
    p.add_argument("--topic", required=True)
    p.add_argument("--name", required=True, help="consumer name: Kafka group id and inbox key")
    p.add_argument("--no-inbox", action="store_true", help="apply every delivery (to show what the inbox prevents)")
    p.add_argument("--stop-when-idle", type=float, default=0, metavar="S", help="exit after S seconds with no records")
    p.add_argument("--crash-after-batches", type=int, default=0, metavar="N",
                   help="die after the Nth batch is in Postgres, before its offsets reach Kafka")
    a = p.parse_args(argv)

    consumer = Consumer({
        "bootstrap.servers": a.kafka,
        "group.id": a.name,
        "isolation.level": "read_committed",   # never read a batch the relay aborted
        "enable.auto.commit": False,           # offsets only after Postgres has the batch
        "auto.offset.reset": "earliest",
        "session.timeout.ms": 6000,            # a crashed member's partitions move on after 6 s, not 45
    })
    consumer.subscribe([a.topic])
    applied = skipped = batches = 0
    started = last = time.monotonic()
    first = None
    with psycopg.connect(a.db, autocommit=True) as db:
        db.execute(INBOX_SQL.read_text())
        db.execute(TOTALS_SQL)
        try:
            while True:
                records = consumer.consume(num_messages=500, timeout=0.5)
                records = [r for r in records if not _skip_error(r)]
                if not records:
                    waited = time.monotonic() - (last if first else started)
                    if a.stop_when_idle and waited > a.stop_when_idle + (0 if first else 30):
                        break  # done; before the first record, allow 30 s more for the group to form
                    continue
                first = first or time.monotonic()
                with db.transaction():
                    for r in records:
                        if not a.no_inbox:
                            claimed = db.execute(
                                "insert into outbox_inbox (consumer, outbox_id) values (%s, %s) on conflict do nothing",
                                (a.name, outbox_id(r.headers()))).rowcount == 1
                            if not claimed:
                                skipped += 1
                                continue
                        event = json.loads(r.value())
                        db.execute(APPLY, (a.name, event["order"], event["amount"]))
                        applied += 1
                batches += 1
                if batches == a.crash_after_batches:
                    print(f"{a.name}: crashed after batch {batches} was in Postgres, before its offsets reached Kafka",
                          flush=True)
                    os._exit(3)  # no cleanup, no offset commit: as a killed process would
                consumer.commit(asynchronous=False)
                last = time.monotonic()
        finally:
            consumer.close()
    rate = (applied + skipped) / (last - first) if first and last > first else 0
    print(f"{a.name}: applied {applied}, skipped {skipped} repeats, {batches} batches, {rate:,.0f} records/s",
          flush=True)
    return 0


def _skip_error(record) -> bool:
    err = record.error()
    if err is None:
        return False
    if err.code() == KafkaError._PARTITION_EOF:
        return True
    raise RuntimeError(f"Kafka: {err}")


if __name__ == "__main__":
    sys.exit(main())
