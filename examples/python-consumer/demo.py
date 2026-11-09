"""The consumer's promise, measured: events written to the outbox, relayed, and applied once despite a crash.

Needs Postgres with the outbox table, Kafka, and the relay running (README, "Run it"). Then:

    python demo.py --db postgresql://app@localhost/app --kafka localhost:9092

It inserts --events order events, waits for the relay to send them, and runs two consumers through the same
crash: each dies after its second batch is in Postgres but before Kafka has the offsets, then restarts. One uses
the inbox, one does not. Exit 0 only if the inbox one matches the outbox exactly and the crash really caused
repeats (the one without the inbox counted some events twice).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
import uuid
from pathlib import Path

import psycopg

HERE = Path(__file__).resolve().parent


def run_consumer(args: argparse.Namespace, topic: str, name: str, *extra: str) -> tuple[int, str]:
    cmd = [sys.executable, str(HERE / "consumer.py"), "--db", args.db, "--kafka", args.kafka,
           "--topic", topic, "--name", name, *extra]
    done = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    out = (done.stdout + done.stderr).strip()
    print("  " + out.replace("\n", "\n  "))
    return done.returncode, out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--db", required=True)
    p.add_argument("--kafka", required=True)
    p.add_argument("--events", type=int, default=10_000)
    args = p.parse_args()
    run = uuid.uuid4().hex[:8]
    topic = f"orders-demo-{run}"  # a fresh topic and consumer names, so the demo can run again on the same setup

    with psycopg.connect(args.db, autocommit=True) as db:
        first, last = db.execute(
            """with ins as (
                 insert into outbox (topic, msg_key, payload)
                 select %s, 'o-' || (g %% 100),
                        convert_to(json_build_object('order', 'o-' || (g %% 100), 'amount', (g %% 50) + 0.25)::text,
                                   'UTF8')
                   from generate_series(1, %s) g
                 returning id)
               select min(id), max(id) from ins""", (topic, args.events)).fetchone()
        want_amount = db.execute("select sum((g %% 50) + 0.25) from generate_series(1, %s) g",
                                 (args.events,)).fetchone()[0]
        print(f"{args.events:,} events written to the outbox (topic {topic}), waiting for the relay")
        t0 = time.monotonic()
        while db.execute("select count(*) from outbox where id between %s and %s and sent_at is null",
                         (first, last)).fetchone()[0]:
            if time.monotonic() - t0 > 120:
                print("the relay did not send them within 120 s: is it running?")
                return 1
            time.sleep(0.2)
        print(f"relayed in {time.monotonic() - t0:.1f} s")

        results = {}
        for label, extra in (("with inbox", ()), ("without inbox", ("--no-inbox",))):
            name = f"{'inbox' if not extra else 'no-inbox'}-{run}"
            print(f"{label} ({name}):")
            code, _ = run_consumer(args, topic, name, "--crash-after-batches", "2", *extra)
            if code != 3:
                print(f"  expected the planned crash (exit 3), got exit {code}")
                return 1
            code, _ = run_consumer(args, topic, name, "--stop-when-idle", "3", *extra)
            if code != 0:
                return 1
            events, amount = db.execute(
                "select coalesce(sum(events), 0), coalesce(sum(amount), 0) from order_totals where consumer = %s",
                (name,)).fetchone()
            results[label] = (events, amount)

    print()
    print(f"{'':16}{'events applied':>16}{'amount':>16}{'counted twice':>16}")
    print(f"{'outbox':16}{args.events:>16,}{want_amount:>16,}{'':>16}")
    for label, (events, amount) in results.items():
        print(f"{label:16}{events:>16,}{amount:>16,}{events - args.events:>16,}")
    inbox_ok = results["with inbox"] == (args.events, want_amount)
    crash_repeated = results["without inbox"][0] > args.events
    if not crash_repeated:
        print("the crash caused no repeats this time, so it proved nothing: run again")
    return 0 if inbox_ok and crash_repeated else 1


if __name__ == "__main__":
    sys.exit(main())
