# outbox-relay

Your service saves an order and must tell Kafka about it. Two writes, two systems: if one fails, the database and
Kafka disagree. The transactional outbox fixes that, and this is a small, tested relay for it, in Java 21.

The app writes the event into an `outbox` table **in the same transaction** as its own data. The relay moves the rows
to Kafka without losing one, without reordering a key, and with only one relay publishing at a time.

```sql
begin;
insert into orders (id, total) values (42, 12.50);
insert into outbox (topic, msg_key, payload, headers)
values ('orders', 'order-42', convert_to('{"id": 42, "total": 12.50}', 'UTF8'), '{"type": "OrderPlaced"}');
commit;
```

## Run it

Java 21 and Maven (the integration tests need Docker):

```bash
mvn -B package -DskipITs                     # target/outbox-relay.jar
export OUTBOX_DB_URL=jdbc:postgresql://localhost:5432/app OUTBOX_DB_USER=app OUTBOX_DB_PASSWORD=...
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
java -jar target/outbox-relay.jar init       # outbox table, index, notify trigger (safe to run again)
java -jar target/outbox-relay.jar run        # SIGTERM lets the batch in hand finish, then exits
```

Consumers read with `isolation.level=read_committed` and drop a repeat by its `outbox-id` header (`Inbox`, below).

Another table name, or a table in its own schema: set `OUTBOX_TABLE=app.orders_outbox` for both `init` and `run`.
Its index, header check, trigger, notify channel and lead get names of their own, so several outboxes in one
database each have a relay that only wakes for its own inserts. Names are lower case letters, digits and `_`, and
are quoted in every statement; anything else is refused before it reaches SQL. The default `outbox` keeps the object
names of earlier versions, so an existing install carries on as it is.

## How it keeps its promises

| Promise | How |
|---|---|
| Nothing lost | A row is marked sent only after Kafka has committed it. No high-water mark: a row that commits late with a lower id is still picked up (tested). |
| Order per key | Rows go out in id order, and each batch is **one Kafka transaction** with an idempotent producer (`acks=all`). A failed batch is aborted, so `read_committed` consumers never see half of it and a retry cannot reorder. |
| One relay at a time | The relay holds a Postgres session advisory lock on its own connection. If that connection dies, the lock goes with it and a standby takes over. The Kafka `transactional.id` is a second fence: a relay that still thinks it leads is refused by Kafka. |
| A bad row is not skipped | A row Kafka refuses for good (too large, say) is narrowed down, everything before it goes out, then the relay stops and names the row. Skipping it would deliver later events for the same key without it. |
| Outages are waited out | Broker or network trouble is retried with backoff, forever, and the database connection is rebuilt. |
| Low lag without polling hard | A statement trigger sends `NOTIFY` on insert, delivered at commit; the relay wakes at once and only polls once a second as a safety net. |

The one thing it cannot do is exactly once: if the relay dies after Kafka commits and before the database marks the
rows, that batch is published again. Every record carries an `outbox-id` header, and a consumer that writes to
Postgres can use `Inbox` to handle each event once: it records the id in the same transaction as the consumer's own
write, so a repeat is skipped and a rolled-back write frees the id again.

```java
conn.setAutoCommit(false);
if (Inbox.claim(conn, "billing", Inbox.outboxId(record.headers()))) {
    // the consumer's own writes, on the same connection
}
conn.commit();
```

`Inbox.install(conn)` creates its table. A claim with autocommit on is refused, because it would commit on its own.
Outbox ids are unique per outbox table, so a consumer that reads from two outboxes uses one consumer name for each
(`billing-orders`, `billing-payments`).

### The same from Python

[examples/python-consumer](examples/python-consumer) is a consumer in Python (confluent-kafka, psycopg 3) that does
the same thing per batch: one Postgres transaction claims each `outbox-id` and applies only new ones, then the Kafka
offsets are committed. `demo.py` runs it against a live relay through a crash between those two commits, next to a
copy without the inbox, and checks the sums (Kafka 4.4 and Postgres 18 on this desktop; CI runs the same):

| 10,000 events | applied | counted twice | records/s |
|---|---|---|---|
| with the inbox | 10,000 | 0 (500 repeats skipped) | 14,456 |
| without it | 10,500 | 500 | 24,300 |

```bash
pip install -r examples/python-consumer/requirements.txt
python examples/python-consumer/demo.py --db postgresql://app@localhost/app --kafka localhost:9092   # relay running
```

## Metrics

Set `OUTBOX_METRICS_ADDR=9464` and the relay serves Prometheus metrics at `http://127.0.0.1:9464/metrics` (a bare
port binds to localhost; give `0.0.0.0:9464` to expose it). Off when unset. The endpoint reads the relay's own
counters, never the database, and answers only `GET /metrics`.

`outbox_relay_published_total`, `batches_total`, `retries_total`, `rejections_total`, `database_errors_total`,
`leading` (1 on the relay that publishes), `lag_seconds{quantile="0.5"|"0.99"}` (insert to Kafka commit), and the
one to page on:

```yaml
- alert: OutboxStuck
  expr: max(outbox_relay_oldest_unsent_age_seconds) > 60 or absent(outbox_relay_leading == 1)
  for: 2m
  annotations:
    summary: "Events are waiting in the outbox (broker down, a refused row, or no relay leading)"
```

`oldest_unsent_age_seconds` is the age of the first unsent row at the relay's last read, recorded in its own loop,
so it keeps growing while Kafka is down and the relay retries.

## Measured

From CI (GitHub's shared runners, 2 CPUs, Postgres and a single Kafka 4.4 broker in containers), runs on
Postgres 14 and 18, every number from the integration tests in `KafkaRelayIT`:

| What | Result |
|---|---|
| A backlog of 50,000 events, 100 keys, 6 partitions, batch 500 | 32,000 to 53,000 events/s (median about 34,800), 100 Kafka transactions |
| Steady traffic, 1,000 events/s, one app transaction per event | insert to consumer p50 3.6 to 13.6 ms; p99 57 to 102 ms in 7 of 8 measurements, 461 ms once |
| Relay JVM killed with SIGKILL 5 times mid-stream (12,000 to 36,000 rows unsent at each kill), restarted each time | 0 lost, order per key kept, 0 or 200 repeats (up to 0.5%) |
| Database connection lost right after a Kafka commit, before the mark | that batch arrives twice (500 repeats), nothing else; 0 lost; a consumer using `Inbox` handles each of the 5,000 events once |

The steady tail is the Kafka commit itself (the relay's own insert to commit p99 is 61 to 99 ms); a consumer with
`fetch.max.wait.ms=50` measured the same. The SIGKILL test mostly lands between batches; the second test forces the
one window where a repeat can happen, so the dedupe is shown, not assumed.

## Tests

```bash
mvn -B verify      # 38 tests: unit, then Postgres and Kafka integration tests in containers
```

- **Unit** (17, in-memory Postgres and Kafka stand-ins): order, retries, the bad-row search, a crash between Kafka
  and the database, leader handover, fencing, the reserved `outbox-id` header, the metrics endpoint over real HTTP,
  table names (what is refused, and that the default keeps the old object names).
- **Postgres** (15, `OUTBOX_TEST_DB_URL` or a container): the real queries, the advisory lock, `NOTIFY` wake-up, a
  late commit with a lower id, headers the schema refuses (and an old row that breaks the check can still be marked
  sent), a second table in its own schema (its own rows, lead and wake-ups), and the inbox (once per consumer, a rollback frees the
  id, two instances racing for one id get one yes).
- **Kafka 4.4 + Postgres** (6, containers, skipped without Docker): the backlog, steady traffic, a 2 MB row the
  producer refuses, fencing between two publishers, the SIGKILL test (the relay runs as its own JVM from the built
  jar) and the forced repeat, run through `Inbox`.

CI runs everything on Postgres 14 and 18, then the Python consumer demo against the built relay, Kafka 4.4 and
Postgres 18.

## Limits

- Order holds per key for rows that commit in id order. Two transactions that write the same key at the same time,
  without locking a shared row, have no defined order to keep: lock the aggregate row, as you would anyway.
- Consumers that read with `read_uncommitted` (the Kafka default) can see aborted batches. Use `read_committed`.
- Sent rows stay in the table with `sent_at` set, and inbox rows stay too. Delete old ones on a schedule:
  `delete from outbox where sent_at < now() - interval '7 days'`; keep inbox rows longer than a relay could be down.
- One table per relay process: several outboxes need one relay each, with its own `OUTBOX_TABLE`.
- Lag numbers depend on the machine; the ones here come from GitHub's shared CI runners.

Built with AI pair programming.

## Licence

MIT
