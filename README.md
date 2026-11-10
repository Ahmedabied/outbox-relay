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

`init` can run on every deploy, on a busy table too. A run that finds everything in place takes no lock on the
table. Plain `create index if not exists` and `drop trigger if exists` do: both lock the table before they look, so
behind one long transaction they wait, and every insert waits behind them. That was found by running the schema file
through [pg-migration-check](https://github.com/Ahmedabied/pg-migration-check) on a table of 500,000 rows. With one
transaction holding an uncommitted insert, the old `init` was still waiting when it was cancelled after 4 s, and a second
insert gave up at its 2 s lock timeout; now `init` returns at once and the insert goes through. When the table does need
a change (an upgrade from an older version), `init` waits 3 s at most for the lock, then stops with nothing changed
and says so.

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
| One relay at a time | The relay holds a Postgres session advisory lock on its own connection, keyed on the table itself (schema and name), so replicas take turns whatever else differs between them. If that connection dies, the lock goes with it and a standby takes over. The Kafka `transactional.id` is a second fence, when replicas share it (the default): a relay that still thinks it leads is refused by Kafka. |
| A bad row is not skipped | A row Kafka refuses for good (too large, say) is narrowed down, everything before it goes out, then the relay stops and names the row. Skipping it would deliver later events for the same key without it. |
| Outages are waited out | Broker or network trouble is retried with backoff, forever, and the database connection is rebuilt. The wait doubles from 100 ms to 10 s while rounds keep failing and drops back to 100 ms after the first clean round (a batch marked sent, an empty outbox, a standby round), so the next outage starts short. A batch Kafka already has when the database goes is kept in memory and marked first once the database is back, so a lost connection repeats nothing. If marking keeps failing (a permission, say), the relay keeps retrying it and publishes nothing new, rather than sending the same batch again and again. It reads nothing meanwhile, so `last_fetch_timestamp_seconds` goes stale and the OutboxRelayStalled alert fires. |
| Low lag without polling hard | A statement trigger sends `NOTIFY` on insert, delivered at commit; the relay wakes at once and only polls once a second as a safety net. |

The one thing it cannot do is exactly once: if the relay process dies after Kafka commits and before the database
marks the rows, that batch is published again by the relay that comes next. A row is marked by its id and creation
time, so a batch kept in memory can never mark a different row that reused the id. Every record carries an `outbox-id` header, and a consumer that writes to
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
copy without the inbox, and checks the sums. Kafka 4.4 and Postgres 18, the same result on a desktop and in CI:

| 10,000 events | applied | counted twice | records/s, desktop | records/s, CI runner |
|---|---|---|---|---|
| with the inbox | 10,000 | 0 (500 repeats skipped) | 14,456 | 3,792 |
| without it | 10,500 | 500 | 24,300 | 7,422 |

The inbox costs one insert per record in the consumer's transaction: about 40 to 50% of a consumer that only adds
up numbers, much less next to real work.

```bash
pip install -r examples/python-consumer/requirements.txt
python examples/python-consumer/demo.py --db postgresql://app@localhost/app --kafka localhost:9092   # relay running
# with OUTBOX_TABLE set, pass the same name: --table app.orders_outbox
```

## Metrics

Set `OUTBOX_METRICS_ADDR=9464` and the relay serves Prometheus metrics at `http://127.0.0.1:9464/metrics` (a bare
port binds to localhost; give `0.0.0.0:9464` to expose it). Off when unset. The endpoint reads the relay's own
counters, never th`outbox_relay_published_total`, `batches_total`, `retries_total`, `rejections_total`, `database_errors_total`,
`leading` (1 on the relay that publishes), `lag_seconds{quantile="0.5"|"0.99"}` (insert to Kafka commit),
`oldest_unsent_age_seconds` (age of the first unsent row at the relay's last read of the outbox) and
`last_fetch_timestamp_seconds` (when that read was).

Alert rules to start from: [`examples/prometheus/alerts.yml`](examples/prometheus/alerts.yml). CI runs their unit
tests (`promtool test rules alerts.test.yml`, one case per failure below), and a Java test checks that every metric
name in the rules is one the relay serves. CI then runs the rules against a real relay that Prometheus scrapes
([`check_live.py`](examples/prometheus/check_live.py)): quiet while healthy; with Kafka stopped, OutboxStuck must go
pending and OutboxRelayStalled must stay quiet for 150 s; with the relay stopped, OutboxNoLeader must go pending. In
CI, OutboxStuck went pending 68 s after the write and OutboxNoLeader 14 s after the stop. Over a 4 minute outage
(local, Kafka 4.4, 5 s scrapes) the relay read every 15 s at first (the producer waits 15 s for the broker), then
every 25 s once its backoff reached 10 s; in two runs Prometheus never saw a read older than 29 s, against the 60 s
of OutboxRelayStalled.

| Alert | Fires when | |
|---|---|---|
| OutboxStuck | the first unsent row was over 60 s old at the last read, for 2 min | page |
| OutboxRelayStalled | no relay has read the outbox for over 60 s, for 2 min | page |
| OutboxNoLeader | no relay leads, for 2 min | page |
| OutboxRowRefused | Kafka refused a batch in the last 15 min | ticket |
| OutboxDatabaseErrors | the database connection keeps dropping, for 10 min | ticket |
| OutboxLagHigh | p99 insert to Kafka commit is over 5 s, for 10 min | ticket |

The first two cover each other. While Kafka is down the relay retries and reads again each time, so the age grows
and OutboxStuck fires. While marking a sent batch keeps failing, the relay retries the mark and reads nothing new,
so the age stands still at its last value and only the stale read time shows it (both cases are tests in
`MetricsServerTest`). A row Kafka refuses for good stops the relay with exit code 1 and the row named in its log
(a 2 MB row: 2 s after the relay took the lead, having published the row before it). Every replica stops on the
same row, so that pages as OutboxNoLeader. Under a restart policy the relay starts, is refused and exits again in a
loop: alert on its restarts too, since a scrape that catches a start leading resets OutboxNoLeader's 2 minutes.

g while Kafka is down and the relay retries.

## Measured

From CI (GitHub's shared runners, 2 CPUs, Postgres and a single Kafka 4.4 broker in containers), runs on
Postgres 14 and 18, every number from the integration tests in `KafkaRelayIT`:

| What | Result |
|---|---|
| A backlog of 50,000 events, 100 keys, 6 partitions, batch 500 | 32,000 to 53,000 events/s (median about 34,800), 100 Kafka transactions |
| Steady traffic, 1,000 events/s, one app transaction per event | insert to consumer p50 3.6 to 13.6 ms; p99 57 to 102 ms in 7 of 8 measurements, 461 ms once |
| Relay JVM killed with SIGKILL 5 times mid-stream (12,000 to 36,000 rows unsent at each kill), restarted each time | 0 lost, order per key kept, 0 or 200 repeats (up to 0.5%) |
| Database connection lost right after a Kafka commit, before the mark | 0 repeats, 0 lost: the same relay connects again and marks the batch it kept |
| Relay process dead right after a Kafka commit, before the mark; the next relay takes over | that batch arrives twice (500 repeats), nothing else; 0 lost; a consumer using `Inbox` handles each of the 5,000 events once |

The steady tail is the Kafka commit itself (the relay's own insert to commit p99 is 61 to 99 ms); a consumer with
`fetch.max.wait.ms=50` measured the same. The SIGKILL test mostly lands between batches; the last two tests force the
one window where a repeat can happen, so both the kept batch and the dedupe are shown, not assumed.

## Tests

```bash
mvn -B verify      # 54 tests: unit, then Postgres and Kafka integration tests in containers
```

- **Unit** (26, in-memory Postgres and Kafka stand-ins): order, retries and their waits, the bad-row search, a lost connection and a
  dead process between Kafka and the database, a mark that keeps failing, trouble before the lead logged once (a
  missing table is said, not retried in silence), leader handover, fencing, the reserved `outbox-id` header, the metrics endpoint over real HTTP,
  table names (what is refused, and that the default keeps the old object names).
- **Postgres** (21, `OUTBOX_TEST_DB_URL` or a container): the real queries, `init` and the inbox install run again behind an open
  transaction (no lock, no wait), an upgrade that cannot get the table (gives up after 3 s, nothing changed), the advisory lock (one lead per table,
  however it is named, kept when the table is made again), `NOTIFY` wake-up, a
  late commit with a lower id, a batch kept across a reconnect marks only its own rows (never a reused id), headers the schema refuses
  (and an old row that breaks the check can still be marked sent), a second table in its own schema (its own rows, lead and wake-ups), and the inbox (once per consumer, a rollback frees the
  id, two instances racing for one id get one yes).
- **Kafka 4.4 + Postgres** (7, containers, skipped without Docker): the backlog, steady traffic, a 2 MB row the
  producer refuses, fencing between two publishers, the SIGKILL test (the relay runs as its own JVM from the built
  jar), a lost connection after a Kafka commit (no repeat) and a death there (one batch repeated, run through
  `Inbox`).

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

## Related

- [pg-migration-check](https://github.com/Ahmedabied/pg-migration-check) reports the locks a schema change takes
  and what they block. It is how the locks in `init` were found (see Run it).
- [valuation-loop](https://github.com/Ahmedabied/valuation-loop) uses the same pattern inside a Spring Boot
  service: the state change and its events in one transaction, a relay to Kafka.

Built with AI pair programming.

## Licence

MIT
