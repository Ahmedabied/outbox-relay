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

## Measured

From CI (GitHub's shared runners, 2 CPUs, Postgres and a single Kafka 4.4 broker in containers), three runs on
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
mvn -B verify      # 29 tests: unit, then Postgres and Kafka integration tests in containers
```

- **Unit** (10, in-memory Postgres and Kafka stand-ins): order, retries, the bad-row search, a crash between Kafka
  and the database, leader handover, fencing, the reserved `outbox-id` header.
- **Postgres** (13, `OUTBOX_TEST_DB_URL` or a container): the real queries, the advisory lock, `NOTIFY` wake-up, a
  late commit with a lower id, headers the schema refuses, and the inbox (once per consumer, a rollback frees the
  id, two instances racing for one id get one yes).
- **Kafka 4.4 + Postgres** (6, containers, skipped without Docker): the backlog, steady traffic, a 2 MB row the
  producer refuses, fencing between two publishers, the SIGKILL test (the relay runs as its own JVM from the built
  jar) and the forced repeat, run through `Inbox`.

CI runs everything on Postgres 14 and 18.

## Limits

- Order holds per key for rows that commit in id order. Two transactions that write the same key at the same time,
  without locking a shared row, have no defined order to keep: lock the aggregate row, as you would anyway.
- Consumers that read with `read_uncommitted` (the Kafka default) can see aborted batches. Use `read_committed`.
- Sent rows stay in the table with `sent_at` set, and inbox rows stay too. Delete old ones on a schedule:
  `delete from outbox where sent_at < now() - interval '7 days'`; keep inbox rows longer than a relay could be down.
- The table name is fixed (`outbox`) and the relay works one table per process.
- Lag numbers depend on the machine; the ones here come from GitHub's shared CI runners.

Built with AI pair programming.

## Licence

MIT
