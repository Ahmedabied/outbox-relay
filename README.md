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
java -jar target/outbox-relay.jar run        # SIGTERM finishes the batch in hand, then exits
```

Consumers read with `isolation.level=read_committed` and drop a repeat by the `outbox-id` header (see below).

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
rows, that batch is published again. Every record carries an `outbox-id` header so consumers can drop the repeat
(keep processed ids in a table with a unique key, in the same transaction as the consumer's own write).

## Tests

```bash
mvn -B verify      # unit tests, then Postgres and Kafka integration tests in containers
```

- **Unit** (in-memory Postgres and Kafka stand-ins): order, retries, the bad-row search, a crash between Kafka and
  the database, leader handover, fencing.
- **Postgres** (`OUTBOX_TEST_DB_URL` or a container): the real queries, the advisory lock, `NOTIFY` wake-up, a late
  commit with a lower id.
- **Kafka 4.4 + Postgres** (containers): a 50,000-row backlog over 6 partitions, steady traffic at 1,000 events/s,
  a 2 MB row the producer refuses, fencing between two publishers, and the crash test: the relay runs as its own JVM
  from the built jar and is killed with SIGKILL three times mid-stream.

CI runs everything on Postgres 14 and 18.

## Limits

- Order holds per key for rows that commit in id order. Two transactions that write the same key at the same time,
  without locking a shared row, have no defined order to keep: lock the aggregate row, as you would anyway.
- Consumers that read with `read_uncommitted` (the Kafka default) can see aborted batches. Use `read_committed`.
- Sent rows stay in the table with `sent_at` set. Delete old ones on a schedule:
  `delete from outbox where sent_at < now() - interval '7 days'`.
- The table name is fixed (`outbox`) and the relay works one table per process.
- Lag numbers depend on the machine; the ones here come from GitHub's shared CI runners.

Built with AI pair programming.

## Licence

MIT
