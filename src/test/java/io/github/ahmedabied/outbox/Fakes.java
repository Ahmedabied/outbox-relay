package io.github.ahmedabied.outbox;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** In-memory stand-ins for Postgres and Kafka, with the failures the relay has to survive. */
final class Fakes {
    private Fakes() {
    }

    /** One database that several relays can share, like the real outbox table and its advisory lock. */
    static final class Db {
        final TreeMap<Long, OutboxRow> rows = new TreeMap<>();
        final Set<Long> sent = new HashSet<>();
        Object leader;

        Db insert(int count, int keys) {
            long next = rows.isEmpty() ? 1 : rows.lastKey() + 1;
            for (int i = 0; i < count; i++, next++) {
                rows.put(next, new OutboxRow(next, "orders", "k" + (next % keys), new byte[] {(byte) next},
                        Map.of("type", "OrderPlaced"), Instant.now()));
            }
            return this;
        }
    }

    static final class Store implements OutboxStore {
        final Db db;
        int failMarks;
        int failLeads; // tryLead throws, as when the table is missing or the database is down
        int fetches;

        Store(Db db) {
            this.db = db;
        }

        @Override
        public boolean tryLead() throws SQLException {
            if (failLeads > 0) {
                failLeads--;
                throw new SQLException("relation \"outbox\" does not exist");
            }
            synchronized (db) {
                if (db.leader == null || db.leader == this) {
                    db.leader = this;
                    return true;
                }
                return false;
            }
        }

        @Override
        public List<OutboxRow> fetchUnsent(int limit) {
            fetches++;
            synchronized (db) {
                List<OutboxRow> out = new ArrayList<>();
                for (OutboxRow r : db.rows.values()) {
                    if (out.size() == limit) {
                        break;
                    }
                    if (!db.sent.contains(r.id())) {
                        out.add(r);
                    }
                }
                return out;
            }
        }

        @Override
        public void markSentAfterReconnect(List<OutboxRow> rows) throws SQLException {
            markSent(rows);
        }

        @Override
        public void markSent(List<OutboxRow> rows) throws SQLException {
            if (failMarks > 0) {
                failMarks--;
                throw new SQLException("connection lost");
            }
            synchronized (db) {
                db.sent.addAll(ids(rows));
            }
        }

        @Override
        public void awaitWork(Duration timeout) {
        }

        @Override
        public void reset() {
            close();
        }

        @Override
        public void close() {
            synchronized (db) {
                if (db.leader == this) {
                    db.leader = null; // the advisory lock goes with the connection
                }
            }
        }
    }

    /** A Kafka that commits a batch whole or not at all. {@code visible} is what read_committed consumers see. */
    static final class Kafka implements Publisher {
        final List<OutboxRow> visible;
        final Set<Long> poison = new HashSet<>();
        boolean namesTheRow = true;
        int retriableFailures;
        boolean fenced;
        final List<Integer> batchSizes = new ArrayList<>();

        Kafka() {
            this(new ArrayList<>());
        }

        Kafka(List<OutboxRow> shared) {
            this.visible = shared;
        }

        @Override
        public void publish(List<OutboxRow> rows) throws PublishException {
            batchSizes.add(rows.size());
            if (fenced) {
                throw new PublishException(PublishException.Kind.FENCED, null, "fenced", null);
            }
            if (retriableFailures > 0) {
                retriableFailures--;
                throw new PublishException(PublishException.Kind.RETRIABLE, null, "broker not available", null);
            }
            for (OutboxRow r : rows) {
                if (poison.contains(r.id())) {
                    throw new PublishException(PublishException.Kind.REJECTED, namesTheRow ? r.id() : null,
                            "RecordTooLargeException", null);
                }
            }
            synchronized (visible) {
                visible.addAll(rows);
            }
        }

        @Override
        public void close() {
        }
    }

    static Relay.Settings fast(int batch) {
        return new Relay.Settings(batch, 3, Duration.ofMillis(1), Duration.ofMillis(1), Duration.ofMillis(1),
                Duration.ofMillis(2));
    }

    static List<Long> ids(List<OutboxRow> rows) {
        List<Long> out = new ArrayList<>(rows.size());
        rows.forEach(r -> out.add(r.id()));
        return out;
    }

    static List<Long> range(long from, long to) {
        List<Long> out = new ArrayList<>();
        for (long i = from; i <= to; i++) {
            out.add(i);
        }
        return out;
    }
}
