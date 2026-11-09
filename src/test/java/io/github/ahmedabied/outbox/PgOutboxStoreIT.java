package io.github.ahmedabied.outbox;

import static io.github.ahmedabied.outbox.Fakes.ids;
import static io.github.ahmedabied.outbox.Fakes.range;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PgOutboxStoreIT {

    @BeforeEach
    void freshTable() throws Exception {
        TestDb.fresh();
    }

    private static PgOutboxStore store() {
        return new PgOutboxStore(TestDb.url(), TestDb.props(), "test");
    }

    private static void sql(Connection c, String sql) throws Exception {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static void insert(Connection c, int n) throws Exception {
        sql(c, "insert into outbox (topic, msg_key, payload) select 'orders', 'k' || (g % 7),"
                + " convert_to('{\"n\": ' || g || '}', 'UTF8') from generate_series(1, " + n + ") g");
    }

    @Test
    void installIsSafeToRunAgain() throws Exception {
        try (Connection c = TestDb.connect()) {
            insert(c, 3);
            PgOutboxStore.install(c);
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select count(*) from outbox")) {
                rs.next();
                assertEquals(3, rs.getInt(1));
            }
        }
    }

    @Test
    void unsentRowsComeInIdOrderWithTheirHeaders() throws Exception {
        try (Connection c = TestDb.connect(); PgOutboxStore store = store()) {
            sql(c, "insert into outbox (topic, msg_key, payload, headers) values"
                    + " ('orders', 'o-1', convert_to('{\"total\": 12}', 'UTF8'), '{\"type\": \"OrderPlaced\"}'),"
                    + " ('orders', null, '\\x00ff'::bytea, '{}')");
            insert(c, 10);
            List<OutboxRow> rows = store.fetchUnsent(5);
            assertEquals(range(1, 5), ids(rows));
            assertEquals("o-1", rows.get(0).key());
            assertEquals(Map.of("type", "OrderPlaced"), rows.get(0).headers());
            assertArrayEquals("{\"total\": 12}".getBytes(StandardCharsets.UTF_8), rows.get(0).payload());
            assertNull(rows.get(1).key());
            assertArrayEquals(new byte[] {0, (byte) 0xff}, rows.get(1).payload());
            assertEquals(Map.of(), rows.get(1).headers());
            store.markSent(ids(rows));
            assertEquals(range(6, 12), ids(store.fetchUnsent(100)));
        }
    }

    @Test
    void theSchemaRefusesHeadersThatWouldStopTheRelayOrFakeTheId() throws Exception {
        try (Connection c = TestDb.connect()) {
            for (String bad : new String[] {"'[]'", "'\"text\"'", "'{\"outbox-id\": \"1\"}'"}) {
                var e = org.junit.jupiter.api.Assertions.assertThrows(java.sql.SQLException.class, () -> sql(c,
                        "insert into outbox (topic, payload, headers) values ('t', '\\x00', " + bad + ")"));
                assertTrue(e.getMessage().contains("outbox_headers_check"), e.getMessage());
            }
        }
    }

    @Test
    void aNullHeaderValueIsDroppedAndAnOldTableCannotJamTheRelay() throws Exception {
        try (Connection c = TestDb.connect(); PgOutboxStore store = store()) {
            sql(c, "insert into outbox (topic, payload, headers) values ('t', '\\x00', '{\"a\": null, \"b\": \"x\"}')");
            // A table from before the check, holding a row the check would refuse:
            sql(c, "alter table outbox drop constraint outbox_headers_check");
            sql(c, "insert into outbox (topic, payload, headers) values ('t', '\\x00', '[1, 2]')");
            List<OutboxRow> rows = store.fetchUnsent(10);
            assertEquals(Map.of("b", "x"), rows.get(0).headers());
            assertEquals(Map.of(), rows.get(1).headers());
            PgOutboxStore.install(c); // puts the check back, for new rows
            var e = org.junit.jupiter.api.Assertions.assertThrows(java.sql.SQLException.class, () -> sql(c,
                    "insert into outbox (topic, payload, headers) values ('t', '\\x00', '[]')"));
            assertTrue(e.getMessage().contains("outbox_headers_check"), e.getMessage());
        }
    }

    @Test
    void oneStoreLeadsAndTheLeadGoesWithItsConnection() throws Exception {
        try (PgOutboxStore a = store(); PgOutboxStore b = store()) {
            assertTrue(a.tryLead());
            assertTrue(a.tryLead()); // still ours
            assertFalse(b.tryLead());
            a.reset(); // connection closed: the session lock is released
            assertTrue(b.tryLead());
            assertFalse(a.tryLead());
        }
    }

    @Test
    void anInsertWakesAWaitingRelayAtOnce() throws Exception {
        try (Connection c = TestDb.connect(); PgOutboxStore store = store()) {
            store.fetchUnsent(1); // connects and listens
            long t0 = System.nanoTime();
            store.awaitWork(Duration.ofMillis(300));
            long idle = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(idle >= 250, "waited " + idle + " ms with nothing to do");

            Thread writer = new Thread(() -> {
                try {
                    Thread.sleep(100);
                    insert(c, 1);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            writer.start();
            t0 = System.nanoTime();
            store.awaitWork(Duration.ofSeconds(10));
            long woke = (System.nanoTime() - t0) / 1_000_000;
            writer.join();
            assertTrue(woke < 2_000, "woke after " + woke + " ms");
            assertEquals(1, store.fetchUnsent(10).size());
        }
    }

    @Test
    void anotherTableInItsOwnSchemaHasItsOwnRowsLeadAndWakeUps() throws Exception {
        OutboxTable orders = OutboxTable.parse("app.orders_outbox");
        try (Connection c = TestDb.connect()) {
            sql(c, "drop schema if exists app cascade");
            sql(c, "create schema app");
            try {
                PgOutboxStore.install(c, orders);
                PgOutboxStore.install(c, orders); // safe to run again
                try (PgOutboxStore plain = store();
                     PgOutboxStore other = new PgOutboxStore(TestDb.url(), TestDb.props(),
                             orders.defaultTransactionalId(), orders)) {
                    plain.fetchUnsent(1); // both connect and listen
                    other.fetchUnsent(1);
                    sql(c, "insert into app.orders_outbox (topic, payload) values ('orders', '\\x01')");
                    long t0 = System.nanoTime();
                    other.awaitWork(Duration.ofSeconds(10));
                    long woke = (System.nanoTime() - t0) / 1_000_000;
                    assertTrue(woke < 2_000, "the orders relay woke after " + woke + " ms");
                    t0 = System.nanoTime();
                    plain.awaitWork(Duration.ofMillis(300));
                    long idle = (System.nanoTime() - t0) / 1_000_000;
                    assertTrue(idle >= 250, "the default relay was woken by the other table (" + idle + " ms)");

                    assertTrue(plain.tryLead());
                    assertTrue(other.tryLead()); // a lead of its own, not the default table's
                    insert(c, 2);
                    assertEquals(List.of(1L, 2L), ids(plain.fetchUnsent(10)));
                    List<OutboxRow> mine = other.fetchUnsent(10);
                    assertEquals(List.of(1L), ids(mine)); // its own ids
                    other.markSent(ids(mine));
                    assertEquals(2, plain.fetchUnsent(10).size()); // marking one table leaves the other alone
                    assertEquals(0, other.fetchUnsent(10).size());
                    var e = org.junit.jupiter.api.Assertions.assertThrows(java.sql.SQLException.class, () -> sql(c,
                            "insert into app.orders_outbox (topic, payload, headers) values ('t', '\\x00', '[]')"));
                    assertTrue(e.getMessage().contains("orders_outbox_headers_check"), e.getMessage());
                }
            } finally {
                sql(c, "drop schema app cascade");
            }
        }
    }

    @Test
    void aRowThatCommitsLateWithALowerIdIsNotSkipped() throws Exception {
        try (Connection slow = TestDb.connect(); Connection fast = TestDb.connect(); PgOutboxStore store = store()) {
            slow.setAutoCommit(false);
            insert(slow, 1); // takes id 1, not committed yet
            insert(fast, 1); // takes id 2, committed
            assertEquals(List.of(2L), ids(store.fetchUnsent(10)));
            store.markSent(List.of(2L));
            slow.commit();
            assertEquals(List.of(1L), ids(store.fetchUnsent(10))); // no high-water mark, so nothing is passed over
        }
    }

    @Test
    void theRelayDrainsARealOutbox() throws Exception {
        try (Connection c = TestDb.connect(); PgOutboxStore store = store()) {
            insert(c, 2_000);
            var kafka = new Fakes.Kafka();
            var relay = new Relay(store, kafka, Fakes.fast(500));
            while (relay.step() != Relay.Step.IDLE) {
                // drain
            }
            assertEquals(range(1, 2_000), ids(kafka.visible));
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("select count(*) from outbox where sent_at is null")) {
                rs.next();
                assertEquals(0, rs.getInt(1));
            }
        }
    }
}
