package io.github.ahmedabied.outbox;

import static io.github.ahmedabied.outbox.Fakes.ids;
import static io.github.ahmedabied.outbox.Fakes.range;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        return new PgOutboxStore(TestDb.url(), TestDb.props());
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

    private static int count(Connection c, String query) throws Exception {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(query)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Test
    void installAgainTakesNoLockSoABusyTableIsNotStalled() throws Exception {
        try (Connection app = TestDb.connect(); Connection c = TestDb.connect(); Connection other = TestDb.connect()) {
            app.setAutoCommit(false);
            insert(app, 1); // one open transaction that wrote an event holds the table against schema changes
            long start = System.nanoTime();
            PgOutboxStore.install(c); // used to wait for that transaction, with every later insert queued behind it
            assertTrue(System.nanoTime() - start < 2_000_000_000L, "install waited for the open transaction");
            sql(other, "set lock_timeout = '2s'");
            insert(other, 1); // nothing stands in the queue in front of it
            app.rollback();
        }
    }

    @Test
    void anUpgradeThatCannotGetTheTableGivesUpAndChangesNothing() throws Exception {
        String hasCheck = "select count(*) from pg_constraint where conname = 'outbox_headers_check'";
        try (Connection app = TestDb.connect(); Connection c = TestDb.connect()) {
            sql(c, "alter table outbox drop constraint outbox_headers_check"); // a table from before the check
            app.setAutoCommit(false);
            insert(app, 1);
            long start = System.nanoTime();
            var e = assertThrows(PgOutboxStore.TableBusyException.class, () -> PgOutboxStore.install(c));
            long waitedMs = (System.nanoTime() - start) / 1_000_000;
            assertTrue(waitedMs >= 2_500 && waitedMs < 10_000, "waited " + waitedMs + " ms");
            assertTrue(e.getMessage().contains("Nothing was changed"), e.getMessage());
            assertTrue(c.getAutoCommit(), "the connection comes back as it was given");
            assertEquals(0, count(c, hasCheck));
            app.commit();
            PgOutboxStore.install(c); // the table is free now: the upgrade goes through
            assertEquals(1, count(c, hasCheck));
        }
    }

    @Test
    void installInsideTheCallersTransactionCommitsOrRollsBackWithIt() throws Exception {
        String hasTrigger = "select count(*) from pg_trigger where tgname = 'outbox_notify'";
        try (Connection c = TestDb.connect()) {
            sql(c, "drop trigger outbox_notify on outbox");
            c.setAutoCommit(false);
            PgOutboxStore.install(c);
            assertFalse(c.getAutoCommit());
            c.rollback(); // the caller's decision: the trigger the install made goes with it
            assertEquals(0, count(c, hasTrigger));
            PgOutboxStore.install(c);
            c.commit();
            assertEquals(1, count(c, hasTrigger));
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
            store.markSent(rows);
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
            store.markSent(rows); // the old row can still be marked sent, or the relay would send it forever
            assertEquals(0, store.fetchUnsent(10).size());
        }
    }

    @Test
    void installUpgradesTheHeaderCheckOfAnEarlierVersion() throws Exception {
        try (Connection c = TestDb.connect(); PgOutboxStore store = store()) {
            sql(c, "alter table outbox drop constraint outbox_headers_check");
            sql(c, "insert into outbox (topic, payload, headers) values ('t', '\\x00', '[1, 2]')");
            // the check as the first version wrote it, added to a table that already held such a row
            sql(c, "alter table outbox add constraint outbox_headers_check"
                    + " check (jsonb_typeof(headers) = 'object' and not headers ? 'outbox-id') not valid");
            PgOutboxStore.install(c);
            store.markSent(store.fetchUnsent(10));
            assertEquals(0, store.fetchUnsent(10).size());
            var e = org.junit.jupiter.api.Assertions.assertThrows(java.sql.SQLException.class, () -> sql(c,
                    "insert into outbox (topic, payload, headers) values ('t', '\\x00', '{\"outbox-id\": \"9\"}')"));
            assertTrue(e.getMessage().contains("outbox_headers_check"), e.getMessage()); // new rows still checked
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
    void theLeadBelongsToTheTableItself() throws Exception {
        try (Connection c = TestDb.connect(); PgOutboxStore a = store();
             PgOutboxStore b = new PgOutboxStore(TestDb.url(), TestDb.props(), OutboxTable.parse("public.outbox"))) {
            assertTrue(a.tryLead());
            assertFalse(b.tryLead()); // the same table, named another way: the same lead
            sql(c, "drop table outbox cascade");
            PgOutboxStore.install(c); // the table made again keeps its key: still one leader
            assertTrue(a.tryLead());
            assertFalse(b.tryLead());
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
                     PgOutboxStore other = new PgOutboxStore(TestDb.url(), TestDb.props(), orders)) {
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
                    other.markSent(mine);
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
    void onlyTheVeryRowsPublishedAreMarked() throws Exception {
        try (Connection c = TestDb.connect(); PgOutboxStore store = store()) {
            insert(c, 1);
            List<OutboxRow> published = store.fetchUnsent(10);
            store.markSent(published);
            String firstMark;
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select sent_at::text from outbox")) {
                rs.next();
                firstMark = rs.getString(1);
            }
            store.markSentAfterReconnect(published); // marked again after a reconnect: stays as it was
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select sent_at::text from outbox")) {
                rs.next();
                assertEquals(firstMark, rs.getString(1));
            }
            // The identity starts again and a new row gets the same id: a remembered batch must not mark it.
            sql(c, "truncate outbox restart identity");
            insert(c, 1);
            store.markSentAfterReconnect(published);
            assertEquals(List.of(1L), ids(store.fetchUnsent(10)));
        }
    }

    @Test
    void aRowThatCommitsLateWithALowerIdIsNotSkipped() throws Exception {
        try (Connection slow = TestDb.connect(); Connection fast = TestDb.connect(); PgOutboxStore store = store()) {
            slow.setAutoCommit(false);
            insert(slow, 1); // takes id 1, not committed yet
            insert(fast, 1); // takes id 2, committed
            List<OutboxRow> committed = store.fetchUnsent(10);
            assertEquals(List.of(2L), ids(committed));
            store.markSent(committed);
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
