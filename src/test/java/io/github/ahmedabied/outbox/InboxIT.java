package io.github.ahmedabied.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InboxIT {

    @BeforeEach
    void freshTable() throws Exception {
        try (Connection c = TestDb.connect(); Statement st = c.createStatement()) {
            st.execute("drop table if exists outbox_inbox");
            Inbox.install(c);
            Inbox.install(c); // safe to run again
        }
    }

    @Test
    void installAgainTakesNoLockSoABusyInboxIsNotStalled() throws Exception {
        try (Connection app = TestDb.connect(); Connection c = TestDb.connect(); Statement st = c.createStatement()) {
            app.setAutoCommit(false);
            assertTrue(Inbox.claim(app, "billing", 1)); // a consumer mid-transaction holds the table for writing
            st.execute("set lock_timeout = '1s'"); // a lock the open transaction blocks would fail here, not hang
            c.setAutoCommit(false);
            Inbox.install(c);
            try (var rs = st.executeQuery("select count(*) from pg_locks where pid = pg_backend_pid()"
                    + " and relation = 'outbox_inbox'::regclass")) {
                rs.next();
                assertEquals(0, rs.getInt(1), "running inbox.sql again took a lock on the table");
            }
            c.rollback();
            app.rollback();
        }
    }

    @Test
    void anIdIsHandledOncePerConsumer() throws Exception {
        try (Connection c = TestDb.connect()) {
            c.setAutoCommit(false);
            assertTrue(Inbox.claim(c, "billing", 7));
            assertFalse(Inbox.claim(c, "billing", 7));
            assertTrue(Inbox.claim(c, "search-index", 7)); // another consumer has its own record
            c.commit();
            assertFalse(Inbox.claim(c, "billing", 7));
            c.rollback();
        }
    }

    @Test
    void aRolledBackWriteFreesTheId() throws Exception {
        try (Connection c = TestDb.connect()) {
            c.setAutoCommit(false);
            assertTrue(Inbox.claim(c, "billing", 8));
            c.rollback(); // the consumer's own write failed
            assertTrue(Inbox.claim(c, "billing", 8)); // so the repeat is handled, not skipped
            c.commit();
        }
    }

    @Test
    void aClaimOutsideATransactionIsRefused() throws Exception {
        try (Connection c = TestDb.connect()) {
            var e = assertThrows(IllegalStateException.class, () -> Inbox.claim(c, "billing", 9));
            assertTrue(e.getMessage().contains("autocommit"));
        }
    }

    @Test
    void twoInstancesRacingForOneIdGetOneYes() throws Exception {
        try (Connection a = TestDb.connect(); Connection b = TestDb.connect()) {
            a.setAutoCommit(false);
            b.setAutoCommit(false);
            assertTrue(Inbox.claim(a, "billing", 10));
            CompletableFuture<Boolean> second = CompletableFuture.supplyAsync(() -> {
                try {
                    return Inbox.claim(b, "billing", 10); // waits on a's open transaction
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Thread.sleep(200);
            assertFalse(second.isDone(), "the second claim should wait for the first transaction");
            a.commit();
            assertFalse(second.get(10, TimeUnit.SECONDS));
            b.commit();
        }
    }

    @Test
    void theIdComesFromTheRelaysHeader() {
        var headers = new RecordHeaders();
        headers.add(KafkaPublisher.ID_HEADER, "123".getBytes(StandardCharsets.UTF_8));
        assertEquals(123, Inbox.outboxId(headers));
        assertThrows(IllegalArgumentException.class, () -> Inbox.outboxId(new RecordHeaders()));
    }
}
