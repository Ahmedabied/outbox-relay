package io.github.ahmedabied.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class OutboxTableTest {

    @Test
    void theDefaultKeepsTheNamesOfEarlierVersions() throws Exception {
        OutboxTable t = OutboxTable.DEFAULT;
        assertEquals("outbox", t.toString());
        assertEquals("\"outbox\"", t.sql());
        assertEquals("outbox_unsent", t.indexName());
        assertEquals("outbox_headers_check", t.checkName());
        assertEquals("outbox_notify", t.notifyName());
        assertEquals("outbox_relay", t.channel());
        assertEquals("outbox-relay", t.defaultTransactionalId());
        String sql = t.fill(schema());
        assertFalse(java.util.regex.Pattern.compile("\\{[a-z_]+}").matcher(sql).find(), "a name left unfilled");
        assertTrue(sql.contains("perform pg_notify('outbox_relay', '');"));
        assertTrue(sql.contains("create index if not exists \"outbox_unsent\" on \"outbox\" (id)"));
    }

    @Test
    void aTableInItsOwnSchema() throws Exception {
        OutboxTable t = OutboxTable.parse(" app.orders_outbox ");
        assertEquals("app.orders_outbox", t.toString());
        assertEquals("\"app\".\"orders_outbox\"", t.sql());
        assertEquals("\"app\".\"orders_outbox_notify\"", t.qualified(t.notifyName()));
        assertEquals("app_orders_outbox_relay", t.channel());
        assertEquals("outbox-relay:app.orders_outbox", t.defaultTransactionalId());
        assertEquals(OutboxTable.parse("app.orders_outbox"), t);
        String sql = t.fill(schema());
        assertTrue(sql.contains("create table if not exists \"app\".\"orders_outbox\" ("));
        assertTrue(sql.contains("conrelid = '\"app\".\"orders_outbox\"'::regclass"));
        assertTrue(sql.contains("execute function \"app\".\"orders_outbox_notify\"();"));
    }

    @Test
    void anythingButAPlainNameIsRefused() {
        for (String bad : new String[] {"", " ", "Outbox", "1outbox", "out-box", "\"outbox\"", "outbox;drop table users",
                "outbox o", "a.b.c", "app.", ".outbox", "app..outbox", "outboxé"}) {
            var e = assertThrows(IllegalArgumentException.class, () -> OutboxTable.parse(bad), bad);
            assertTrue(e.getMessage().startsWith("outbox table name '"), e.getMessage());
        }
    }

    @Test
    void namesPostgresWouldCutAreRefused() {
        OutboxTable.parse("t".repeat(49)); // t..t_headers_check is 63
        var e = assertThrows(IllegalArgumentException.class, () -> OutboxTable.parse("t".repeat(50)));
        assertTrue(e.getMessage().contains("63"), e.getMessage());
        assertThrows(IllegalArgumentException.class, () -> OutboxTable.parse("s".repeat(30) + "." + "t".repeat(30)));
    }

    private static String schema() throws Exception {
        try (InputStream in = OutboxTable.class.getResourceAsStream("/outbox.sql")) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
