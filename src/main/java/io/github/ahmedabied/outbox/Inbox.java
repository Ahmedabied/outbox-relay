package io.github.ahmedabied.outbox;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * The consumer's half of the deal: the relay can publish a batch twice (a crash between the Kafka commit and the
 * database mark), so a consumer that writes to Postgres records each outbox id it handles, in the same transaction
 * as its own write. A repeat then finds its id taken and is skipped; a rolled-back write frees the id again.
 *
 * <pre>{@code
 * conn.setAutoCommit(false);
 * if (Inbox.claim(conn, "billing", Inbox.outboxId(record.headers()))) {
 *     // the consumer's own writes, on the same connection
 * }
 * conn.commit();
 * }</pre>
 */
public final class Inbox {
    private Inbox() {
    }

    /** Create the inbox table. Safe to run again. */
    public static void install(Connection conn) throws SQLException, IOException {
        try (InputStream in = Inbox.class.getResourceAsStream("/inbox.sql"); Statement st = conn.createStatement()) {
            if (in == null) {
                throw new IOException("inbox.sql is missing from the jar");
            }
            st.execute(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    /**
     * True the first time this consumer sees the id: handle the event. False for a repeat: skip it.
     * Must run inside the transaction that holds the consumer's own writes, or a crash between the two would
     * mark an event handled that never was. Another consumer instance holding the same id in an open transaction
     * makes this wait for it, then answer false.
     */
    public static boolean claim(Connection conn, String consumer, long outboxId) throws SQLException {
        if (conn.getAutoCommit()) {
            throw new IllegalStateException("claim in the same transaction as the consumer's own writes:"
                    + " autocommit is on, so the claim would commit on its own");
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into outbox_inbox (consumer, outbox_id) values (?, ?) on conflict do nothing")) {
            ps.setString(1, consumer);
            ps.setLong(2, outboxId);
            return ps.executeUpdate() == 1;
        }
    }

    /** The outbox id the relay put on a record. */
    public static long outboxId(Headers headers) {
        Header h = headers.lastHeader(KafkaPublisher.ID_HEADER);
        if (h == null) {
            throw new IllegalArgumentException("no " + KafkaPublisher.ID_HEADER + " header: not from outbox-relay");
        }
        return Long.parseLong(new String(h.value(), StandardCharsets.UTF_8));
    }
}
