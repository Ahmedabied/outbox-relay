package io.github.ahmedabied.outbox;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.postgresql.PGConnection;

/**
 * The outbox table in Postgres, read over one connection that also holds the lead (a session advisory lock)
 * and listens for inserts. If the connection drops, the lock goes with it and another relay can take over.
 */
public final class PgOutboxStore implements OutboxStore {
    private static final String FETCH = """
            select o.id, o.topic, o.msg_key, o.payload, o.created_at,
                   case when o.headers = '{}' or jsonb_typeof(o.headers) <> 'object' then null
                        else (select array_agg(e.key order by e.key)
                                from jsonb_each_text(jsonb_strip_nulls(o.headers)) e) end,
                   case when o.headers = '{}' or jsonb_typeof(o.headers) <> 'object' then null
                        else (select array_agg(e.value order by e.key)
                                from jsonb_each_text(jsonb_strip_nulls(o.headers)) e) end
              from {table} o
             where o.sent_at is null
             order by o.id
             limit ?""";

    // By id only: the rows were fetched on this connection moments ago, under the lead. About 2 ms per 500 rows.
    private static final String MARK = "update {table} set sent_at = clock_timestamp() where id = any(?)";

    // After a reconnect, by id and creation time (microseconds, as Postgres keeps it), so only the very rows that
    // were published: about 1 ms more per 500 rows, which is why it is not the everyday statement.
    private static final String MARK_CHECKED = """
            update {table} o set sent_at = clock_timestamp()
              from unnest(?::bigint[], ?::bigint[]) as m(id, created_us)
             where o.id = m.id and o.sent_at is null
               and (extract(epoch from o.created_at) * 1000000)::bigint = m.created_us""";

    private final String url;
    private final Properties props;
    private final String lockName;
    private final OutboxTable table;
    private final String fetch;
    private final String mark;
    private final String markChecked;
    private Connection conn;

    /** The default table, {@code outbox}. */
    public PgOutboxStore(String url, Properties props, String lockName) {
        this(url, props, lockName, OutboxTable.DEFAULT);
    }

    /**
     * @param lockName relays that share it share one lead; use one name per outbox table
     */
    public PgOutboxStore(String url, Properties props, String lockName, OutboxTable table) {
        this.url = url;
        this.props = new Properties();
        this.props.putAll(props);
        this.props.putIfAbsent("tcpKeepAlive", "true");
        this.props.putIfAbsent("ApplicationName", "outbox-relay");
        this.lockName = lockName;
        this.table = table;
        this.fetch = table.fill(FETCH);
        this.mark = table.fill(MARK);
        this.markChecked = table.fill(MARK_CHECKED);
    }

    /** Create the default outbox table, its index and the notify trigger. Safe to run again. */
    public static void install(Connection conn) throws SQLException, IOException {
        install(conn, OutboxTable.DEFAULT);
    }

    /** Create this outbox table, its index and the notify trigger. Safe to run again. */
    public static void install(Connection conn, OutboxTable table) throws SQLException, IOException {
        try (InputStream in = PgOutboxStore.class.getResourceAsStream("/outbox.sql");
             Statement st = conn.createStatement()) {
            if (in == null) {
                throw new IOException("outbox.sql is missing from the jar");
            }
            st.execute(table.fill(new String(in.readAllBytes(), StandardCharsets.UTF_8)));
        }
    }

    private Connection conn() throws SQLException {
        if (conn == null) {
            Connection c = DriverManager.getConnection(url, props);
            try {
                c.setAutoCommit(true);
                try (Statement st = c.createStatement()) {
                    // before the first fetch, so no insert is missed
                    st.execute("listen " + OutboxTable.quote(table.channel()));
                }
            } catch (SQLException e) {
                c.close();
                throw e;
            }
            conn = c;
        }
        return conn;
    }

    @Override
    public boolean tryLead() throws SQLException {
        try (PreparedStatement ps = conn().prepareStatement("select pg_try_advisory_lock(hashtext(?))")) {
            ps.setString(1, "outbox-relay:" + lockName);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    @Override
    public List<OutboxRow> fetchUnsent(int limit) throws SQLException {
        List<OutboxRow> rows = new ArrayList<>(Math.min(limit, 10_000));
        try (PreparedStatement ps = conn().prepareStatement(fetch)) {
            ps.setInt(1, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(new OutboxRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBytes(4),
                            headers(rs.getArray(6), rs.getArray(7)),
                            rs.getTimestamp(5).toInstant()));
                }
            }
        }
        return rows;
    }

    // The schema allows only an object without outbox-id; nulls are stripped above. A table made some other way
    // still cannot stop the relay: anything else is read as no headers.
    private static Map<String, String> headers(Array keys, Array values) throws SQLException {
        if (keys == null) {
            return Map.of();
        }
        String[] k = (String[]) keys.getArray();
        String[] v = (String[]) values.getArray();
        Map<String, String> out = new HashMap<>(k.length * 2);
        for (int i = 0; i < k.length; i++) {
            out.put(k[i], v[i]);
        }
        return out;
    }

    @Override
    public void markSent(List<OutboxRow> rows) throws SQLException {
        Long[] ids = new Long[rows.size()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = rows.get(i).id();
        }
        try (PreparedStatement ps = conn().prepareStatement(mark)) {
            ps.setArray(1, conn().createArrayOf("bigint", ids));
            ps.executeUpdate();
        }
    }

    @Override
    public void markSentAfterReconnect(List<OutboxRow> rows) throws SQLException {
        Long[] ids = new Long[rows.size()];
        Long[] created = new Long[rows.size()];
        for (int i = 0; i < ids.length; i++) {
            OutboxRow r = rows.get(i);
            ids[i] = r.id();
            created[i] = r.createdAt().getEpochSecond() * 1_000_000L + r.createdAt().getNano() / 1_000;
        }
        try (PreparedStatement ps = conn().prepareStatement(markChecked)) {
            ps.setArray(1, conn().createArrayOf("bigint", ids));
            ps.setArray(2, conn().createArrayOf("bigint", created));
            ps.executeUpdate();
        }
    }

    @Override
    public void awaitWork(Duration timeout) throws SQLException {
        // Returns at once if a notification came in while the last batch was out.
        conn().unwrap(PGConnection.class).getNotifications((int) Math.max(1, timeout.toMillis()));
    }

    @Override
    public void reset() {
        close();
    }

    @Override
    public void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
                // closing a connection that is already broken
            }
            conn = null;
        }
    }
}
