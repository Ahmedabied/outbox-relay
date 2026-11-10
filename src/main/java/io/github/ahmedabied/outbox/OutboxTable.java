package io.github.ahmedabied.outbox;

import java.util.regex.Pattern;

/**
 * Which outbox table a relay serves: {@code outbox} by default, or another name, optionally with its schema
 * ({@code app.orders_outbox}). The names of its index, check, trigger and notify channel come from it, so the
 * default gives the same objects as before ({@code outbox_unsent}, {@code outbox_relay}, ...).
 *
 * <p>Names are lower case letters, digits and underscores, starting with a letter or underscore. Every name is
 * quoted in the SQL as well, so nothing from the environment is ever read as SQL, and reserved words still work.
 */
public final class OutboxTable {
    private static final Pattern IDENTIFIER = Pattern.compile("[a-z_][a-z0-9_]*");
    private static final int MAX_IDENTIFIER = 63; // Postgres cuts longer names silently; refuse them instead

    public static final OutboxTable DEFAULT = parse("outbox"); // after IDENTIFIER: parse() needs it

    private final String schema; // null: the first schema on the search_path, as an unqualified name always was
    private final String name;

    private OutboxTable(String schema, String name) {
        this.schema = schema;
        this.name = name;
    }

    /** {@code outbox}, {@code orders_outbox} or {@code app.orders_outbox}. */
    public static OutboxTable parse(String spec) {
        String[] parts = spec.trim().split("\\.", -1);
        if (parts.length > 2) {
            throw bad(spec, "at most one dot, between schema and table");
        }
        for (String p : parts) {
            if (!IDENTIFIER.matcher(p).matches()) {
                throw bad(spec, "use lower case letters, digits and _, starting with a letter or _");
            }
        }
        OutboxTable t = parts.length == 2 ? new OutboxTable(parts[0], parts[1]) : new OutboxTable(null, parts[0]);
        for (String derived : new String[] {t.checkName(), t.channel(), t.schema == null ? "" : t.schema}) {
            if (derived.length() > MAX_IDENTIFIER) {
                throw bad(spec, "too long: '" + derived + "' would pass Postgres' 63 character limit");
            }
        }
        return t;
    }

    private static IllegalArgumentException bad(String spec, String why) {
        return new IllegalArgumentException("outbox table name '" + spec + "': " + why);
    }

    /** The table as written in SQL, quoted. */
    String sql() {
        return qualified(name);
    }

    /** The table as a Postgres string literal, for {@code ::regclass}. */
    String literal() {
        return "'" + sql() + "'";
    }

    /** A sibling object in the table's schema (a function), quoted. */
    String qualified(String object) {
        return schema == null ? quote(object) : quote(schema) + "." + quote(object);
    }

    String indexName() {
        return name + "_unsent";
    }

    String checkName() {
        return name + "_headers_check";
    }

    String notifyName() {
        return name + "_notify";
    }

    /**
     * The channel the insert trigger notifies and the relay listens on. Two tables whose names meet here (app.outbox
     * and app_outbox) share it, which costs the other relay a wake-up and an empty fetch, nothing more.
     */
    String channel() {
        return (schema == null ? "" : schema + "_") + name + "_relay";
    }

    /** One transactional id, and so one lead, per table, unless OUTBOX_TRANSACTIONAL_ID says otherwise. */
    String defaultTransactionalId() {
        return this.equals(DEFAULT) ? "outbox-relay" : "outbox-relay:" + this;
    }

    /** Fills the names in braces in outbox.sql and the relay's own statements. */
    String fill(String sql) {
        return sql.replace("{table}", sql())
                .replace("{table_literal}", literal())
                .replace("{check}", quote(checkName()))
                .replace("{check_literal}", "'" + checkName() + "'")
                .replace("{index_literal}", "'" + qualified(indexName()) + "'")
                .replace("{index}", quote(indexName()))
                .replace("{function}", qualified(notifyName()))
                .replace("{trigger_literal}", "'" + notifyName() + "'")
                .replace("{trigger}", quote(notifyName()))
                .replace("{channel_literal}", "'" + channel() + "'");
    }

    static String quote(String identifier) {
        return '"' + identifier + '"'; // safe: parse() let in no quote or anything else to escape
    }

    @Override
    public String toString() {
        return schema == null ? name : schema + "." + name;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof OutboxTable t && toString().equals(t.toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
