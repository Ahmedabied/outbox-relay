package io.github.ahmedabied.outbox;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * A real Postgres for the integration tests: OUTBOX_TEST_DB_URL if set (user and password in the URL), otherwise a
 * Testcontainers one (image from OUTBOX_TEST_PG_IMAGE, default postgres:18-alpine).
 */
final class TestDb {
    private static String url;
    private static Properties props;

    private TestDb() {
    }

    static synchronized String url() {
        if (url == null) {
            props = new Properties();
            String given = System.getenv("OUTBOX_TEST_DB_URL");
            if (given != null && !given.isBlank()) {
                url = given;
            } else {
                String image = System.getenv().getOrDefault("OUTBOX_TEST_PG_IMAGE", "postgres:18-alpine");
                @SuppressWarnings("resource") // lives for the whole test run; Testcontainers removes it at exit
                PostgreSQLContainer pg = new PostgreSQLContainer(image);
                pg.start();
                url = pg.getJdbcUrl();
                props.setProperty("user", pg.getUsername());
                props.setProperty("password", pg.getPassword());
            }
        }
        return url;
    }

    static Properties props() {
        url();
        return props;
    }

    static Connection connect() throws SQLException {
        return DriverManager.getConnection(url(), props());
    }

    /** A fresh, empty outbox table. */
    static void fresh() throws Exception {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("drop table if exists outbox cascade");
            PgOutboxStore.install(c);
        }
    }
}
