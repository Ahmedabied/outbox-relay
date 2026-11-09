package io.github.ahmedabied.outbox;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

/** {@code java -jar outbox-relay.jar init|run}, configured by environment variables. */
public final class Main {
    static {
        // One line per message: time, level, text.
        if (System.getProperty("java.util.logging.SimpleFormatter.format") == null) {
            System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tT %4$s %5$s%6$s%n");
        }
        // Kafka's client prints its whole config at INFO on every connect; its warnings are what matter here.
        if (System.getProperty("org.slf4j.simpleLogger.log.org.apache.kafka") == null) {
            System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka", "warn");
        }
    }

    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    private static final String USAGE = """
            usage: java -jar outbox-relay.jar init | run

              init   create the outbox table, its index and the notify trigger (safe to run again)
              run    relay until stopped; SIGTERM lets the batch in hand finish (5 s at most)

            environment:
              OUTBOX_DB_URL            jdbc:postgresql://host:5432/db           (required)
              OUTBOX_DB_USER, OUTBOX_DB_PASSWORD
              KAFKA_BOOTSTRAP_SERVERS  host:9092                                (run)
              OUTBOX_TRANSACTIONAL_ID  default outbox-relay; one per outbox table
              OUTBOX_BATCH_SIZE        default 500
              OUTBOX_STATS_SECONDS     print counters and lag this often, default 30 (0: never)
              OUTBOX_METRICS_ADDR      serve Prometheus metrics at /metrics: PORT (127.0.0.1) or HOST:PORT; off if unset
            """;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1 || !(args[0].equals("init") || args[0].equals("run"))) {
            System.err.print(USAGE);
            System.exit(2);
        }
        String url = need("OUTBOX_DB_URL");
        Properties db = new Properties();
        put(db, "user", System.getenv("OUTBOX_DB_USER"));
        put(db, "password", System.getenv("OUTBOX_DB_PASSWORD"));
        if (args[0].equals("init")) {
            try (Connection c = DriverManager.getConnection(url, db)) {
                PgOutboxStore.install(c);
            }
            System.err.println("outbox table ready");
            return;
        }
        String txId = env("OUTBOX_TRANSACTIONAL_ID", "outbox-relay");
        Relay.Settings settings = Relay.Settings.defaults()
                .withBatchSize(Integer.parseInt(env("OUTBOX_BATCH_SIZE", "500")));
        long statsEvery = Long.parseLong(env("OUTBOX_STATS_SECONDS", "30"));

        PgOutboxStore store = new PgOutboxStore(url, db, txId);
        KafkaPublisher publisher = new KafkaPublisher(need("KAFKA_BOOTSTRAP_SERVERS"), txId, Map.of());
        Relay relay = new Relay(store, publisher, settings);
        String metricsAddr = env("OUTBOX_METRICS_ADDR", "");
        MetricsServer metrics = metricsAddr.isEmpty() ? null : new MetricsServer(MetricsServer.parse(metricsAddr), relay);
        if (metrics != null) {
            LOG.info("metrics at http://" + metricsAddr + "/metrics");
        }
        CountDownLatch done = new CountDownLatch(1);
        Thread worker = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            relay.stop();
            try {
                // A batch takes milliseconds. Longer means waiting on a broker that is not there: interrupt.
                // Cut short mid-commit, a batch is at worst published again, never lost.
                if (!done.await(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    worker.interrupt();
                    done.await(5, java.util.concurrent.TimeUnit.SECONDS);
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }));
        if (statsEvery > 0) {
            Thread printer = new Thread(() -> {
                try {
                    while (true) {
                        Thread.sleep(Duration.ofSeconds(statsEvery));
                        LOG.info(relay.stats().toString());
                    }
                } catch (InterruptedException ignored) {
                    // exiting
                }
            }, "outbox-relay-stats");
            printer.setDaemon(true);
            printer.start();
        }
        int exit = 0;
        try {
            relay.run();
        } catch (RelayStoppedException e) {
            LOG.severe(e.getMessage());
            exit = 1;
        } catch (InterruptedException e) {
            LOG.warning("stopped while waiting on Kafka or the database; the batch in hand will be published again");
        } finally {
            if (metrics != null) {
                metrics.close();
            }
            publisher.close();
            store.close();
            LOG.info(relay.stats().toString());
            done.countDown();
        }
        if (exit != 0) {
            System.exit(exit);
        }
    }

    private static String need(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            System.err.print(name + " is not set\n\n" + USAGE);
            System.exit(2);
        }
        return v;
    }

    private static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : v;
    }

    private static void put(Properties p, String key, String value) {
        if (value != null) {
            p.setProperty(key, value);
        }
    }
}
