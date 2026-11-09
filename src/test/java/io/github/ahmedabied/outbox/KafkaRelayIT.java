package io.github.ahmedabied.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Postgres and Kafka in containers (CI). Skipped where Docker is missing. */
@Testcontainers(disabledWithoutDocker = true)
class KafkaRelayIT {
    private static final Path BENCH = Path.of("target", "bench.md");

    @BeforeEach
    void freshTable() throws Exception {
        TestDb.fresh();
    }

    private static void report(String line) throws IOException {
        System.out.println("BENCH " + line);
        Files.writeString(BENCH, line + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    /** A relay running in a thread; its failure, if any, is kept for the test to look at. */
    private static final class Running implements AutoCloseable {
        final Relay relay;
        final Thread thread;
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final PgOutboxStore store;
        final KafkaPublisher publisher;

        Running(String txId, int batch) {
            store = new PgOutboxStore(TestDb.url(), TestDb.props(), txId);
            publisher = new KafkaPublisher(KafkaEnv.bootstrap(), txId, Map.of());
            relay = new Relay(store, publisher, Relay.Settings.defaults().withBatchSize(batch));
            thread = new Thread(() -> {
                try {
                    relay.run();
                } catch (Throwable t) {
                    failure.set(t);
                }
            }, "relay-" + txId);
            thread.start();
        }

        @Override
        public void close() {
            relay.stop();
            try {
                thread.join(30_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            publisher.close();
            store.close();
        }
    }

    @Test
    void aBacklogArrivesWholeAndInOrderPerKey() throws Exception {
        String topic = KafkaEnv.newTopic(6);
        int n = 50_000;
        try (Connection c = TestDb.connect()) {
            KafkaEnv.insert(c, topic, n, 100);
        }
        long t0 = System.nanoTime();
        try (Running r = new Running("backlog", 500); KafkaEnv.Consumer consumer = new KafkaEnv.Consumer(topic)) {
            assertTrue(consumer.pollUntil(x -> x.seen.size() >= n, Duration.ofSeconds(120)), "timed out");
            double seconds = (System.nanoTime() - t0) / 1e9;
            assertEquals(n, consumer.seen.size());
            assertEquals(n, consumer.distinct()); // no repeats without a crash
            consumer.firstArrivals().forEach((key, ids) -> assertTrue(KafkaEnv.increasing(ids), key));
            report(String.format("backlog: %,d events waiting, 100 keys, 6 partitions, batch 500: all consumed in"
                    + " %.1f s, %,.0f events/s, %,d Kafka transactions", n, seconds, n / seconds,
                    r.relay.stats().batches.sum()));
        }
    }

    @Test
    void steadyTrafficLag() throws Exception {
        // Measured once with fetch.max.wait.ms=50 as well: no change. The tail is the Kafka commit, not the consumer.
        steady("consumer defaults", Map.of());
    }

    private void steady(String label, Map<String, Object> consumerSettings) throws Exception {
        TestDb.fresh();
        String topic = KafkaEnv.newTopic(6);
        int perSecond = 1_000;
        int seconds = 10;
        int total = perSecond * seconds;
        try (Running r = new Running("steady", 500);
             KafkaEnv.Consumer consumer = new KafkaEnv.Consumer(topic, consumerSettings);
             Connection c = TestDb.connect();
             PreparedStatement ps = c.prepareStatement("insert into outbox (topic, msg_key, payload) values (?, ?,"
                     + " convert_to((extract(epoch from clock_timestamp()) * 1000000)::bigint::text, 'UTF8'))")) {
            consumer.pollUntil(x -> false, Duration.ofSeconds(2)); // joined the group before the traffic starts
            AtomicReference<Boolean> got = new AtomicReference<>(false);
            Thread reader = new Thread(() -> got.set(consumer.pollUntil(x -> x.seen.size() >= total,
                    Duration.ofSeconds(seconds + 60))));
            reader.start();
            long start = System.nanoTime();
            for (int i = 0; i < total; i++) {
                long due = start + i * 1_000_000_000L / perSecond;
                while (System.nanoTime() < due) {
                    Thread.onSpinWait();
                }
                ps.setString(1, topic);
                ps.setString(2, "k" + (i % 100));
                ps.executeUpdate(); // one transaction per event, like an app
            }
            reader.join();
            assertTrue(got.get(), "timed out");
            long[] lag = consumer.seen.stream().mapToLong(s -> s.receivedMicros() - s.insertedMicros()).sorted()
                    .toArray();
            Stats st = r.relay.stats();
            report(String.format("steady, %s: %,d events/s for %d s, one transaction each. Insert to committed in"
                            + " Kafka p50 %.1f ms, p99 %.1f ms. Insert to consumer p50 %.1f ms, p99 %.1f ms,"
                            + " max %.1f ms. %,d Kafka transactions", label, perSecond, seconds,
                    st.lag(0.5).toNanos() / 1e6, st.lag(0.99).toNanos() / 1e6, lag[lag.length / 2] / 1e3,
                    lag[(int) (lag.length * 0.99)] / 1e3, lag[lag.length - 1] / 1e3, st.batches.sum()));
        }
    }

    @Test
    void aDeathBetweenTheKafkaCommitAndTheDatabaseMarkRepeatsThatBatchOnly() throws Exception {
        String topic = KafkaEnv.newTopic(6);
        int n = 5_000;
        try (Connection c = TestDb.connect()) {
            KafkaEnv.insert(c, topic, n, 100);
        }
        var real = new PgOutboxStore(TestDb.url(), TestDb.props(), "window");
        // The third batch reaches Kafka, then the connection dies before the mark, as if the process had.
        OutboxStore dying = new OutboxStore() {
            int marks;

            @Override
            public boolean tryLead() throws Exception {
                return real.tryLead();
            }

            @Override
            public List<OutboxRow> fetchUnsent(int limit) throws Exception {
                return real.fetchUnsent(limit);
            }

            @Override
            public void markSent(List<Long> ids) throws Exception {
                if (++marks == 3) {
                    real.close();
                    throw new java.sql.SQLException("connection lost between the Kafka commit and the mark");
                }
                real.markSent(ids);
            }

            @Override
            public void awaitWork(Duration timeout) throws Exception {
                real.awaitWork(timeout);
            }

            @Override
            public void reset() {
                real.reset();
            }

            @Override
            public void close() {
                real.close();
            }
        };
        try (var publisher = new KafkaPublisher(KafkaEnv.bootstrap(), "window", Map.of());
             KafkaEnv.Consumer consumer = new KafkaEnv.Consumer(topic)) {
            var relay = new Relay(dying, publisher, Relay.Settings.defaults().withBatchSize(500));
            while (relay.step() != Relay.Step.IDLE) {
                // drain
            }
            dying.close();
            assertTrue(consumer.pollUntil(x -> x.seen.size() >= n + 500, Duration.ofSeconds(30)));
            consumer.pollUntil(x -> false, Duration.ofSeconds(1));
            assertEquals(n + 500, consumer.seen.size()); // the third batch, twice; nothing else
            assertEquals(n, consumer.distinct());
            consumer.firstArrivals().forEach((key, ids) -> assertTrue(KafkaEnv.increasing(ids), key));
            report(String.format("death between Kafka commit and database mark: %,d events, batch 500: the batch in"
                    + " that window arrived twice (500 repeats), 0 lost, order per key kept once repeats are dropped"
                    + " by outbox-id", n));
        }
    }

    @Test
    void aRowKafkaRefusesStopsTheRelayWithEverythingBeforeItDelivered() throws Exception {
        String topic = KafkaEnv.newTopic(1);
        try (Connection c = TestDb.connect()) {
            KafkaEnv.insert(c, topic, 10, 3);
            try (PreparedStatement ps = c.prepareStatement(
                    "insert into outbox (topic, msg_key, payload) values (?, 'k1', ?)")) {
                ps.setString(1, topic);
                ps.setBytes(2, new byte[2 * 1024 * 1024]); // over the producer's 1 MB max.request.size
                ps.executeUpdate();
            }
            KafkaEnv.insert(c, topic, 10, 3);
        }
        try (Running r = new Running("poison", 500); KafkaEnv.Consumer consumer = new KafkaEnv.Consumer(topic)) {
            r.thread.join(60_000);
            Throwable stopped = r.failure.get();
            assertTrue(stopped instanceof RelayStoppedException, String.valueOf(stopped));
            assertTrue(stopped.getMessage().startsWith("row 11 cannot be published"), stopped.getMessage());
            assertTrue(stopped.getMessage().contains("RecordTooLarge"), stopped.getMessage());
            consumer.pollUntil(x -> false, Duration.ofSeconds(3));
            assertEquals(Fakes.range(1, 10), consumer.seen.stream().map(KafkaEnv.Seen::id).toList());
        }
    }

    @Test
    void aSecondPublisherWithTheSameIdFencesTheFirst() throws Exception {
        String topic = KafkaEnv.newTopic(1);
        var row = new OutboxRow(1, topic, "k", new byte[] {1}, Map.of(), Instant.now());
        try (var first = new KafkaPublisher(KafkaEnv.bootstrap(), "fence", Map.of());
             var second = new KafkaPublisher(KafkaEnv.bootstrap(), "fence", Map.of())) {
            first.publish(List.of(row));
            second.publish(List.of(row)); // initTransactions bumps the epoch
            var e = assertThrows(PublishException.class, () -> first.publish(List.of(row)));
            assertEquals(PublishException.Kind.FENCED, e.kind());
        }
    }

    @Test
    void killingTheRelayMidStreamLosesNothingAndKeepsOrder() throws Exception {
        String topic = KafkaEnv.newTopic(6);
        int n = 40_000;
        long[] killAt = {4_000, 10_000, 16_000, 22_000, 28_000}; // rows marked sent in the database
        List<Long> unsentAtKill = new ArrayList<>();
        try (Connection c = TestDb.connect()) {
            KafkaEnv.insert(c, topic, n, 100);
            for (long at : killAt) {
                Process p = child();
                long sent = waitForSent(c, at, Duration.ofSeconds(90));
                p.destroyForcibly(); // SIGKILL: no shutdown hook, no goodbye to Postgres or Kafka
                p.waitFor();
                unsentAtKill.add(n - sent);
                assertTrue(sent < n, "the relay had finished before the kill, so the kill tested nothing");
            }
        }
        try (KafkaEnv.Consumer consumer = new KafkaEnv.Consumer(topic)) {
            Process last = child();
            try {
                assertTrue(consumer.pollUntil(x -> x.distinct() >= n, Duration.ofSeconds(120)),
                        "only " + consumer.distinct() + " of " + n);
                consumer.pollUntil(x -> false, Duration.ofSeconds(2)); // any late repeats
            } finally {
                last.destroy();
                last.waitFor();
            }
            assertEquals(n, consumer.distinct()); // nothing lost
            consumer.firstArrivals().forEach((key, ids) -> assertTrue(KafkaEnv.increasing(ids), key));
            long repeats = consumer.seen.size() - n;
            report(String.format("crash: %,d events, batch 100, relay JVM killed with SIGKILL %d times mid-stream"
                    + " (rows still unsent at each kill: %s), restarted each time: 0 lost, order per key kept,"
                    + " %,d repeats (%.2f%%), each one dropped by its outbox-id", n, killAt.length, unsentAtKill,
                    repeats, 100.0 * repeats / n));
        }
    }

    private static long waitForSent(Connection c, long atLeast, Duration timeout) throws Exception {
        long end = System.nanoTime() + timeout.toNanos();
        try (PreparedStatement ps = c.prepareStatement("select count(*) from outbox where sent_at is not null")) {
            while (true) {
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    long sent = rs.getLong(1);
                    if (sent >= atLeast || System.nanoTime() > end) {
                        return sent;
                    }
                }
                Thread.sleep(2);
            }
        }
    }

    /** The relay as its own JVM from the built jar, the way it runs in production, so it can be killed for real. */
    private static Process child() throws IOException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Path jar = Path.of("target", "outbox-relay.jar");
        assertTrue(Files.exists(jar), "run mvn verify: the integration tests use the packaged jar");
        ProcessBuilder pb = new ProcessBuilder(java, "-Xmx256m", "-jar", jar.toString(), "run");
        Map<String, String> env = pb.environment();
        String url = TestDb.url();
        var props = TestDb.props();
        env.put("OUTBOX_DB_URL", url);
        if (props.getProperty("user") != null) {
            env.put("OUTBOX_DB_USER", props.getProperty("user"));
            env.put("OUTBOX_DB_PASSWORD", props.getProperty("password"));
        }
        env.put("KAFKA_BOOTSTRAP_SERVERS", KafkaEnv.bootstrap());
        env.put("OUTBOX_TRANSACTIONAL_ID", "crash");
        env.put("OUTBOX_BATCH_SIZE", "100");
        env.put("OUTBOX_STATS_SECONDS", "0");
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(Path.of("target", "crash-relay.log").toFile()));
        return pb.start();
    }
}
