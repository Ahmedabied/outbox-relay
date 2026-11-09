package io.github.ahmedabied.outbox;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testcontainers.kafka.KafkaContainer;

/** One Kafka broker for the whole test run, and a read_committed consumer that records what arrives. */
final class KafkaEnv {
    static final String IMAGE = System.getenv().getOrDefault("OUTBOX_TEST_KAFKA_IMAGE", "apache/kafka-native:4.4.0");
    private static KafkaContainer kafka;

    private KafkaEnv() {
    }

    @SuppressWarnings("resource") // lives for the whole test run; Testcontainers removes it at exit
    static synchronized String bootstrap() {
        if (kafka == null) {
            kafka = new KafkaContainer(IMAGE)
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                    .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
            kafka.start();
        }
        return kafka.getBootstrapServers();
    }

    static String newTopic(int partitions) throws Exception {
        String name = "orders-" + UUID.randomUUID().toString().substring(0, 8);
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap()))) {
            admin.createTopics(List.of(new NewTopic(name, partitions, (short) 1))).all().get();
        }
        return name;
    }

    /** Insert n rows for the topic over the given number of keys; the payload is the insert time in microseconds. */
    static void insert(Connection c, String topic, int n, int keys) throws Exception {
        try (Statement st = c.createStatement()) {
            st.execute("insert into outbox (topic, msg_key, payload) select '" + topic + "', 'k' || (g % " + keys
                    + "), convert_to((extract(epoch from clock_timestamp()) * 1000000)::bigint::text, 'UTF8')"
                    + " from generate_series(1, " + n + ") g");
        }
    }

    /** What a consumer saw, in arrival order. */
    record Seen(long id, String key, long insertedMicros, long receivedMicros) {
    }

    static final class Consumer implements AutoCloseable {
        final List<Seen> seen = new ArrayList<>();
        private final KafkaConsumer<String, byte[]> consumer;

        Consumer(String topic) {
            Properties p = new Properties();
            p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
            p.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
            p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            consumer = new KafkaConsumer<>(p, new StringDeserializer(), new ByteArrayDeserializer());
            consumer.subscribe(List.of(topic));
        }

        /** Poll until the condition holds or the timeout passes. */
        boolean pollUntil(Predicate<Consumer> done, Duration timeout) {
            long end = System.nanoTime() + timeout.toNanos();
            while (!done.test(this) && System.nanoTime() < end) {
                for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(100))) {
                    Header h = r.headers().lastHeader(KafkaPublisher.ID_HEADER);
                    long id = Long.parseLong(new String(h.value(), StandardCharsets.UTF_8));
                    long inserted = parseOrZero(r.value());
                    seen.add(new Seen(id, r.key(), inserted, java.time.temporal.ChronoUnit.MICROS.between(java.time.Instant.EPOCH, java.time.Instant.now())));
                }
            }
            return done.test(this);
        }

        long distinct() {
            return seen.stream().mapToLong(Seen::id).distinct().count();
        }

        /** Per key, the ids in arrival order with repeats dropped. */
        Map<String, List<Long>> firstArrivals() {
            Map<String, List<Long>> out = new HashMap<>();
            java.util.Set<Long> had = new java.util.HashSet<>();
            for (Seen s : seen) {
                if (had.add(s.id())) {
                    out.computeIfAbsent(s.key(), k -> new ArrayList<>()).add(s.id());
                }
            }
            return out;
        }

        private static long parseOrZero(byte[] v) {
            try {
                return Long.parseLong(new String(v, StandardCharsets.UTF_8));
            } catch (NumberFormatException e) {
                return 0;
            }
        }

        @Override
        public void close() {
            consumer.close();
        }
    }

    static boolean increasing(List<Long> ids) {
        for (int i = 1; i < ids.size(); i++) {
            if (ids.get(i) <= ids.get(i - 1)) {
                return false;
            }
        }
        return true;
    }
}
