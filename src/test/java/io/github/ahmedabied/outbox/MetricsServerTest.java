package io.github.ahmedabied.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

class MetricsServerTest {
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static HttpResponse<String> send(int port, String method, String path) throws Exception {
        var req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .method(method, HttpRequest.BodyPublishers.noBody()).build();
        return HTTP.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private static Map<String, Double> parse(String body) {
        Map<String, Double> out = new HashMap<>();
        for (String line : body.split("\n")) {
            if (!line.startsWith("#") && !line.isBlank()) {
                int sp = line.lastIndexOf(' ');
                out.put(line.substring(0, sp), Double.parseDouble(line.substring(sp + 1)));
            }
        }
        return out;
    }

    @Test
    void servesTheRelaysCountersInPrometheusText() throws Exception {
        var db = new Fakes.Db().insert(1_200, 7);
        var relay = new Relay(new Fakes.Store(db), new Fakes.Kafka(), Fakes.fast(500));
        while (relay.step() != Relay.Step.IDLE) {
            // drain
        }
        try (var server = new MetricsServer(new InetSocketAddress("127.0.0.1", 0), relay)) {
            var res = send(server.port(), "GET", "/metrics");
            assertEquals(200, res.statusCode());
            assertTrue(res.headers().firstValue("Content-Type").orElse("").startsWith("text/plain; version=0.0.4"));
            Map<String, Double> m = parse(res.body());
            assertEquals(1_200, m.get("outbox_relay_published_total"));
            assertEquals(3, m.get("outbox_relay_batches_total"));
            assertEquals(1, m.get("outbox_relay_leading"));
            assertEquals(0, m.get("outbox_relay_oldest_unsent_age_seconds")); // empty at the last fetch
            assertTrue(Math.abs(m.get("outbox_relay_last_fetch_timestamp_seconds") - Instant.now().getEpochSecond()) < 60);
            assertTrue(m.containsKey("outbox_relay_lag_seconds{quantile=\"0.99\"}"));
            assertEquals("http://127.0.0.1:" + server.port() + "/metrics", server.url());
            assertEquals(404, send(server.port(), "GET", "/").statusCode());
            assertEquals(405, send(server.port(), "POST", "/metrics").statusCode());
        }
    }

    @Test
    void aStuckOutboxShowsAsTheAgeOfItsFirstRow() throws Exception {
        var db = new Fakes.Db();
        db.rows.put(1L, new OutboxRow(1, "orders", "k", new byte[0], Map.of(), Instant.now().minusSeconds(90)));
        var kafka = new Fakes.Kafka();
        kafka.retriableFailures = 1_000; // the broker is down
        var relay = new Relay(new Fakes.Store(db), kafka, Fakes.fast(500));
        assertEquals(Relay.Step.RETRY, relay.step());
        Map<String, Double> m = parse(MetricsServer.render(relay, Instant.now()));
        assertTrue(m.get("outbox_relay_oldest_unsent_age_seconds") >= 90, m.toString());
        assertEquals(1, m.get("outbox_relay_retries_total"));
    }

    /** A clock the test moves by hand. */
    private static final class Tick extends Clock {
        Instant now = Instant.now();

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    @Test
    void whileKafkaIsDownTheRelayKeepsReadingSoTheAgeGrows() throws Exception {
        var db = new Fakes.Db().insert(10, 1);
        var kafka = new Fakes.Kafka();
        kafka.retriableFailures = 1_000;
        var clock = new Tick();
        var relay = new Relay(new Fakes.Store(db), kafka, Fakes.fast(500), clock);
        Instant lastFetch = null;
        Duration age = Duration.ofMillis(-1);
        for (int i = 0; i < 4; i++) {
            clock.now = clock.now.plusSeconds(10); // the longest backoff by default
            assertEquals(Relay.Step.RETRY, relay.step());
            assertTrue(lastFetch == null || relay.stats().lastFetch().isAfter(lastFetch));
            assertTrue(relay.stats().oldestUnsentAge().compareTo(age) > 0);
            lastFetch = relay.stats().lastFetch();
            age = relay.stats().oldestUnsentAge();
        }
    }

    @Test
    void whileMarkingFailsTheRelayStopsReadingSoOnlyTheLastFetchTimeShowsIt() throws Exception {
        var db = new Fakes.Db().insert(10, 1);
        var store = new Fakes.Store(db);
        store.failMarks = 5; // a permission or a broken connection: Kafka has the batch, the outbox does not know
        var clock = new Tick();
        var relay = new Relay(store, new Fakes.Kafka(), Fakes.fast(500), clock);
        assertEquals(Relay.Step.RETRY, relay.step()); // fetched and published, the mark failed
        Instant fetched = relay.stats().lastFetch();
        Duration age = relay.stats().oldestUnsentAge();
        for (int i = 0; i < 4; i++) {
            clock.now = clock.now.plusSeconds(30);
            assertEquals(Relay.Step.RETRY, relay.step()); // the kept batch is marked first; nothing is read meanwhile
        }
        // Two minutes on, the age gauge has not moved (an alert on it alone stays quiet): the stale last fetch
        // is what OutboxRelayStalled pages on.
        assertEquals(fetched, relay.stats().lastFetch());
        assertEquals(age, relay.stats().oldestUnsentAge());
        Map<String, Double> m = parse(MetricsServer.render(relay, clock.instant()));
        assertEquals(120, clock.instant().getEpochSecond() - m.get("outbox_relay_last_fetch_timestamp_seconds"), 1);
        assertEquals(5, m.get("outbox_relay_database_errors_total"));
        clock.now = clock.now.plusSeconds(1);
        assertEquals(Relay.Step.PUBLISHED, relay.step()); // marked at last
        assertEquals(Relay.Step.IDLE, relay.step());
        assertTrue(relay.stats().lastFetch().isAfter(fetched));
    }

    @Test
    void everyMetricTheAlertRulesUseIsOneTheRelayServes() throws Exception {
        String rules = Files.readString(Path.of("examples/prometheus/alerts.yml"));
        var used = new TreeSet<String>();
        var name = Pattern.compile("\\boutbox_relay_[a-z_]+").matcher(rules);
        while (name.find()) {
            used.add(name.group());
        }
        var served = new TreeSet<String>();
        var relay = new Relay(new Fakes.Store(new Fakes.Db()), new Fakes.Kafka(), Fakes.fast(500));
        for (String key : parse(MetricsServer.render(relay, Instant.now())).keySet()) {
            served.add(key.replaceAll("\\{.*", ""));
        }
        assertTrue(used.size() >= 5, used.toString());
        assertTrue(served.containsAll(used), "the rules use " + used + ", the relay serves " + served);
    }

    @Test
    void aBarePortStaysOnThisMachine() {
        assertEquals("127.0.0.1", MetricsServer.parse("9464").getHostString());
        assertEquals(9464, MetricsServer.parse("9464").getPort());
        assertEquals("0.0.0.0", MetricsServer.parse("0.0.0.0:9464").getHostString());
        var any6 = MetricsServer.parse("[::]:9464").getAddress();
        assertTrue(any6 instanceof java.net.Inet6Address && any6.isAnyLocalAddress());
        for (String bad : List.of("", "x", "70000", ":9464", "host:")) {
            assertThrows(IllegalArgumentException.class, () -> MetricsServer.parse(bad), bad);
        }
    }
}
