package io.github.ahmedabied.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
