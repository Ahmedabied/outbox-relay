package io.github.ahmedabied.outbox;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Locale;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * {@code GET /metrics} in the Prometheus text format, from the JDK's own HTTP server. Off unless asked for; reads
 * only the relay's in-memory counters, never the database, and accepts nothing but that one GET.
 */
public final class MetricsServer implements AutoCloseable {
    private final HttpServer server;

    public MetricsServer(InetSocketAddress address, Relay relay) throws IOException {
        server = HttpServer.create(address, 8);
        server.createContext("/", exchange -> handle(exchange, relay));
        server.start();
    }

    /**
     * {@code 9464} means 127.0.0.1:9464: the counters stay on the machine unless an address says otherwise
     * ({@code 0.0.0.0:9464}, {@code [::]:9464}).
     */
    public static InetSocketAddress parse(String addr) {
        String host = "127.0.0.1";
        String port = addr.trim();
        int colon = port.lastIndexOf(':');
        if (colon >= 0) {
            host = port.substring(0, colon);
            port = port.substring(colon + 1);
            if (host.startsWith("[") && host.endsWith("]")) {
                host = host.substring(1, host.length() - 1);
            }
        }
        try {
            int p = Integer.parseInt(port);
            if (p < 0 || p > 65_535 || host.isEmpty()) {
                throw new NumberFormatException();
            }
            return new InetSocketAddress(host, p);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("metrics address must be PORT or HOST:PORT, not " + addr);
        }
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private static void handle(HttpExchange exchange, Relay relay) throws IOException {
        try (exchange) {
            int status;
            byte[] body;
            if (!exchange.getRequestURI().getPath().equals("/metrics")) {
                status = 404;
                body = "not found\n".getBytes(StandardCharsets.UTF_8);
            } else if (!exchange.getRequestMethod().equals("GET")) {
                status = 405;
                body = "GET only\n".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Allow", "GET");
            } else {
                status = 200;
                body = render(relay, Instant.now()).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            }
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
    }

    static String render(Relay relay, Instant now) {
        Stats s = relay.stats();
        StringBuilder b = new StringBuilder(1024);
        metric(b, "published_total", "counter", "Rows published to Kafka and marked sent.", s.published.sum());
        metric(b, "batches_total", "counter", "Kafka transactions committed.", s.batches.sum());
        metric(b, "retries_total", "counter", "Batches retried after broker or network trouble.", s.retries.sum());
        metric(b, "rejections_total", "counter", "Batches Kafka refused because of a row in them.",
                s.rejections.sum());
        metric(b, "database_errors_total", "counter", "Database errors; each one drops and rebuilds the connection.",
                s.databaseErrors.sum());
        metric(b, "leading", "gauge", "1 while this relay holds the lead and publishes.", relay.leading() ? 1 : 0);
        b.append("# HELP outbox_relay_lag_seconds Insert to Kafka commit, over the last 65,536 rows.\n")
                .append("# TYPE outbox_relay_lag_seconds gauge\n");
        for (double q : new double[] {0.5, 0.99}) {
            b.append("outbox_relay_lag_seconds{quantile=\"").append(q).append("\"} ")
                    .append(seconds(s.lag(q).toNanos() / 1e9)).append('\n');
        }
        metric(b, "oldest_unsent_age_seconds", "gauge",
                "Age of the first unsent row at the last fetch; 0 when the outbox was empty then.",
                s.oldestUnsentAge().toMillis() / 1e3);
        Instant last = s.lastFetch();
        metric(b, "last_fetch_timestamp_seconds", "gauge",
                "When the relay last read the outbox (unix time); 0 if it never has.",
                last == null ? 0 : last.toEpochMilli() / 1e3);
        return b.toString();
    }

    private static void metric(StringBuilder b, String name, String type, String help, double value) {
        b.append("# HELP outbox_relay_").append(name).append(' ').append(help).append('\n')
                .append("# TYPE outbox_relay_").append(name).append(' ').append(type).append('\n')
                .append("outbox_relay_").append(name).append(' ').append(seconds(value)).append('\n');
    }

    private static String seconds(double v) {
        return v == Math.rint(v) && Math.abs(v) < 1e15 ? Long.toString((long) v) : String.format(Locale.ROOT, "%.6f", v);
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
