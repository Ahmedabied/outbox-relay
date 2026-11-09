package io.github.ahmedabied.outbox;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

/** Counters and the insert-to-published lag of the last rows, safe to read from another thread. */
public final class Stats {
    private static final int WINDOW = 1 << 16;

    public final LongAdder published = new LongAdder();
    public final LongAdder batches = new LongAdder();
    public final LongAdder retries = new LongAdder();
    public final LongAdder rejections = new LongAdder();
    public final LongAdder databaseErrors = new LongAdder();

    private final long[] lagMicros = new long[WINDOW];
    private long recorded;
    private volatile long lastFetchMillis;
    private volatile long oldestUnsentMillis;

    /** Recorded at every fetch, in the relay's own thread, so reading it never touches the database. */
    void fetched(List<OutboxRow> rows, Instant now) {
        lastFetchMillis = now.toEpochMilli();
        oldestUnsentMillis = rows.isEmpty() ? 0 : Math.max(0, Duration.between(rows.get(0).createdAt(), now).toMillis());
    }

    /** Age of the first unsent row at the last fetch; zero when the outbox was empty then. */
    public Duration oldestUnsentAge() {
        return Duration.ofMillis(oldestUnsentMillis);
    }

    /** When the relay last read the outbox, or null if it never has (a standby, say). */
    public Instant lastFetch() {
        long t = lastFetchMillis;
        return t == 0 ? null : Instant.ofEpochMilli(t);
    }

    synchronized void published(List<OutboxRow> rows, Instant now) {
        for (OutboxRow row : rows) {
            lagMicros[(int) (recorded++ & (WINDOW - 1))] = Duration.between(row.createdAt(), now).toNanos() / 1000;
        }
        published.add(rows.size());
        batches.increment();
    }

    /** Lag from the row's insert to the end of its Kafka commit, at quantile q (0.5, 0.99), over the last 65,536 rows. */
    public synchronized Duration lag(double q) {
        int n = (int) Math.min(recorded, WINDOW);
        if (n == 0) {
            return Duration.ZERO;
        }
        long[] copy = Arrays.copyOf(lagMicros, n);
        Arrays.sort(copy);
        int i = (int) Math.min(n - 1, Math.max(0, Math.ceil(q * n) - 1));
        return Duration.ofNanos(copy[i] * 1000);
    }

    @Override
    public String toString() {
        return String.format("published %d in %d batches, lag p50 %.1f ms p99 %.1f ms, retries %d, rejections %d,"
                        + " database errors %d", published.sum(), batches.sum(), lag(0.5).toNanos() / 1e6,
                lag(0.99).toNanos() / 1e6, retries.sum(), rejections.sum(), databaseErrors.sum());
    }
}
