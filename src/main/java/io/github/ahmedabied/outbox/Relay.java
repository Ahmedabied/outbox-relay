package io.github.ahmedabied.outbox;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Moves rows from the outbox to Kafka: fetch the oldest unsent rows, publish them as one Kafka transaction, mark
 * them sent. Only the relay holding the lead works; the others wait.
 *
 * <p>The rules it keeps:
 * <ul>
 *   <li>Rows are published in id order. A batch reaches consumers whole or not at all, so a retry cannot reorder.
 *   <li>A row is marked sent only after Kafka has it. A crash in between publishes the batch again: duplicates,
 *       never a loss. Consumers drop a repeat by its {@code outbox-id} header.
 *   <li>A row Kafka refuses for good (too large, say) is narrowed down by shrinking the batch, everything before it
 *       goes out, and then the relay stops and names it. It never skips a row, because what comes after it for the
 *       same key would then arrive without it.
 *   <li>Broker or network trouble is retried with backoff, forever: that is an outage, not a bad row.
 * </ul>
 */
public final class Relay {
    private static final Logger LOG = Logger.getLogger(Relay.class.getName());

    /** How the relay paces itself. */
    public record Settings(int batchSize, int maxRejections, Duration idleWait, Duration standbyWait,
                           Duration minBackoff, Duration maxBackoff) {
        public Settings {
            if (batchSize < 1 || maxRejections < 1) {
                throw new IllegalArgumentException("batchSize and maxRejections must be at least 1");
            }
        }

        public static Settings defaults() {
            return new Settings(500, 3, Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofMillis(100),
                    Duration.ofSeconds(10));
        }

        public Settings withBatchSize(int size) {
            return new Settings(size, maxRejections, idleWait, standbyWait, minBackoff, maxBackoff);
        }
    }

    /** What one step did. */
    public enum Step { STANDBY, IDLE, PUBLISHED, FULL, RETRY }

    private final OutboxStore store;
    private final Publisher publisher;
    private final Settings settings;
    private final Clock clock;
    private final Stats stats = new Stats();

    private boolean leading;
    private int limit;
    private int failuresInARow;
    private long rejectedRow = -1;
    private int rejections;
    private boolean suspectNext;
    private volatile boolean stopping;

    public Relay(OutboxStore store, Publisher publisher, Settings settings) {
        this(store, publisher, settings, Clock.systemUTC());
    }

    public Relay(OutboxStore store, Publisher publisher, Settings settings, Clock clock) {
        this.store = store;
        this.publisher = publisher;
        this.settings = settings;
        this.clock = clock;
        this.limit = settings.batchSize();
    }

    public Stats stats() {
        return stats;
    }

    public boolean leading() {
        return leading;
    }

    /** Ask {@link #run()} to return after the batch in hand. */
    public void stop() {
        stopping = true;
    }

    /** Relay until {@link #stop()}. Throws {@link RelayStoppedException} when it must not go on. */
    public void run() throws InterruptedException {
        while (!stopping) {
            switch (step()) {
                case STANDBY -> Thread.sleep(settings.standbyWait());
                case IDLE -> idle();
                case PUBLISHED, FULL, RETRY -> { }
            }
        }
    }

    /** One round: take the lead if free, then fetch, publish and mark one batch. */
    public Step step() throws InterruptedException {
        try {
            if (!leading) {
                leading = store.tryLead();
                if (!leading) {
                    return Step.STANDBY;
                }
                limit = settings.batchSize();
                LOG.info("leading: this relay publishes now");
            }
            List<OutboxRow> rows = store.fetchUnsent(limit);
            if (rows.isEmpty()) {
                return Step.IDLE;
            }
            try {
                publisher.publish(rows);
            } catch (PublishException e) {
                return failed(rows, e);
            }
            List<Long> ids = new ArrayList<>(rows.size());
            for (OutboxRow row : rows) {
                ids.add(row.id());
            }
            store.markSent(ids); // dying before this line publishes the batch again: a duplicate, never a loss
            stats.published(rows, clock.instant());
            failuresInARow = 0;
            int asked = limit;
            limit = suspectNext ? 1 : settings.batchSize(); // the row Kafka named comes next: try it alone
            suspectNext = false;
            return rows.size() == asked ? Step.FULL : Step.PUBLISHED;
        } catch (InterruptedException | RelayStoppedException e) {
            throw e;
        } catch (Exception e) { // the database: the connection, and the lead that lives on it, is gone
            if (leading) {
                LOG.log(Level.WARNING, "database trouble, giving up the lead and connecting again: " + e);
            }
            leading = false;
            store.reset();
            stats.databaseErrors.increment();
            backoff();
            return Step.RETRY;
        }
    }

    private Step failed(List<OutboxRow> rows, PublishException e) throws InterruptedException {
        switch (e.kind()) {
            case FENCED -> throw new RelayStoppedException(
                    "another relay is publishing with the same transactional id; this one stops", e);
            case RETRIABLE -> {
                stats.retries.increment();
                LOG.warning("publish failed, trying again: " + e.getMessage());
                backoff();
                return Step.RETRY;
            }
            case REJECTED -> {
                stats.rejections.increment();
                if (rows.size() == 1) {
                    long id = rows.get(0).id();
                    rejections = id == rejectedRow ? rejections + 1 : 1;
                    rejectedRow = id;
                    if (rejections >= settings.maxRejections()) {
                        throw new RelayStoppedException("row " + id + " cannot be published (" + e.getMessage()
                                + "). The relay stops rather than skip it, because rows after it for the same key"
                                + " would arrive without it. Fix or remove the row, then start the relay again.", e);
                    }
                    backoff();
                    return Step.RETRY;
                }
                int at = indexOf(rows, e.rowId());
                // Publish what comes before the bad row; then try the bad row alone. Without a hint, halve.
                limit = at > 0 ? at : at == 0 ? 1 : Math.max(1, rows.size() / 2);
                suspectNext = at > 0;
                return Step.RETRY;
            }
            default -> throw new IllegalStateException(e.kind().toString());
        }
    }

    private static int indexOf(List<OutboxRow> rows, Long id) {
        if (id == null) {
            return -1;
        }
        for (int i = 0; i < rows.size(); i++) {
            if (rows.get(i).id() == id) {
                return i;
            }
        }
        return -1;
    }

    private void idle() throws InterruptedException {
        try {
            store.awaitWork(settings.idleWait());
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            leading = false;
            store.reset();
            stats.databaseErrors.increment();
            backoff();
        }
    }

    private void backoff() throws InterruptedException {
        long min = settings.minBackoff().toMillis();
        long wait = Math.min(settings.maxBackoff().toMillis(), min << Math.min(failuresInARow, 20));
        failuresInARow++;
        Thread.sleep(wait);
    }
}
