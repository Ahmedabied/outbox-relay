package io.github.ahmedabied.outbox;

import java.time.Duration;
import java.util.List;

/** Where the events wait. The Postgres version is {@link PgOutboxStore}; tests use an in-memory one. */
public interface OutboxStore extends AutoCloseable {

    /** Try to become the one active relay. Stays true while this store keeps its connection. */
    boolean tryLead() throws Exception;

    /** Unsent rows in id order, at most limit. */
    List<OutboxRow> fetchUnsent(int limit) throws Exception;

    /** Record that these rows, fetched on this same connection moments ago, are in Kafka. */
    void markSent(List<OutboxRow> rows) throws Exception;

    /**
     * The same for rows fetched before the connection was lost: each must also match its creation time, so a row
     * that is gone, or an id used again after the identity was reset, is never marked, and a row another relay has
     * marked since stays as it was. Slower than {@link #markSent}, so only for this rare case.
     */
    void markSentAfterReconnect(List<OutboxRow> rows) throws Exception;

    /** Wait until new rows may have arrived, or until the timeout. */
    void awaitWork(Duration timeout) throws Exception;

    /** Drop the connection (and with it the lead); the next call connects again. */
    void reset();

    @Override
    void close();
}
