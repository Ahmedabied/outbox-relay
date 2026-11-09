package io.github.ahmedabied.outbox;

import java.util.List;

/** Where the events go. The Kafka version is {@link KafkaPublisher}; tests use an in-memory one. */
public interface Publisher extends AutoCloseable {

    /**
     * Publish the rows as one unit: when this returns, every row is visible to consumers, in this order. When it
     * throws, none is (Kafka: the transaction was aborted, so read_committed consumers never see those rows).
     * If the outcome is unknown (a commit that timed out), it throws: publishing twice is safe, losing is not.
     */
    void publish(List<OutboxRow> rows) throws PublishException, InterruptedException;

    @Override
    void close();
}
