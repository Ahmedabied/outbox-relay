package io.github.ahmedabied.outbox;

import java.time.Instant;
import java.util.Map;

/** One event the application wrote to the outbox table in the same transaction as its own data. */
public record OutboxRow(long id, String topic, String key, byte[] payload, Map<String, String> headers,
                        Instant createdAt) {
    public OutboxRow {
        headers = Map.copyOf(headers);
    }
}
