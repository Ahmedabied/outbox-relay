package io.github.ahmedabied.outbox;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

class KafkaPublisherTest {

    @Test
    void aRowHeaderCannotPassItselfOffAsTheOutboxId() {
        var row = new OutboxRow(42, "orders", "k", new byte[0],
                Map.of("type", "OrderPlaced", KafkaPublisher.ID_HEADER, "1"), Instant.now());
        var headers = KafkaPublisher.headersFor(row);
        int ids = 0;
        for (Header h : headers) {
            ids += h.key().equals(KafkaPublisher.ID_HEADER) ? 1 : 0;
        }
        assertEquals(1, ids);
        assertEquals("42", new String(headers.lastHeader(KafkaPublisher.ID_HEADER).value(), StandardCharsets.UTF_8));
        assertEquals("OrderPlaced", new String(headers.lastHeader("type").value(), StandardCharsets.UTF_8));
    }
}
