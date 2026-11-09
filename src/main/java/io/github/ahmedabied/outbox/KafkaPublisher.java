package io.github.ahmedabied.outbox;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TransactionAbortedException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Publishes each batch as one Kafka transaction with an idempotent producer (acks=all). Consumers that read with
 * {@code isolation.level=read_committed} see a batch whole or not at all, so a failed and retried batch cannot
 * reorder or half-deliver anything. The transactional id also fences an old relay that still thinks it leads.
 */
public final class KafkaPublisher implements Publisher {
    /** Header carrying the outbox row id, for consumers to drop a repeat. */
    public static final String ID_HEADER = "outbox-id";

    private final Map<String, Object> config;
    private KafkaProducer<String, byte[]> producer;

    /**
     * @param extra any producer settings to add or override (linger.ms, compression.type, security ...)
     */
    public KafkaPublisher(String bootstrapServers, String transactionalId, Map<String, Object> extra) {
        Map<String, Object> c = new HashMap<>();
        c.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        c.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 15_000);
        c.putAll(extra);
        // Not overridable: the guarantees rest on these.
        c.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        c.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        c.put(ProducerConfig.ACKS_CONFIG, "all");
        this.config = Map.copyOf(c);
    }

    @Override
    public void publish(List<OutboxRow> rows) throws PublishException, InterruptedException {
        List<Future<RecordMetadata>> sent = new ArrayList<>(rows.size());
        try {
            if (producer == null) {
                KafkaProducer<String, byte[]> p =
                        new KafkaProducer<>(config, new StringSerializer(), new ByteArraySerializer());
                try {
                    p.initTransactions(); // fences any older producer with this id and settles its transaction
                } catch (RuntimeException e) {
                    p.close(Duration.ZERO);
                    throw e;
                }
                producer = p;
            }
            producer.beginTransaction();
            for (OutboxRow row : rows) {
                sent.add(producer.send(
                        new ProducerRecord<>(row.topic(), null, row.key(), row.payload(), headersFor(row))));
            }
            producer.commitTransaction();
        } catch (InterruptException e) {
            drop();
            Thread.currentThread().interrupt();
            throw new InterruptedException("interrupted while publishing");
        } catch (KafkaException e) {
            throw failure(rows, sent, e);
        }
    }

    /** The row's headers, then the outbox id, which no row header can replace (the schema refuses one too). */
    static RecordHeaders headersFor(OutboxRow row) {
        RecordHeaders headers = new RecordHeaders();
        row.headers().forEach((k, v) -> {
            if (!k.equals(ID_HEADER)) {
                headers.add(k, v.getBytes(StandardCharsets.UTF_8));
            }
        });
        headers.add(ID_HEADER, Long.toString(row.id()).getBytes(StandardCharsets.UTF_8));
        return headers;
    }

    private PublishException failure(List<OutboxRow> rows, List<Future<RecordMetadata>> sent, KafkaException e) {
        if (has(e, ProducerFencedException.class) || has(e, InvalidProducerEpochException.class)) {
            drop();
            return new PublishException(PublishException.Kind.FENCED, null, message(e), e);
        }
        // Which row failed first, and why. Rows after it fail only because the transaction was aborted.
        for (int i = 0; i < sent.size(); i++) {
            Future<RecordMetadata> f = sent.get(i);
            if (!f.isDone()) {
                continue;
            }
            Throwable cause;
            try {
                f.get();
                continue;
            } catch (ExecutionException ex) {
                cause = ex.getCause();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
            if (cause instanceof TransactionAbortedException) {
                continue;
            }
            abortOrDrop();
            long id = rows.get(i).id();
            PublishException.Kind kind = has(cause, RetriableException.class)
                    ? PublishException.Kind.RETRIABLE : PublishException.Kind.REJECTED;
            return new PublishException(kind, id, "row " + id + ": " + message(cause), cause);
        }
        // The commit itself failed or its outcome is unknown: start over with a fresh producer. Publishing the
        // batch again is safe; assuming it went through is not.
        drop();
        return new PublishException(PublishException.Kind.RETRIABLE, null, message(e), e);
    }

    private void abortOrDrop() {
        try {
            producer.abortTransaction();
        } catch (KafkaException e) {
            drop();
        }
    }

    private void drop() {
        if (producer != null) {
            try {
                producer.close(Duration.ofSeconds(5));
            } catch (KafkaException ignored) {
                // a producer in a fatal state may refuse even this
            }
            producer = null;
        }
    }

    private static boolean has(Throwable t, Class<? extends Throwable> type) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (type.isInstance(c)) {
                return true;
            }
        }
        return false;
    }

    private static String message(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String m = root == t ? String.valueOf(t.getMessage()) : t.getMessage() + " (" + root.getMessage() + ")";
        return root.getClass().getSimpleName() + ": " + m;
    }

    @Override
    public void close() {
        drop();
    }
}
