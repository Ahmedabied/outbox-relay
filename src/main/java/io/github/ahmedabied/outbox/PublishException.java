package io.github.ahmedabied.outbox;

/** A batch that did not reach consumers. */
public final class PublishException extends Exception {
    private static final long serialVersionUID = 1L;

    /** What the relay should do about it. */
    public enum Kind {
        /** Worth trying again as it is: the broker or network had a moment. */
        RETRIABLE,
        /** Will fail again the same way: some row in the batch cannot be published. */
        REJECTED,
        /** Another relay with the same transactional id took over. This one must stop. */
        FENCED
    }

    private final Kind kind;
    private final Long rowId;

    public PublishException(Kind kind, Long rowId, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
        this.rowId = rowId;
    }

    public Kind kind() {
        return kind;
    }

    /** The row that caused it, when the publisher can tell. */
    public Long rowId() {
        return rowId;
    }
}
