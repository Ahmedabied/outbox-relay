package io.github.ahmedabied.outbox;

/** The relay stopped on purpose rather than skip a row or publish out of order. */
public final class RelayStoppedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public RelayStoppedException(String message, Throwable cause) {
        super(message, cause);
    }
}
