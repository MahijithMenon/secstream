package com.mahijith.secstream.delivery;

/**
 * A sink write failure, classified by whether retrying can help. The classification is the sink's
 * responsibility because only it knows its failure modes (e.g. SQLState 08xxx is a lost connection,
 * 23xxx a constraint violation).
 */
public final class SinkException extends Exception {
  private static final long serialVersionUID = 1L;

  private final boolean retryable;

  private SinkException(String message, Throwable cause, boolean retryable) {
    super(message, cause);
    this.retryable = retryable;
  }

  public static SinkException transientFailure(String message, Throwable cause) {
    return new SinkException(message, cause, true);
  }

  public static SinkException permanentFailure(String message, Throwable cause) {
    return new SinkException(message, cause, false);
  }

  public boolean retryable() {
    return retryable;
  }
}
