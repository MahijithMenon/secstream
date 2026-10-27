package com.mahijith.secstream.model;

/**
 * An event that can never be processed successfully, no matter how often it is retried. The message
 * is safe to log: it names fields and constraints, never field values.
 */
public final class InvalidEventException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public InvalidEventException(String message) {
    super(message);
  }

  public InvalidEventException(String message, Throwable cause) {
    super(message, cause);
  }
}
