package com.mahijith.secstream.pipeline;

/** Result of offering an event to the pipeline. Only ACCEPTED transfers responsibility. */
public enum Admission {
  ACCEPTED,
  /** The ingest queue stayed full for the whole admission timeout. The caller should retry. */
  REJECTED_QUEUE_FULL,
  /** The payload exceeds the configured size limit. Retrying will not help. */
  REJECTED_TOO_LARGE,
  /** The pipeline is shutting down. */
  REJECTED_CLOSED
}
