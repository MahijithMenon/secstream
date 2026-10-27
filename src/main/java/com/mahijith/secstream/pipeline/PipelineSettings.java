package com.mahijith.secstream.pipeline;

/**
 * @param workers worker threads running the CPU-bound processing stages
 * @param queueCapacity bound of the ingest queue; producers block or are rejected beyond it
 * @param maxPayloadChars events larger than this are rejected at admission
 */
public record PipelineSettings(int workers, int queueCapacity, int maxPayloadChars) {
  public PipelineSettings {
    if (workers < 1 || workers > 1024) {
      throw new IllegalArgumentException("workers must be within 1..1024");
    }
    if (queueCapacity < 1) {
      throw new IllegalArgumentException("queueCapacity must be >= 1");
    }
    if (maxPayloadChars < 1) {
      throw new IllegalArgumentException("maxPayloadChars must be >= 1");
    }
  }
}
