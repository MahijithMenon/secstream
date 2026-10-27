package com.mahijith.secstream.pipeline;

/**
 * @param graceful true if every queued and in-flight event finished normally within the timeout;
 *     false if a forced stop dead-lettered work as SHUTDOWN_ABANDONED
 */
public record ShutdownReport(boolean graceful, long abandonedBeforeProcessing) {}
