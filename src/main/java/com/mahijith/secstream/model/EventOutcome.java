package com.mahijith.secstream.model;

/**
 * What the observed system decided. Authentication events use SUCCESS/FAILURE, network events use
 * ALLOWED/BLOCKED, so routing rules can tell "a login failed" apart from "a packet was dropped".
 */
public enum EventOutcome {
  SUCCESS,
  FAILURE,
  ALLOWED,
  BLOCKED,
  UNKNOWN
}
