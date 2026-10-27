package com.mahijith.secstream.model;

/** Hard size limits that bound per-event memory regardless of what an untrusted source sends. */
public final class EventLimits {
  public static final int MAX_TEXT_LENGTH = 256;
  public static final int MAX_ATTRIBUTES = 32;
  public static final int MAX_ATTRIBUTE_KEY_LENGTH = 64;
  public static final int MAX_ATTRIBUTE_VALUE_LENGTH = 512;

  private EventLimits() {}
}
