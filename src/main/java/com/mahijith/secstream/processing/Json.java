package com.mahijith.secstream.processing;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

/** The single, shared JSON mapper. ObjectMapper is thread-safe once configured. */
public final class Json {
  /** Deepest nesting we accept; real events are two levels deep at most. */
  private static final int MAX_NESTING_DEPTH = 16;

  public static final ObjectMapper MAPPER =
      JsonMapper.builder(
              JsonFactory.builder()
                  .streamReadConstraints(
                      StreamReadConstraints.builder().maxNestingDepth(MAX_NESTING_DEPTH).build())
                  .build())
          .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
          .build();

  private Json() {}
}
