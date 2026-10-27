package com.mahijith.secstream.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class PostgresSinkClassificationTest {
  @ParameterizedTest
  @CsvSource({
    "08006, true", // connection failure
    "40001, true", // serialization failure
    "40P01, true", // deadlock
    "53300, true", // too many connections
    "57P01, true", // admin shutdown
    "57014, true", // query cancelled (statement timeout)
    "23505, false", // unique violation
    "23514, false", // check violation
    "22P02, false", // invalid text representation
    "42P01, false" // undefined table
  })
  void classifiesBySqlState(String state, boolean retryable) {
    assertThat(PostgresSink.classify(new SQLException("x", state)).retryable())
        .isEqualTo(retryable);
  }
}
