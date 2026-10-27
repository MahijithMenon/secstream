package com.mahijith.secstream.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.mahijith.secstream.delivery.SinkException;
import com.mahijith.secstream.model.IpAddress;
import com.mahijith.secstream.testing.TestEvents;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Runs against a disposable PostgreSQL. Skipped (not failed) when Docker is unavailable. */
@Testcontainers(disabledWithoutDocker = true)
class PostgresSinkIT {
  @Container
  static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

  private PostgresSink sink() {
    return new PostgresSink(
        "db",
        POSTGRES.getJdbcUrl(),
        POSTGRES.getUsername(),
        POSTGRES.getPassword(),
        Duration.ofSeconds(5));
  }

  private long count(String where) throws Exception {
    try (Connection c =
            DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT count(*) FROM security_events WHERE " + where)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  @Test
  void migratesBatchInsertsAndIsIdempotentOnRetry() throws Exception {
    PostgresSink sink = sink();
    sink.open();
    List<com.mahijith.secstream.model.SecurityEvent> batch = new ArrayList<>();
    for (int i = 0; i < 1_000; i++) {
      batch.add(
          TestEvents.event("pg" + i)
              .destinationIp(IpAddress.parse("2001:db8::" + Integer.toHexString(i + 1)))
              .destinationPort(443)
              .attribute("n", Integer.toString(i))
              .build());
    }
    sink.write(batch);
    sink.write(batch.subList(0, 10)); // a retry after an ambiguous failure
    sink.close();

    assertThat(count("source = 'test' AND user_id LIKE 'user-pg%'")).isEqualTo(1_000);
    assertThat(count("attributes->>'n' = '999' AND destination_ip = '2001:db8::3e8'::inet"))
        .isEqualTo(1);
  }

  @Test
  void reopeningAppliesNoMigrationTwice() throws Exception {
    PostgresSink a = sink();
    a.open();
    a.close();
    PostgresSink b = sink();
    b.open();
    b.write(List.of(TestEvents.event("again").build()));
    b.close();
    assertThat(count("user_id = 'user-again'")).isEqualTo(1);
  }

  @Test
  void hostileValuesAreStoredLiterallyViaBindParameters() throws Exception {
    PostgresSink sink = sink();
    sink.open();
    sink.write(
        List.of(
            TestEvents.event("inj").application("x'); DROP TABLE security_events; --").build()));
    sink.close();
    assertThat(count("application LIKE 'x''); DROP%'")).isEqualTo(1);
  }

  @Test
  void unreachableDatabaseFailsAtOpen() {
    PostgresSink bad =
        new PostgresSink(
            "db", "jdbc:postgresql://127.0.0.1:1/none", "u", "p", Duration.ofSeconds(1));
    assertThatThrownBy(bad::open).isInstanceOf(SinkException.class);
  }
}
