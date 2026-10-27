package com.mahijith.secstream.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mahijith.secstream.delivery.EventSink;
import com.mahijith.secstream.delivery.SinkException;
import com.mahijith.secstream.model.IpAddress;
import com.mahijith.secstream.model.SecurityEvent;
import com.mahijith.secstream.processing.Json;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.flywaydb.core.Flyway;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batched, transactional, idempotent inserts into {@code security_events}.
 *
 * <ul>
 *   <li><b>One connection, no pool.</b> The sink is driven by a single dispatcher thread (see
 *       {@link EventSink}), so a pool would only add a dependency. A broken connection is closed
 *       and lazily re-opened on the next attempt.
 *   <li><b>One transaction per batch.</b> Either the whole batch commits or none of it does, so a
 *       retry never needs to reason about which rows made it.
 *   <li><b>Idempotent.</b> {@code ON CONFLICT (event_id) DO NOTHING} means a retry after an
 *       ambiguous failure (commit succeeded but the acknowledgement was lost) cannot duplicate
 *       rows. Combined with at-least-once delivery this gives effectively-once rows per event id.
 *   <li><b>Prepared statements only.</b> Event values are bound as parameters, never concatenated.
 * </ul>
 */
public final class PostgresSink implements EventSink {
  private static final Logger log = LoggerFactory.getLogger(PostgresSink.class);

  static final String INSERT_SQL =
      """
      INSERT INTO security_events (
          event_id, source_event_id, source, format, category, outcome, severity,
          occurred_at, received_at, user_id, device_id, application,
          source_ip, destination_ip, destination_port, attributes)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::inet, ?::inet, ?, ?::jsonb)
      ON CONFLICT (event_id) DO NOTHING
      """;

  private final String name;
  private final String jdbcUrl;
  private final String user;
  private final String password;
  private final Duration statementTimeout;
  private @Nullable Connection connection;

  public PostgresSink(
      String name, String jdbcUrl, String user, String password, Duration statementTimeout) {
    this.name = name;
    this.jdbcUrl = jdbcUrl;
    this.user = user;
    this.password = password;
    this.statementTimeout = statementTimeout;
  }

  @Override
  public String name() {
    return name;
  }

  /** Applies pending schema migrations, then connects. Fails fast at startup if the DB is down. */
  @Override
  public void open() throws SinkException {
    try {
      Flyway.configure()
          .dataSource(jdbcUrl, user, password)
          .locations("classpath:db/migration")
          .load()
          .migrate();
      connection();
    } catch (SQLException | RuntimeException e) {
      throw SinkException.permanentFailure("cannot initialise database " + redactedUrl(), e);
    }
  }

  @Override
  public void write(List<SecurityEvent> batch) throws SinkException {
    try {
      Connection c = connection();
      try (PreparedStatement ps = c.prepareStatement(INSERT_SQL)) {
        ps.setQueryTimeout(Math.toIntExact(Math.max(1, statementTimeout.toSeconds())));
        for (SecurityEvent e : batch) {
          bind(ps, e);
          ps.addBatch();
        }
        ps.executeBatch();
        c.commit();
      } catch (SQLException e) {
        rollbackQuietly(c);
        throw e;
      }
    } catch (SQLException e) {
      SinkException classified = classify(e);
      if (classified.retryable()) {
        closeConnection();
      }
      throw classified;
    }
  }

  private static void bind(PreparedStatement ps, SecurityEvent e) throws SQLException {
    ps.setString(1, e.id().value());
    setNullableString(ps, 2, e.sourceEventId());
    ps.setString(3, e.source());
    ps.setString(4, e.format().configName());
    ps.setString(5, e.category().name());
    ps.setString(6, e.outcome().name());
    ps.setString(7, e.severity().name());
    ps.setTimestamp(8, Timestamp.from(e.occurredAt()));
    ps.setTimestamp(9, Timestamp.from(e.receivedAt()));
    setNullableString(ps, 10, e.userId());
    setNullableString(ps, 11, e.deviceId());
    setNullableString(ps, 12, e.application());
    setNullableString(ps, 13, IpAddress.valueOf(e.sourceIp()));
    setNullableString(ps, 14, IpAddress.valueOf(e.destinationIp()));
    Integer port = e.destinationPort();
    if (port == null) {
      ps.setNull(15, Types.INTEGER);
    } else {
      ps.setInt(15, port);
    }
    try {
      ps.setString(16, Json.MAPPER.writeValueAsString(e.attributes()));
    } catch (JsonProcessingException ex) {
      throw new SQLException("attributes not serializable", "22000", ex);
    }
  }

  private static void setNullableString(PreparedStatement ps, int index, @Nullable String value)
      throws SQLException {
    if (value == null) {
      ps.setNull(index, Types.VARCHAR);
    } else {
      ps.setString(index, value);
    }
  }

  /**
   * SQLState classes (first two characters) decide retryability. Data and integrity errors are a
   * property of the event and will fail identically forever; everything else (lost connection,
   * serialization failure, server shutting down, resource exhaustion, timeouts) may succeed later.
   */
  static SinkException classify(SQLException e) {
    String state = e.getSQLState();
    String sqlClass = state == null || state.length() < 2 ? "" : state.substring(0, 2);
    String message = "postgres error sqlstate=" + state;
    return switch (sqlClass) {
      case "22", "23", "42" -> SinkException.permanentFailure(message, e);
      default -> SinkException.transientFailure(message, e);
    };
  }

  private Connection connection() throws SQLException {
    if (connection == null || connection.isClosed()) {
      Properties props = new Properties();
      props.setProperty("user", user);
      props.setProperty("password", password);
      props.setProperty("ApplicationName", "secstream");
      props.setProperty("connectTimeout", Long.toString(Math.max(1, statementTimeout.toSeconds())));
      // Rewrites a batch of single-row INSERTs into multi-row INSERTs: far fewer round trips.
      props.setProperty("reWriteBatchedInserts", "true");
      Connection c = DriverManager.getConnection(jdbcUrl, props);
      c.setAutoCommit(false);
      connection = c;
    }
    return connection;
  }

  private static void rollbackQuietly(Connection c) {
    try {
      c.rollback();
    } catch (SQLException ignored) {
      // The original failure is what matters; the connection is likely broken anyway.
    }
  }

  private void closeConnection() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        log.atDebug().setMessage("closing broken connection failed").setCause(e).log();
      } finally {
        connection = null;
      }
    }
  }

  private String redactedUrl() {
    int q = jdbcUrl.indexOf('?');
    return q < 0 ? jdbcUrl : jdbcUrl.substring(0, q);
  }

  @Override
  public void close() {
    closeConnection();
  }
}
