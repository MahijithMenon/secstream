package com.mahijith.secstream.config;

import com.mahijith.secstream.config.SecStreamConfig.ConsoleSinkConfig;
import com.mahijith.secstream.config.SecStreamConfig.DiscardSinkConfig;
import com.mahijith.secstream.config.SecStreamConfig.FileSinkConfig;
import com.mahijith.secstream.config.SecStreamConfig.FileSourceConfig;
import com.mahijith.secstream.config.SecStreamConfig.GeneratorSourceConfig;
import com.mahijith.secstream.config.SecStreamConfig.HttpSourceConfig;
import com.mahijith.secstream.config.SecStreamConfig.PostgresSinkConfig;
import com.mahijith.secstream.config.SecStreamConfig.RetryConfig;
import com.mahijith.secstream.config.SecStreamConfig.RouteConfig;
import com.mahijith.secstream.config.SecStreamConfig.SinkConfig;
import com.mahijith.secstream.config.SecStreamConfig.SourceConfig;
import com.mahijith.secstream.model.EventCategory;
import com.mahijith.secstream.model.EventFormat;
import com.mahijith.secstream.model.EventOutcome;
import com.mahijith.secstream.model.Severity;
import com.mahijith.secstream.processing.ProcessingStages;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

/** Collects every configuration problem with a path-like location, e.g. {@code sinks[1] (db)}. */
final class ConfigValidator {
  private static final Pattern NAME = Pattern.compile("^[a-z][a-z0-9-]{0,62}$");
  private static final int MAX_WORKERS = 1024;
  private static final int MAX_QUEUE = 10_000_000;
  private static final int MAX_BODY = 64 * 1024 * 1024;

  private final List<String> problems = new ArrayList<>();
  private final Function<String, String> env;

  private ConfigValidator(Function<String, String> env) {
    this.env = env;
  }

  static List<String> validate(SecStreamConfig c, Function<String, String> env) {
    ConfigValidator v = new ConfigValidator(env);
    v.pipeline(c);
    v.processing(c);
    v.sources(c.sources());
    Set<String> sinkNames = v.sinks(c.sinks());
    v.routes(c.routes(), sinkNames);
    v.metrics(c);
    return List.copyOf(v.problems);
  }

  private void pipeline(SecStreamConfig c) {
    var p = c.pipeline();
    range("pipeline.workers", p.workers(), 1, MAX_WORKERS);
    range("pipeline.queueCapacity", p.queueCapacity(), 1, MAX_QUEUE);
    range("pipeline.maxPayloadBytes", p.maxPayloadBytes(), 64, MAX_BODY);
    duration("pipeline.shutdownTimeout", p.shutdownTimeout());
  }

  private void processing(SecStreamConfig c) {
    var p = c.processing();
    severity("processing.minSeverity", p.minSeverity());
    range("processing.dedupCapacity", p.dedupCapacity(), 64, MAX_QUEUE);
    for (String e : p.enrichers()) {
      if (!ProcessingStages.ENRICHERS.contains(e)) {
        problems.add(
            "processing.enrichers: unknown enricher '"
                + e
                + "', known: "
                + ProcessingStages.ENRICHERS);
      }
    }
  }

  private void sources(List<SourceConfig> sources) {
    if (sources.isEmpty()) {
      problems.add("sources: at least one source is required");
    }
    Set<String> names = new HashSet<>();
    for (int i = 0; i < sources.size(); i++) {
      SourceConfig s = sources.get(i);
      String at = "sources[" + i + "]" + (s.name() == null ? "" : " (" + s.name() + ")");
      name(at, s.name(), names);
      switch (s) {
        case FileSourceConfig f -> {
          required(at + ".path", f.path());
          format(at + ".format", f.format());
        }
        case GeneratorSourceConfig g -> {
          if (g.count() < 0) {
            problems.add(at + ".count: must be >= 0");
          }
          range(at + ".ratePerSecond", g.ratePerSecond(), 0, 10_000_000);
          fraction(at + ".firewallFraction", g.firewallFraction());
          fraction(at + ".failureFraction", g.failureFraction());
          fraction(at + ".duplicateFraction", g.duplicateFraction());
          fraction(at + ".malformedFraction", g.malformedFraction());
          if (g.duplicateFraction() + g.malformedFraction() > 1.0) {
            problems.add(at + ": duplicateFraction + malformedFraction must be <= 1");
          }
        }
        case HttpSourceConfig h -> {
          required(at + ".host", h.host());
          range(at + ".port", h.port(), 0, 65_535);
          range(at + ".maxBodyBytes", h.maxBodyBytes(), 64, MAX_BODY);
          range(at + ".maxEventsPerRequest", h.maxEventsPerRequest(), 1, 100_000);
          range(at + ".threads", h.threads(), 1, 256);
          duration(at + ".admissionTimeout", h.admissionTimeout());
          if (h.authTokenEnv() != null) {
            secret(at + ".authTokenEnv", h.authTokenEnv());
          }
        }
      }
    }
  }

  private Set<String> sinks(List<SinkConfig> sinks) {
    if (sinks.isEmpty()) {
      problems.add("sinks: at least one sink is required");
    }
    Set<String> names = new HashSet<>();
    for (int i = 0; i < sinks.size(); i++) {
      SinkConfig s = sinks.get(i);
      String at = "sinks[" + i + "]" + (s.name() == null ? "" : " (" + s.name() + ")");
      name(at, s.name(), names);
      range(at + ".delivery.queueCapacity", s.delivery().queueCapacity(), 1, MAX_QUEUE);
      range(at + ".delivery.batchSize", s.delivery().batchSize(), 1, 10_000);
      retry(at + ".delivery.retry", s.delivery().retry());
      switch (s) {
        case ConsoleSinkConfig ignored -> {}
        case DiscardSinkConfig ignored -> {}
        case FileSinkConfig f -> required(at + ".path", f.path());
        case PostgresSinkConfig p -> {
          String url = p.url();
          if (url == null || !url.startsWith("jdbc:postgresql://")) {
            problems.add(at + ".url: must start with jdbc:postgresql://");
          }
          required(at + ".user", p.user());
          if (required(at + ".passwordEnv", p.passwordEnv())) {
            secret(at + ".passwordEnv", p.passwordEnv());
          }
          duration(at + ".statementTimeout", p.statementTimeout());
        }
      }
    }
    return names;
  }

  private void retry(String at, RetryConfig r) {
    range(at + ".maxAttempts", r.maxAttempts(), 1, 100);
    Duration initial = duration(at + ".initialBackoff", r.initialBackoff());
    Duration max = duration(at + ".maxBackoff", r.maxBackoff());
    if (initial != null && max != null && max.compareTo(initial) < 0) {
      problems.add(at + ": maxBackoff must be >= initialBackoff");
    }
    if (!(r.multiplier() >= 1.0 && r.multiplier() <= 10.0)) {
      problems.add(at + ".multiplier: must be within 1.0..10.0");
    }
  }

  private void routes(List<RouteConfig> routes, Set<String> sinkNames) {
    if (routes.isEmpty()) {
      problems.add("routes: at least one route is required, otherwise every event is unrouted");
    }
    Set<String> names = new HashSet<>();
    for (int i = 0; i < routes.size(); i++) {
      RouteConfig r = routes.get(i);
      String at = "routes[" + i + "]" + (r.name() == null ? "" : " (" + r.name() + ")");
      name(at, r.name(), names);
      severity(at + ".minSeverity", r.minSeverity());
      for (String category : r.categories()) {
        enumValue(at + ".categories", category, EventCategory.class);
      }
      for (String outcome : r.outcomes()) {
        enumValue(at + ".outcomes", outcome, EventOutcome.class);
      }
      if (r.sinks().isEmpty()) {
        problems.add(at + ".sinks: at least one sink is required");
      }
      for (String sink : r.sinks()) {
        if (!sinkNames.contains(sink)) {
          problems.add(at + ".sinks: unknown sink '" + sink + "', defined sinks: " + sinkNames);
        }
      }
    }
  }

  private void metrics(SecStreamConfig c) {
    var m = c.metrics();
    required("metrics.host", m.host());
    range("metrics.port", m.port(), 0, 65_535);
    duration("metrics.logInterval", m.logInterval());
  }

  private void name(String at, @Nullable String name, Set<String> seen) {
    if (name == null || !NAME.matcher(name).matches()) {
      problems.add(
          at + ".name: required, lowercase letters, digits and '-', starting with a letter");
    } else if (!seen.add(name)) {
      problems.add(at + ".name: duplicate name '" + name + "'");
    }
  }

  private boolean required(String at, @Nullable String value) {
    if (value == null || value.isBlank()) {
      problems.add(at + ": required");
      return false;
    }
    return true;
  }

  private void range(String at, int value, int min, int max) {
    if (value < min || value > max) {
      problems.add(at + ": " + value + " is outside " + min + ".." + max);
    }
  }

  private void fraction(String at, double value) {
    if (!(value >= 0.0 && value <= 1.0)) {
      problems.add(at + ": must be within 0.0..1.0");
    }
  }

  private @Nullable Duration duration(String at, String value) {
    try {
      return Durations.parse(value);
    } catch (IllegalArgumentException e) {
      problems.add(at + ": " + e.getMessage());
      return null;
    }
  }

  private void severity(String at, String value) {
    try {
      Severity.parse(value);
    } catch (IllegalArgumentException e) {
      problems.add(at + ": " + e.getMessage());
    }
  }

  private void format(String at, @Nullable String value) {
    if (required(at, value)) {
      try {
        EventFormat.fromConfigName(value);
      } catch (IllegalArgumentException e) {
        problems.add(at + ": " + e.getMessage());
      }
    }
  }

  private <E extends Enum<E>> void enumValue(String at, String value, Class<E> type) {
    try {
      Enum.valueOf(type, value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      problems.add(
          at
              + ": unknown value '"
              + value
              + "', expected one of "
              + Arrays.toString(type.getEnumConstants()));
    }
  }

  private void secret(String at, String envName) {
    String value = env.apply(envName);
    if (value == null || value.isEmpty()) {
      problems.add(at + ": environment variable " + envName + " is not set");
    }
  }
}
