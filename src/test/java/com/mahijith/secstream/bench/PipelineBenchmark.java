package com.mahijith.secstream.bench;

import com.mahijith.secstream.delivery.DeadLetterChannel;
import com.mahijith.secstream.delivery.EventSink;
import com.mahijith.secstream.delivery.InMemoryDeadLetterStore;
import com.mahijith.secstream.delivery.RetryPolicy;
import com.mahijith.secstream.delivery.SinkDispatcher;
import com.mahijith.secstream.delivery.Sleeper;
import com.mahijith.secstream.metrics.MetricsSnapshot;
import com.mahijith.secstream.metrics.PipelineMetrics;
import com.mahijith.secstream.metrics.SinkMetrics;
import com.mahijith.secstream.model.Severity;
import com.mahijith.secstream.pipeline.Admission;
import com.mahijith.secstream.pipeline.Pipeline;
import com.mahijith.secstream.pipeline.PipelineSettings;
import com.mahijith.secstream.processing.Deduplicator;
import com.mahijith.secstream.processing.EventProcessor;
import com.mahijith.secstream.processing.NetworkZoneEnricher;
import com.mahijith.secstream.processing.SeverityFilter;
import com.mahijith.secstream.routing.EventMatcher;
import com.mahijith.secstream.routing.Router;
import com.mahijith.secstream.routing.RoutingRule;
import com.mahijith.secstream.sink.DiscardingSink;
import com.mahijith.secstream.sink.JsonLinesFileSink;
import com.mahijith.secstream.source.GeneratorSettings;
import com.mahijith.secstream.source.SyntheticEventGenerator;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * End-to-end throughput and latency at 1, 2, 4 and 8 workers. Not JMH: the question is how the
 * whole pipeline (queues, threads, sinks) scales, not how fast one method is. See
 * docs/BENCHMARKS.md for methodology.
 *
 * <p>System properties: {@code bench.events} (default 500000), {@code bench.runs} (5), {@code
 * bench.warmups} (2), {@code bench.sink} ({@code discard} or {@code file}), {@code bench.producers}
 * (2), {@code bench.queue} (10000).
 */
public final class PipelineBenchmark {
  private static final int EVENTS = Integer.getInteger("bench.events", 500_000);
  private static final int RUNS = Integer.getInteger("bench.runs", 5);
  private static final int WARMUPS = Integer.getInteger("bench.warmups", 2);
  private static final int PRODUCERS = Integer.getInteger("bench.producers", 2);
  private static final int QUEUE = Integer.getInteger("bench.queue", 10_000);
  private static final String SINK = System.getProperty("bench.sink", "discard");
  private static final int[] WORKERS = {1, 2, 4, 8};

  private record Result(
      double eventsPerSecond, long p50, long p95, long p99, boolean balanced, long deadLetters) {}

  private PipelineBenchmark() {}

  public static void main(String[] args) throws Exception {
    List<List<SyntheticEventGenerator.Generated>> streams = generate();
    long bytes = streams.stream().flatMap(List::stream).mapToLong(g -> g.payload().length()).sum();
    StringBuilder report = new StringBuilder();
    report.append(
        String.format(
            "JDK %s (%s), %d available processors, max heap %d MiB%n",
            Runtime.version(),
            System.getProperty("java.vm.name"),
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().maxMemory() / (1024 * 1024)));
    report.append(
        String.format(
            "events=%d producers=%d queue=%d sink=%s batch=500 avgPayload=%d chars warmups=%d runs=%d%n%n",
            EVENTS, PRODUCERS, QUEUE, SINK, bytes / EVENTS, WARMUPS, RUNS));
    report.append(
        "| workers | median events/s | min | max | p50 µs | p95 µs | p99 µs | accounting balanced | dead letters |\n");
    report.append("|---:|---:|---:|---:|---:|---:|---:|:---:|---:|\n");
    for (int workers : WORKERS) {
      for (int i = 0; i < WARMUPS; i++) {
        runOnce(workers, streams);
      }
      List<Result> results = new ArrayList<>();
      for (int i = 0; i < RUNS; i++) {
        results.add(runOnce(workers, streams));
      }
      double[] tput = results.stream().mapToDouble(Result::eventsPerSecond).sorted().toArray();
      Result median =
          results.stream()
              .sorted((a, b) -> Double.compare(a.eventsPerSecond, b.eventsPerSecond))
              .toList()
              .get(results.size() / 2);
      boolean balanced = results.stream().allMatch(Result::balanced);
      long dead = results.stream().mapToLong(Result::deadLetters).sum();
      report.append(
          String.format(
              "| %d | %,.0f | %,.0f | %,.0f | %d | %d | %d | %s | %d |%n",
              workers,
              tput[tput.length / 2],
              tput[0],
              tput[tput.length - 1],
              median.p50,
              median.p95,
              median.p99,
              balanced ? "yes" : "NO",
              dead));
      System.err.println("done workers=" + workers + " " + Arrays.toString(tput));
    }
    System.out.println(report);
    Path out = Path.of("target", "bench-" + SINK + ".md");
    Files.createDirectories(out.getParent());
    Files.writeString(out, report);
  }

  /** Pre-generated so payload construction is not measured. 1% malformed, 2% duplicates. */
  private static List<List<SyntheticEventGenerator.Generated>> generate() {
    List<List<SyntheticEventGenerator.Generated>> streams = new ArrayList<>();
    for (int p = 0; p < PRODUCERS; p++) {
      int n = EVENTS / PRODUCERS;
      SyntheticEventGenerator g =
          new SyntheticEventGenerator(
              new GeneratorSettings(n, 0, 0.5, 0.2, 0.02, 0.01, 500 + p), Instant.now());
      List<SyntheticEventGenerator.Generated> events = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        events.add(g.next());
      }
      streams.add(events);
    }
    return streams;
  }

  private static Result runOnce(int workers, List<List<SyntheticEventGenerator.Generated>> streams)
      throws Exception {
    PipelineMetrics metrics = new PipelineMetrics();
    DeadLetterChannel dlq = new DeadLetterChannel(new InMemoryDeadLetterStore(), metrics);
    Path tmp = Files.createTempFile("secstream-bench", ".jsonl");
    EventSink sink =
        "file".equals(SINK) ? new JsonLinesFileSink("out", tmp, false) : new DiscardingSink("out");
    SinkDispatcher dispatcher =
        new SinkDispatcher(
            sink,
            QUEUE,
            500,
            RetryPolicy.NONE,
            dlq,
            metrics.sink("out"),
            Sleeper.SYSTEM,
            new SplittableRandom(1),
            Clock.systemUTC());
    EventProcessor processor =
        new EventProcessor(
            EventProcessor.defaultNormalizers(),
            new SeverityFilter(Severity.INFO),
            new Deduplicator(1_000_000),
            List.of(new NetworkZoneEnricher()),
            new Router(List.of(new RoutingRule("all", EventMatcher.ANY, List.of("out")))));
    Pipeline pipeline =
        new Pipeline(
            new PipelineSettings(workers, QUEUE, 64 * 1024),
            processor,
            List.of(dispatcher),
            dlq,
            metrics,
            Clock.systemUTC());
    pipeline.start();

    CyclicBarrier start = new CyclicBarrier(streams.size() + 1);
    ExecutorService producers = Executors.newFixedThreadPool(streams.size());
    List<Future<?>> futures = new ArrayList<>();
    for (List<SyntheticEventGenerator.Generated> stream : streams) {
      futures.add(
          producers.submit(
              () -> {
                start.await();
                for (SyntheticEventGenerator.Generated g : stream) {
                  if (pipeline.submit("bench", g.format(), g.payload()) != Admission.ACCEPTED) {
                    throw new IllegalStateException("rejected during benchmark");
                  }
                }
                return null;
              }));
    }
    start.await();
    long t0 = System.nanoTime();
    for (Future<?> f : futures) {
      f.get();
    }
    pipeline.shutdown(
        Duration.ofMinutes(5)); // drains everything: the clock stops at the last write
    long elapsed = System.nanoTime() - t0;
    producers.shutdown();
    deleteQuietly(tmp);

    MetricsSnapshot s = metrics.snapshot();
    SinkMetrics.SinkSnapshot k = s.sinks().get("out");
    return new Result(
        s.accepted() * 1e9 / elapsed,
        k.deliveryLatency().p50Micros(),
        k.deliveryLatency().p95Micros(),
        k.deliveryLatency().p99Micros(),
        s.processingBalanced() && s.deliveriesBalanced() && s.accepted() == EVENTS,
        s.deadLettersTotal());
  }

  private static void deleteQuietly(Path p) {
    try {
      Files.deleteIfExists(p);
    } catch (IOException ignored) {
      // temp file; the OS cleans up eventually
    }
  }
}
