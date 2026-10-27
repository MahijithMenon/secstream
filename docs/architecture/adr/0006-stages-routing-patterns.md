# ADR 0006: Stage order, fan-out routing, and which patterns are used

Status: accepted (2026-10-27)

## Decision
- **Order:** normalize (parse + map + validate) → severity filter → dedup → enrich → route. The guide's separate "parse" and "normalize" stages merge, because severity is derived during normalization and filtering needs it; keeping raw JSON trees between stages would leak format details through the pipeline.
- **Router is fan-out, not Chain of Responsibility:** every matching rule adds its sinks (deduplicated, rule order kept). The motivating rule set is "failed sign-ins → alerts **and** everything → archive".
- Patterns, used where they solve a real problem:
  - *Strategy*: `EventNormalizer` per format, `EventEnricher`, `EventSink`, `RetryPolicy`, `Sleeper` (testability).
  - *Builder*: `SecurityEvent` (16 fields, many optional).
  - *Factory*: `PipelineAssembler` (config → concrete sources/sinks).
  - *Producer–consumer / poison pill*: queues and end-of-stream markers.
- Not used: *Observer* for metrics (direct calls to a thread-safe `PipelineMetrics` are simpler and cheaper; there is one consumer), generic `EventFilter` chains (two filters with different outcomes, so they are explicit), DI frameworks.
