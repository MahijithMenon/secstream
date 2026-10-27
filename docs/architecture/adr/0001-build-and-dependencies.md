# ADR 0001: Maven, Java 21, minimal dependencies

Status: accepted (2026-10-27)

## Context
The repository was empty. Maven 3.9.14 and Temurin/Homebrew JDK 21.0.10 are installed; Gradle is not.

## Decision
- **Maven**, Java `release 21`, enforced by `maven-enforcer-plugin` (`[21,22)`), plus `requirePluginVersions`, `dependencyConvergence`, `banDuplicatePomDependencyVersions`. Every plugin and dependency version is explicit; `project.build.outputTimestamp` is fixed for reproducible jars. Maven has no native lockfile; explicit versions + convergence + Dependabot are the reproducibility controls.
- Dependencies (latest stable on Maven Central as of 2026-10-27):
  - Jackson 2.22.4 (databind + YAML). Jackson 3.x exists (3.2.4) but 2.x is still maintained and has the wider ecosystem; migration is a mechanical follow-up.
  - PostgreSQL JDBC 42.7.14, Flyway 13.11.1 (schema migrations).
  - SLF4J 2.0.20 + Logback 1.6.6 (fluent key-value API for structured logs).
  - HdrHistogram 2.2.2 (wait-free latency recording; writing a correct concurrent histogram is not where this project should spend effort).
  - JSpecify 1.0.1 (`@Nullable`, which SpotBugs also understands).
  - Tests: JUnit 6.1.3, AssertJ 3.27.7, Awaitility 4.3.0, Testcontainers 2.0.5.
- **Not** used: Spring/Guice (explicit wiring in `PipelineAssembler` is ~150 lines), Javalin (JDK `HttpServer` suffices for one endpoint), Micrometer (a hand-written Prometheus text renderer is ~100 lines and has no global registry), connection pools (one writer thread per sink).
- Quality: Spotless + google-java-format **1.36.0** (1.37.0 fails inside Spotless 3.10.4 with a `NoClassDefFoundError` for its commonmark dependency), SpotBugs 4.10.5 (`Max` effort, `Medium` threshold, documented exclusions in `config/spotbugs-exclude.xml`), `-Xlint:all -Werror`.
- Vulnerability scanning: Dependabot (weekly). OWASP dependency-check was not added: it needs an NVD API key and takes minutes per run.

## Consequences
`mvn verify` is the single gate (compile, unit tests, ITs, format, SpotBugs). Integration tests need Docker and are skipped, not failed, without it.
