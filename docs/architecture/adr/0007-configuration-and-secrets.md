# ADR 0007: Strict YAML configuration, secrets from the environment

Status: accepted (2026-10-27)

## Decision
- Jackson YAML into records with `FAIL_ON_UNKNOWN_PROPERTIES`; polymorphic `type:` for sources and sinks; defaults applied in record constructors.
- `ConfigValidator` collects **all** problems with paths (`sinks[1] (db).passwordEnv: environment variable X is not set`) and the app exits with code 2.
- Secrets are never in YAML: config names the environment variable (`passwordEnv`, `authTokenEnv`). `HttpSourceSettings.toString()` and log lines never include the token; JDBC URLs are logged without query strings.
- Network listeners default to `127.0.0.1`.

## Consequences
Onboarding a new source/sink type = config record + validator case + assembler case + implementation; the pipeline is untouched. Sealed interfaces make the compiler flag every switch that misses the new type.
