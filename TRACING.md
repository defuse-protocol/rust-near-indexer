# Performance Tracing

OpenTelemetry tracing is integrated to diagnose block processing bottlenecks and database / cache hot spots. You can export spans to Jaeger (default dev choice) or OpenObserve using the bundled `docker-compose.tracing.yml`.

## Quick Start (Jaeger)

1. Start Jaeger (all‑in‑one) locally:
```bash
docker compose -f docker-compose.tracing.yml up -d jaeger
```
   Exposed ports:
   - 16686 (UI) – http://localhost:16686
   - 4317 (OTLP gRPC)
   - 4318 (OTLP HTTP)

2. Set / append to your `.env`:
```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318/v1/traces
OTEL_SERVICE_NAME=near-defuse-indexer
OTEL_SERVICE_VERSION=0.2.0
RUST_LOG=near_defuse_indexer=debug,near_lake_framework=info
```

3. Run the indexer (tracing auto‑initializes when `OTEL_EXPORTER_OTLP_ENDPOINT` is set):
```bash
cargo run --release
```

4. Open the Jaeger UI → Search → Service = `near-defuse-indexer`.

## Optional: OpenObserve

You can also spin up OpenObserve (receives OTLP) from the same compose file:
```bash
docker compose -f docker-compose.tracing.yml up -d openobserve
```
Then set:
```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:5080/api/default/v1/traces
ZO_ROOT_USER_EMAIL=admin@example.com  # first-run creds defined in compose
ZO_ROOT_USER_PASSWORD=admin123
```
UI: http://localhost:5080 (login with the above credentials).

## Handler & Span Model

Top-level per block:
- `handle_streamer_message`

Nested major handlers:
- `handle_transactions`
- `handle_receipts_and_outcomes` (combined receipts + execution outcomes)
- `handle_events`

Within each handler you will see phase spans:
- Transactions: `extract_transactions_and_outcomes`, `insert_transactions_to_db`
- Combined handler phases:
  - `extract_execution_outcomes`
  - `cache_map_new_receipts_from_outcomes`
  - `populate_potential_cache_from_untracked_outcomes`
  - `extract_receipts`
  - `insert_batches` → sub‑spans `insert_execution_outcomes_to_db` / `insert_receipts_to_db`
- Events: `parse_events`, `insert_events_to_db`

Common lower-level spans:
- `database_insert` (from `database::insert_rows`) – includes row counts & duration
- Cache promotions / mapping (trace/debug logs + parent spans above)

## Environment Variables (Tracing)

| Variable | Purpose | Notes |
|----------|---------|-------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Enables OTLP export | If unset, tracing falls back to plain `tracing` (no export) |
| `OTEL_SERVICE_NAME` | Service name | Default baked into config helper |
| `OTEL_SERVICE_VERSION` | Semantic version tag | Optional |
| `RUST_LOG` | Controls log verbosity | Set module filters for noise control |

Additional (optional) common OTLP vars you may set manually if needed:
| `OTEL_EXPORTER_OTLP_TIMEOUT` | Request timeout ms | Default SDK value |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` or `grpc` | Match the port you target |

## Using the Compose File

`docker-compose.tracing.yml` contains two services:
1. `jaeger` – all‑in‑one collector + query + UI, exposes OTLP gRPC & HTTP directly (no separate collector needed for dev).
2. `openobserve` – optional alternative backend (disabled unless started explicitly).

You can bring only what you need:
```bash
# Jaeger only
docker compose -f docker-compose.tracing.yml up -d jaeger

# OpenObserve only
docker compose -f docker-compose.tracing.yml up -d openobserve

# Both
docker compose -f docker-compose.tracing.yml up -d
```

Stop & clean:
```bash
docker compose -f docker-compose.tracing.yml down -v
```

## Interpreting Traces

Look at `handle_streamer_message` duration first. Drill into slow spans:
1. If `insert_batches` dominates → investigate ClickHouse latency (IO / compression / network).
2. If `extract_execution_outcomes` or `extract_receipts` heavy → parsing or cache contention.
3. High time in `cache_map_new_receipts_from_outcomes` / `populate_potential_cache_from_untracked_outcomes` → lock contention; consider sharding or reducing copies.
4. Events slow → likely JSON parsing or large log volume.

Enable more granularity by raising log level to `trace` to see per‑receipt / per‑outcome mapping traces (they appear as logs, not spans, to limit overhead).

## Perf Optimization Checklist via Tracing

- Aim for balanced durations across phases; large skew signals a hotspot.
- Watch variance over blocks; outliers can indicate data-dependent slow paths.
- Correlate spikes with `STORE_ERRORS_TOTAL` increments to spot failing inserts.
- Use Jaeger trace comparison to see improvements after code changes.

## FAQ

**Q: Nothing shows in Jaeger.**
A: Verify endpoint & protocol match (4318 HTTP with `/v1/traces`). Ensure env vars exported before `cargo run`.

**Q: Should I add an OpenTelemetry Collector?**
A: For local dev, Jaeger all‑in‑one is enough. Add a collector only if you need tail sampling, multiple exporters, or advanced processors.

**Q: High span count?**
A: This setup keeps spans coarse; fine events are logs. If you need per asset row spans, add them temporarily and remove afterward.

---
Tracing docs reflect the current combined receipts/outcomes handler (`handle_receipts_and_outcomes`).
