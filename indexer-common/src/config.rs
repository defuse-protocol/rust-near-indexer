use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub const INDEXER: &str = "near_defuse_indexer";

/// Minimal config for OpenTelemetry tracing initialization.
pub struct OtelConfig {
    pub endpoint: String,
    pub service_name: String,
    pub service_version: String,
}

pub async fn init_tracing_with_otel(otel: Option<&OtelConfig>) -> anyhow::Result<()> {
    // Initialize default directives from environment variable RUST_LOG
    // with a fallback to INFO level for the indexer target
    let mut env_filter = tracing_subscriber::EnvFilter::new(format!("{}=info,info", INDEXER));

    // Override or add directives from RUST_LOG if set
    if let Ok(rust_log) = std::env::var("RUST_LOG")
        && !rust_log.is_empty()
    {
        for directive in rust_log.split(',').filter_map(|s| match s.trim().parse() {
            Ok(directive) => Some(directive),
            Err(err) => {
                eprintln!("Ignoring directive `{}`: {}", s, err);
                None
            }
        }) {
            env_filter = env_filter.add_directive(directive);
        }
    }

    // Configures how trace context propagates across services
    opentelemetry::global::set_text_map_propagator(
        opentelemetry::sdk::propagation::TraceContextPropagator::new(),
    );

    if let Some(otel_config) = otel {
        // Log only scheme+host to avoid leaking auth tokens in the endpoint URL
        let redacted = otel_config
            .endpoint
            .find("://")
            .map(|scheme_end| {
                let after_scheme = &otel_config.endpoint[..scheme_end + 3];
                let rest = &otel_config.endpoint[scheme_end + 3..];
                let host_end = rest
                    .find(['/', '?', ':'])
                    .unwrap_or(rest.len());
                format!("{}{}", after_scheme, &rest[..host_end])
            })
            .unwrap_or_else(|| "<invalid URL>".to_string());
        eprintln!(
            "Initializing OpenTelemetry tracing with endpoint: {}",
            redacted
        );

        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .http()
                    .with_endpoint(&otel_config.endpoint),
            )
            .with_trace_config(opentelemetry_sdk::trace::config().with_resource(
                opentelemetry_sdk::Resource::new(vec![
                    opentelemetry::KeyValue::new(
                        opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                        otel_config.service_name.clone(),
                    ),
                    opentelemetry::KeyValue::new(
                        opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
                        otel_config.service_version.clone(),
                    ),
                ]),
            ))
            .install_batch(opentelemetry_sdk::runtime::Tokio)?;

        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .with(telemetry)
            .try_init()?;
    } else {
        eprintln!("OpenTelemetry endpoint not configured, using default tracing");
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .try_init()?;
    }

    Ok(())
}
