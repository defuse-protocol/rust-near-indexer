use clap::Parser;
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub const INDEXER: &str = "near_defuse_indexer";

/// Shared configuration fields used by all indexer binaries.
#[derive(Parser, Clone)]
pub struct CommonConfig {
    /// Fallback block height to start if no previous height is found in the database
    #[clap(long, env = "BLOCK_HEIGHT", default_value = "0")]
    pub block_height: u64,

    /// Stop the indexer after processing this block height (env: BLOCK_END)
    #[clap(long, env = "BLOCK_END")]
    pub block_end: Option<u64>,

    /// Forces the indexer to start from the specified block height provided via --block-height,
    /// even if a higher block height is found in the database.
    #[clap(long, hide = true)]
    pub force_from_block_height: bool,

    /// Redis server URL for receipt-to-tx cache (env: REDIS_URL)
    #[clap(long, env = "REDIS_URL")]
    pub redis_url: String,

    /// Redis cache TTL in seconds (env: REDIS_TTL_SECONDS, default: 900)
    #[clap(long, env = "REDIS_TTL_SECONDS", default_value = "900")]
    pub redis_ttl_seconds: u64,

    /// Metrics server port (env: METRICS_SERVER_PORT, default: 8080)
    #[clap(long, env = "METRICS_SERVER_PORT", default_value = "8080")]
    pub metrics_server_port: u16,

    /// Metrics server basic auth username (env: METRICS_BASIC_AUTH_USER)
    #[clap(
        long,
        env = "METRICS_BASIC_AUTH_USER",
        requires = "metrics_basic_auth_password"
    )]
    pub metrics_basic_auth_user: Option<String>,

    /// Metrics server basic auth password (env: METRICS_BASIC_AUTH_PASSWORD)
    #[clap(
        long,
        env = "METRICS_BASIC_AUTH_PASSWORD",
        requires = "metrics_basic_auth_user"
    )]
    pub metrics_basic_auth_password: Option<String>,

    /// OpenTelemetry OTLP endpoint for trace export (env: OTEL_EXPORTER_OTLP_ENDPOINT)
    #[clap(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    pub otel_endpoint: Option<String>,

    /// Service name for tracing (env: OTEL_SERVICE_NAME, default: near-defuse-indexer)
    #[clap(long, env = "OTEL_SERVICE_NAME", default_value = "near-defuse-indexer")]
    pub otel_service_name: String,

    /// Service version for tracing (env: OTEL_SERVICE_VERSION, default: 0.2.0)
    #[clap(long, env = "OTEL_SERVICE_VERSION", default_value = "0.2.0")]
    pub otel_service_version: String,

    /// Concurrency level for per-outcome processing (env: OUTCOME_CONCURRENCY, default: 32)
    #[clap(long, env = "OUTCOME_CONCURRENCY", default_value = "32")]
    pub outcome_concurrency: usize,
}

/// BlocksAPI-specific configuration fields.
/// Flattened into binary AppConfigs that use BlocksAPI as their data source.
#[derive(Parser, Clone)]
pub struct BlockApiParams {
    /// Blocks API server address (env: BLOCKSAPI_SERVER_ADDR)
    #[clap(long, env = "BLOCKSAPI_SERVER_ADDR")]
    pub blocksapi_server_addr: String,

    /// Blocks API token (env: BLOCKSAPI_TOKEN)
    #[clap(long, env = "BLOCKSAPI_TOKEN")]
    pub blocksapi_token: String,
}

impl CommonConfig {
    /// Build an `OtelConfig` from the common fields, if an endpoint is configured.
    pub fn otel_config(&self) -> Option<OtelConfig> {
        self.otel_endpoint.as_ref().map(|endpoint| OtelConfig {
            endpoint: endpoint.clone(),
            service_name: self.otel_service_name.clone(),
            service_version: self.otel_service_version.clone(),
        })
    }
}

/// Build a BlocksApi configuration from BlocksAPI fields and a start block height.
pub fn build_blocksapi_config(
    blocksapi: &BlockApiParams,
    start_block: u64,
) -> blocksapi::BlocksApiConfig {
    blocksapi::BlocksApiConfigBuilder::default()
        .server_addr(blocksapi.blocksapi_server_addr.clone())
        .start_on(Some(start_block))
        .blocksapi_token(Some(blocksapi.blocksapi_token.clone()))
        .batch_size(30)
        .concurrency(1000)
        .buffer_size(2 * 1024 * 1024 * 1024)
        .concurrency_limit(2048)
        .build()
        .expect("Error creating Blocks API config")
}

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
                let host_end = rest.find(['/', '?', ':']).unwrap_or(rest.len());
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
