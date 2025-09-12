use actix_web::{App, Error, HttpResponse, HttpServer, Responder, dev::ServiceRequest, get};
use actix_web_httpauth::{
    extractors::basic::{BasicAuth, Config},
    middleware::HttpAuthentication,
};

use prometheus::{Encoder, IntCounter, IntGauge, IntGaugeVec, Opts};

type Result<T, E> = std::result::Result<T, E>;

fn try_create_int_counter(name: &str, help: &str) -> Result<IntCounter, prometheus::Error> {
    let opts = Opts::new(name, help);
    let counter = IntCounter::with_opts(opts)?;
    prometheus::register(Box::new(counter.clone()))?;
    Ok(counter)
}

fn try_create_int_gauge(name: &str, help: &str) -> Result<IntGauge, prometheus::Error> {
    let opts = Opts::new(name, help);
    let gauge = IntGauge::with_opts(opts)?;
    prometheus::register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

fn register_int_gauge_vec(
    name: &str,
    help: &str,
    label_names: &[&str],
) -> Result<IntGaugeVec, prometheus::Error> {
    let opts = Opts::new(name, help);
    let counter = IntGaugeVec::new(opts, label_names)?;
    prometheus::register(Box::new(counter.clone()))?;
    Ok(counter)
}

lazy_static! {
    pub(crate) static ref BLOCK_PROCESSED_TOTAL: IntCounter = try_create_int_counter(
        "total_blocks_processed",
        "Total number of blocks processed by indexer regardless of restarts. Used to calculate Block Processing Rate(BPS)"
    )
    .unwrap();
    pub(crate) static ref LATEST_BLOCK_HEIGHT: IntGauge = try_create_int_gauge(
        "latest_block_height",
        "Last seen block height by indexer"
    )
    .unwrap();

    pub(crate) static ref ASSETS_IN_BLOCK_TOTAL: IntGaugeVec = register_int_gauge_vec(
        "assets_in_block_total",
        "Total number of assets in the processed block",
        &["asset_type"] // This declares a label named `asset_type`
    ).unwrap();

    pub(crate) static ref ASSETS_IN_BLOCK_CAPTURED_TOTAL: IntGaugeVec = register_int_gauge_vec(
        "assets_in_block_captured_total",
        "Total number of captured assets in the processed block",
        &["asset_type"] // This declares a label named `asset_type`
    ).unwrap();

    pub(crate) static ref POTENTIAL_ASSET_MISS_TOTAL: IntGaugeVec = register_int_gauge_vec(
        "potential_asset_miss_total",
        "Total number of potential asset misses",
        &["asset_type"] // This declares a label named `asset_type`
    ).unwrap();

    pub(crate) static ref PROMOTIONS_TOTAL: IntGaugeVec = register_int_gauge_vec(
        "promotions_total",
        "Total number of cache promotions from potential to main",
        &["asset_type"]
    ).unwrap();

    pub(crate) static ref STORE_ERRORS_TOTAL: IntCounter = try_create_int_counter(
        "total_tx_store_errors",
        "Total number of errors while storing transactions"
    )
    .unwrap();
}

#[get("/metrics")]
async fn get_metrics() -> impl Responder {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        tracing::error!("could not encode metrics: {}", e);
        return HttpResponse::InternalServerError().finish();
    }
    match String::from_utf8(buffer) {
        Ok(v) => HttpResponse::Ok()
            .content_type(encoder.format_type())
            .body(v),
        Err(e) => {
            tracing::error!("metrics could not be from_utf8'd: {}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

pub(crate) fn init_server(port: u16) -> anyhow::Result<actix_web::dev::Server> {
    tracing::info!("Starting metrics server on http://0.0.0.0:{port}/metrics (no auth)");
    let server = HttpServer::new(|| App::new().service(get_metrics))
        .bind(("0.0.0.0", port))?
        .disable_signals()
        .workers(2)
        .run();

    Ok(server)
}

pub(crate) fn init_server_with_basic_auth(
    port: u16,
    basic_auth: (String, String),
) -> anyhow::Result<actix_web::dev::Server> {
    tracing::info!("Starting metrics server on http://0.0.0.0:{port}/metrics (with basic auth)");

    async fn basic_auth_validator(
        req: ServiceRequest,
        credentials: BasicAuth,
        expected: &(String, String),
    ) -> Result<ServiceRequest, (Error, ServiceRequest)> {
        if credentials.user_id() == expected.0 && credentials.password().unwrap_or("") == expected.1
        {
            Ok(req)
        } else {
            let config = Config::default();
            Err((
                actix_web_httpauth::extractors::AuthenticationError::from(config).into(),
                req,
            ))
        }
    }

    let creds = basic_auth.clone();

    let server = HttpServer::new(move || {
        let value = creds.clone();
        let auth = HttpAuthentication::basic(move |req, credentials| {
            let value = value.clone();
            async move { basic_auth_validator(req, credentials, &value).await }
        });

        App::new().service(get_metrics).wrap(auth)
    })
    .bind(("0.0.0.0", port))?
    .disable_signals()
    .workers(2)
    .run();

    Ok(server)
}
