use clap::Parser;
use indexer_common::config::{BlockApiParams, CommonConfig};

/// Application configuration for the Explorer indexer (PostgreSQL backend).
#[derive(Parser, Clone)]
#[clap(author, version, about = "NEAR Defuse Indexer — PostgreSQL backend")]
pub struct AppConfig {
    #[clap(flatten)]
    pub common: CommonConfig,

    #[clap(flatten)]
    pub blocksapi: BlockApiParams,

    /// PostgreSQL connection URL (env: DATABASE_URL)
    #[clap(long, env = "DATABASE_URL")]
    pub database_url: String,
}
