#[macro_use]
extern crate lazy_static;

pub mod cache;
pub mod config;
pub mod database;
pub mod handlers;
pub mod metrics;
pub mod types;

pub const CONTRACT_ACCOUNT_IDS_OF_INTEREST: &[&str] =
    &["intents.near", "defuse-alpha.near", "staging-intents.near"];
