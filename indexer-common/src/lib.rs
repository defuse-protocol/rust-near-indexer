#[macro_use]
extern crate lazy_static;

pub mod cache;
pub mod config;
pub mod extractors;
pub mod metrics;

pub use indexer_primitives as types;

pub const CONTRACT_ACCOUNT_IDS_OF_INTEREST: &[&str] =
    &["intents.near", "defuse-alpha.near", "staging-intents.near"];

pub fn any_account_id_of_interest(account_ids: &[&str]) -> bool {
    account_ids
        .iter()
        .any(|id| CONTRACT_ACCOUNT_IDS_OF_INTEREST.contains(id))
}
