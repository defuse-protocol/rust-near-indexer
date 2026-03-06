#[macro_use]
extern crate lazy_static;

pub mod cache;
pub mod config;
pub mod extractors;
pub mod metrics;

pub use indexer_primitives as types;

pub const CONTRACT_ACCOUNT_IDS_OF_INTEREST: &[&str] =
    &["intents.near", "defuse-alpha.near", "staging-intents.near"];

pub fn any_account_id_of_interest(account_ids: &[&str], accounts_of_interest: &[&str]) -> bool {
    account_ids
        .iter()
        .any(|id| accounts_of_interest.contains(id))
}
