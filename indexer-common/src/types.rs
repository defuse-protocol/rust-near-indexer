use clickhouse::Row;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use blocksapi::near_indexer_primitives::{self, near_primitives};

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub enum ReceiptOrDataId {
    ReceiptId(near_indexer_primitives::CryptoHash),
    // We don't store data now, but maybe we'll be in the future
    _DataId(near_indexer_primitives::CryptoHash),
}

impl std::fmt::Display for ReceiptOrDataId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReceiptOrDataId::ReceiptId(hash) => write!(f, "{}", hash),
            ReceiptOrDataId::_DataId(hash) => write!(f, "{}", hash),
        }
    }
}
// Creating type aliases to make HashMap types for cache more explicit
pub type ParentTransactionHashString = String;

#[derive(Row, Serialize)]
pub struct EventRow {
    pub block_height: u64,
    pub block_timestamp: u64,
    pub block_hash: String,
    pub contract_id: String,
    pub execution_status: String,
    pub version: String,
    pub standard: String,
    pub index_in_log: u64,
    pub event: String,
    pub data: String,
    pub related_receipt_id: String,
    pub related_receipt_receiver_id: String,
    pub related_receipt_predecessor_id: String,
    pub tx_hash: Option<String>,
    pub receipt_index_in_block: u64,
}

#[derive(Deserialize)]
pub struct EventJson {
    pub version: String,
    pub standard: String,
    pub event: String,
    pub data: Value,
}

#[derive(Row, Serialize)]
pub struct TransactionRow {
    pub block_height: u64,
    pub block_timestamp: u64,
    pub block_hash: String,
    pub transaction_hash: String,
    pub signer_id: String,
    pub receiver_id: String,
    pub actions: String,
}

#[derive(Row, Serialize)]
pub struct ReceiptRow {
    pub block_height: u64,
    pub block_timestamp: u64,
    pub block_hash: String,
    pub parent_transaction_hash: String,
    pub receipt_id: String,
    pub receiver_id: String,
    pub predecessor_id: String,
    pub actions: String,
}

#[derive(Row, Serialize)]
pub struct ExecutionOutcomeRow {
    pub block_height: u64,
    pub block_timestamp: u64,
    pub block_hash: String,
    pub execution_outcome_id: String,
    pub parent_transaction_hash: String,
    pub executor_id: String,
    pub receipt_ids: Vec<String>,
    pub status: String,
    pub logs: String,
    pub tokens_burnt: String,
    pub gas_burnt: u64,
}

#[derive(Serialize)]
#[serde(tag = "action_type", content = "params")]
pub enum Action {
    CreateAccount(near_primitives::action::CreateAccountAction),
    DeployContract(near_primitives::action::DeployContractAction),
    FunctionCall(Box<near_primitives::action::FunctionCallAction>),
    Transfer(near_primitives::action::TransferAction),
    Stake(Box<near_primitives::action::StakeAction>),
    AddKey(Box<near_primitives::action::AddKeyAction>),
    DeleteKey(Box<near_primitives::action::DeleteKeyAction>),
    DeleteAccount(near_primitives::action::DeleteAccountAction),
    Delegate(Box<near_primitives::action::delegate::SignedDelegateAction>),
    DeployGlobalContract(near_primitives::action::DeployGlobalContractAction),
    UseGlobalContract(Box<near_primitives::action::UseGlobalContractAction>),
    DeterministicStateInit(Box<near_primitives::action::DeterministicStateInitAction>),
}

impl TryFrom<&near_primitives::views::ActionView> for Action {
    type Error = anyhow::Error;
    fn try_from(action: &near_primitives::views::ActionView) -> Result<Self, Self::Error> {
        let action: near_primitives::action::Action =
            action.clone().try_into().map_err(anyhow::Error::msg)?;

        Ok(match action {
            near_primitives::action::Action::CreateAccount(a) => Action::CreateAccount(a),
            near_primitives::action::Action::DeployContract(a) => Action::DeployContract(a),
            near_primitives::action::Action::FunctionCall(a) => Action::FunctionCall(a),
            near_primitives::action::Action::Transfer(a) => Action::Transfer(a),
            near_primitives::action::Action::Stake(a) => Action::Stake(a),
            near_primitives::action::Action::AddKey(a) => Action::AddKey(a),
            near_primitives::action::Action::DeleteKey(a) => Action::DeleteKey(a),
            near_primitives::action::Action::DeleteAccount(a) => Action::DeleteAccount(a),
            near_primitives::action::Action::Delegate(a) => Action::Delegate(a),
            near_primitives::action::Action::DeployGlobalContract(a) => {
                Action::DeployGlobalContract(a)
            }
            near_primitives::action::Action::UseGlobalContract(a) => Action::UseGlobalContract(a),
            near_primitives::action::Action::DeterministicStateInit(a) => {
                Action::DeterministicStateInit(a)
            }
        })
    }
}
