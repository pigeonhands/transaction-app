mod processor;
mod reader;

use rust_decimal::Decimal;

pub use processor::TransactionService;
pub use reader::*;

use serde::{Serialize, Deserialize};

#[derive(Debug, Deserialize, PartialEq)]
pub enum TransactionType {
    #[serde(rename = "deposit")]
    Deposit,
    #[serde(rename = "withdrawal")]
    Withdrawal,
    #[serde(rename = "dispute")]
    Dispute,
    #[serde(rename = "resolve")]
    Resolve,
    #[serde(rename = "chargeback")]
    Chargeback,
}
impl TransactionType {
    pub fn to_str(&self) -> &'static str {
        match self {
            Self::Deposit => "deposit",
            Self::Withdrawal => "withdrawal",
            Self::Dispute => "dispute",
            Self::Resolve => "resolve",
            Self::Chargeback => "chargeback",
        }
    }
    pub fn from_str(t: &str) -> Option<Self> {
        match t {
            "deposit" => Some(Self::Deposit),
            "withdrawal" => Some(Self::Withdrawal),
            "dispute" => Some(Self::Dispute),
            "resolve" => Some(Self::Resolve),
            "chargeback" => Some(Self::Chargeback),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct Transaction {
    #[serde(rename = "tx")]
    pub id: u32,
    #[serde(rename = "type")]
    pub transaction_type: TransactionType,
    #[serde(rename = "client")]
    pub client_id: u16,
    pub amount: Option<Decimal>,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct Client {
    #[serde(rename = "client")]
    pub id: u16,
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,
    pub locked: bool,
}