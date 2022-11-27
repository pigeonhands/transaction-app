use super::Transaction;
use std::io;
pub struct TransactionReader<R: io::Read> {
    reader: csv::Reader<R>,
}

impl<R: io::Read> TransactionReader<R> {
    pub fn new(reader: R) -> Self {
        let reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .trim(csv::Trim::All)
            .from_reader(reader);
        Self { reader }
    }

    pub fn transactions<'a>(
        &'a mut self,
    ) -> impl Iterator<Item = Result<Transaction, csv::Error>> + 'a {
        self.reader.deserialize()
    }
}

#[cfg(test)]
mod tests {
    use crate::transactions::{Transaction, TransactionReader, TransactionType};
    use rust_decimal::{prelude::FromPrimitive, Decimal};
    use std::io;

    #[test]
    fn test_transaction_reader() {
        let test_csv = r#"
type, client, tx, amount
deposit, 1, 1, 1.0
withdrawal, 1, 4, 1.5
dispute, 2, 5
resolve, 1, 1
chargeback, 1, 1"#;

        let mut transaction_reader = {
            let reader = io::BufReader::new(io::Cursor::new(test_csv));
            TransactionReader::new(reader)
        };

        let transactions = transaction_reader
            .transactions()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(
            transactions,
            &[
                Transaction {
                    id: 1,
                    transaction_type: TransactionType::Deposit,
                    client_id: 1,
                    amount: Decimal::from_f64(1.0)
                },
                Transaction {
                    id: 4,
                    transaction_type: TransactionType::Withdrawal,
                    client_id: 1,
                    amount: Decimal::from_f64(1.5)
                },
                Transaction {
                    id: 5,
                    transaction_type: TransactionType::Dispute,
                    client_id: 2,
                    amount: None
                },
                Transaction {
                    id: 1,
                    transaction_type: TransactionType::Resolve,
                    client_id: 1,
                    amount: None
                },
                Transaction {
                    id: 1,
                    transaction_type: TransactionType::Chargeback,
                    client_id: 1,
                    amount: None
                }
            ]
        );
    }
}
