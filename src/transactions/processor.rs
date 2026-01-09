use std::ops::Mul;

use super::{Client, Transaction, TransactionType};
use anyhow::Context;
use futures::{stream::Stream, StreamExt};
use rust_decimal::prelude::{Decimal, ToPrimitive};
use rust_decimal_macros::dec;
use serde::Serialize;
use sqlx::{sqlite::Sqlite, FromRow, Pool};

static DECIMAL_SCALE: u32 = 4;
static STORAGE_MUL: Decimal = dec!(10000);

#[derive(Debug, PartialEq, FromRow, Serialize)]
struct ClientDb {
    #[serde(rename = "client")]
    pub id: u16,
    pub available: i64,
    pub held: i64,
    pub total: i64,
    pub locked: bool,
}

impl Into<Client> for ClientDb {
    fn into(self) -> Client {
        Client {
            id: self.id,
            available: Decimal::new(self.available, DECIMAL_SCALE),
            held: Decimal::new(self.held, DECIMAL_SCALE),
            total: Decimal::new(self.total, DECIMAL_SCALE),
            locked: self.locked,
        }
    }
}

#[derive(FromRow)]
struct DBTransaction {
    pub id: u32,
    #[sqlx(rename = "type")]
    pub transaction_type: String,
    pub client_id: u16,
    pub amount: Option<i64>,
}

impl Into<Transaction> for DBTransaction {
    fn into(self) -> Transaction {
        Transaction {
            id: self.id,
            transaction_type: TransactionType::from_str(&self.transaction_type)
                .expect("Invalid transaction type"),
            client_id: self.client_id,
            amount: self.amount.map(|a| Decimal::new(a, DECIMAL_SCALE)),
        }
    }
}

pub struct TransactionService {
    pool: Pool<Sqlite>,
}

impl TransactionService {
    pub async fn new(pool: Pool<Sqlite>) -> anyhow::Result<Self> {
        sqlx::query(include_str!("../../SCHEMA.sql"))
            .execute(&pool)
            .await?;
        Ok(Self { pool })
    }

    pub async fn get_client(&self, client_id: u16) -> anyhow::Result<Option<Client>> {
        let client = sqlx::query_as::<_, ClientDb>(
            "SELECT *, (held+available) as total from [Clients] WHERE id=? LIMIT 1",
        )
        .bind(client_id)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to get client")?;
        Ok(client.map(|c| c.into()))
    }

    pub async fn get_clients(&self) -> impl Stream<Item = Result<Client, sqlx::Error>> + '_ {
        sqlx::query_as::<_, ClientDb>("SELECT *, (held+available) as total from Clients")
            .fetch(&self.pool)
            .map(|cstream_client| cstream_client.map(|c| c.into()))
    }

    pub async fn get_transaction(
        &self,
        transaction_id: u32,
    ) -> anyhow::Result<Option<Transaction>> {
        let client: Option<DBTransaction> =
            sqlx::query_as("SELECT * FROM [Transactions] WHERE id=? LIMIT 1")
                .bind(transaction_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(client.map(|c| c.into()))
    }

    pub async fn get_dispute(&self, transaction_id: u32) -> anyhow::Result<Option<Transaction>> {
        let client: Option<DBTransaction> =
            sqlx::query_as("SELECT t.* FROM [Disputes] d LEFT JOIN [Transactions] t on t.id = d.transaction_id WHERE d.transaction_id=? LIMIT 1")
                .bind(transaction_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(client.map(|c| c.into()))
    }

    pub async fn process_transaction(&self, transaction: &Transaction) -> anyhow::Result<()> {
        //sqlite dosent support "decimal" so covert to i64
        let amount_i64 = transaction
            .amount
            .map(|a| a.mul(STORAGE_MUL).to_i64())
            .flatten();

        let client = self.get_client(transaction.client_id).await?;
        let mut tx = self.pool.begin().await?;

        let is_basic_transaction = matches!(
            transaction.transaction_type,
            TransactionType::Deposit | TransactionType::Withdrawal
        );

        // Ignore locked clients and create client for basic transactions if dosent exist
        let client = match client {
            Some(c@Client { locked: false, .. }) => Some(c),
            None if is_basic_transaction => {
                Some(sqlx::query_as::<_, ClientDb>("INSERT INTO Clients VALUES(?, 0, 0, false) RETURNING *, (held+available) as total")
                    .bind(transaction.client_id)
                    .bind(transaction.client_id)
                    .fetch_one(&mut *tx)
                    .await
                    .context("Failed to create client")?
                    .into())
            },
            None => None,
            _ =>  {
                return Ok(())
            }
        };

        if is_basic_transaction {
            sqlx::query("INSERT INTO [Transactions] VALUES (?, ?, ?, ?)")
                .bind(transaction.id)
                .bind(transaction.transaction_type.to_str())
                .bind(transaction.client_id)
                .bind(amount_i64)
                .execute(&mut *tx)
                .await
                .context("Failed to insert transaction")?;
        }

        match (&transaction.transaction_type, client) {
            (TransactionType::Deposit, Some(client)) => {
                let amount = amount_i64
                    .ok_or_else(|| anyhow::anyhow!("Deposit transaction requires an amount"))?;

                self.process_deposit(&mut tx, client, amount)
                    .await
                    .context("Failed to process deposit")?;
            }
            (TransactionType::Withdrawal, Some(client)) => {
                let amount = amount_i64
                    .ok_or_else(|| anyhow::anyhow!("Deposit transaction requires an amount"))?;

                self.process_withdraw(&mut tx, transaction.id, client, amount)
                    .await
                    .context("Failed to process withdraw")?;
            }
            (TransactionType::Dispute, _) => self
                .process_dispute(&mut tx, transaction.id)
                .await
                .context("Failed to process dispute")?,
            (TransactionType::Resolve, _) => self
                .process_resolve(&mut tx, transaction.id)
                .await
                .context("Failed to process resolve")?,
            (TransactionType::Chargeback, _) => self
                .process_chargeback(&mut tx, transaction.id)
                .await
                .context("Failed to process chargeback")?,
            _ => {
                tx.rollback().await?;
                return Ok(());
            }
        }

        tx.commit().await.context("Failed to commit transaction")?;

        Ok(())
    }

    async fn process_deposit<'a>(
        &'a self,
        tx: &mut sqlx::Transaction<'a, Sqlite>,
        client: Client,
        amount: i64,
    ) -> anyhow::Result<()> {
        sqlx::query("UPDATE Clients SET available = (available + ?) WHERE id=?")
            .bind(amount)
            .bind(client.id)
            .execute(&mut **tx)
            .await?;

        Ok(())
    }

    async fn process_withdraw<'a>(
        &'a self,
        tx: &mut sqlx::Transaction<'a, Sqlite>,
        _transaction_id: u32,
        client: Client,
        amount: i64,
    ) -> anyhow::Result<()> {
        sqlx::query("UPDATE Clients SET available = (available - ?) WHERE id=? AND available >= ?")
            .bind(amount)
            .bind(client.id)
            .bind(amount)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    async fn process_dispute<'a>(
        &'a self,
        tx: &mut sqlx::Transaction<'a, Sqlite>,
        transaction_id: u32,
    ) -> anyhow::Result<()> {
        let disputed_transaction = match self.get_transaction(transaction_id).await? {
            Some(t) => t,
            None => return Ok(()),
        };

        let amount_i64 = disputed_transaction
            .amount
            .map(|a| a.mul(STORAGE_MUL).to_i64())
            .flatten()
            .ok_or_else(|| anyhow::anyhow!("No amount in disputed transaction"))?;

        sqlx::query("UPDATE Clients SET available = (available - ?), held = (held + ?) WHERE id=?")
            .bind(amount_i64)
            .bind(amount_i64)
            .bind(disputed_transaction.client_id)
            .execute(&mut **tx)
            .await?;

        sqlx::query("INSERT INTO Disputes VALUES(?)")
            .bind(transaction_id)
            .execute(&mut **tx)
            .await?;

        Ok(())
    }

    async fn process_resolve<'a>(
        &'a self,
        tx: &mut sqlx::Transaction<'a, Sqlite>,
        transaction_id: u32,
    ) -> anyhow::Result<()> {
        let disputed_transaction = match self.get_dispute(transaction_id).await? {
            Some(t) => t,
            None => return Ok(()),
        };

        let amount_i64 = disputed_transaction
            .amount
            .map(|a| a.mul(STORAGE_MUL).to_i64())
            .flatten()
            .ok_or_else(|| anyhow::anyhow!("No amount in disputed transaction"))?;

        sqlx::query("UPDATE Clients SET available = available + ?, held = held - ? WHERE id=?")
            .bind(amount_i64)
            .bind(amount_i64)
            .bind(disputed_transaction.client_id)
            .execute(&mut **tx)
            .await?;

        sqlx::query("DELETE FROM Disputes WHERE transaction_id=?")
            .bind(transaction_id)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    async fn process_chargeback<'a>(
        &'a self,
        tx: &mut sqlx::Transaction<'a, Sqlite>,
        transaction_id: u32,
    ) -> anyhow::Result<()> {
        let disputed_transaction = match self.get_dispute(transaction_id).await? {
            Some(t) => t,
            None => return Ok(()),
        };

        let amount_i64 = disputed_transaction
            .amount
            .map(|a| a.mul(STORAGE_MUL).to_i64())
            .flatten()
            .ok_or_else(|| anyhow::anyhow!("No amount in disputed transaction"))?;

        sqlx::query("UPDATE Clients SET held = held - ?, locked=true WHERE id=?")
            .bind(amount_i64)
            .bind(disputed_transaction.client_id)
            .bind(amount_i64)
            .execute(&mut **tx)
            .await?;

        sqlx::query("DELETE FROM Disputes WHERE transaction_id=?")
            .bind(transaction_id)
            .execute(&mut **tx)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Client, Transaction, TransactionService, TransactionType};
    use rust_decimal::{prelude::FromPrimitive, Decimal};
    use rust_decimal_macros::dec;
    use sqlx::sqlite::SqliteConnectOptions;
    use std::str::FromStr;

    async fn create_service() -> TransactionService {
        let options = SqliteConnectOptions::from_str("sqlite://:memory:")
            .unwrap()
            .create_if_missing(true);
        let db_pool = sqlx::sqlite::SqlitePool::connect_with(options)
            .await
            .unwrap();
        TransactionService::new(db_pool).await.unwrap()
    }

    async fn test_service(transactions: &[Transaction], mut expected_output: Vec<Client>) {
        let svc = create_service().await;
        for t in transactions {
            svc.process_transaction(t).await.unwrap();
        }

        let mut clients: Vec<Client> = svc.get_clients_vec().await.unwrap();
        clients.sort_by_key(|c| c.id);

        expected_output.sort_by_key(|c| c.id);

        assert_eq!(clients, expected_output);
    }
    #[tokio::test]
    async fn test_deposit() {
        test_service(
            &[
                Transaction {
                    id: 0,
                    transaction_type: TransactionType::Deposit,
                    client_id: 1,
                    amount: Decimal::from_f64(10.5563),
                },
                Transaction {
                    id: 1,
                    transaction_type: TransactionType::Deposit,
                    client_id: 1,
                    amount: Decimal::from_f64(2.1234),
                },
                Transaction {
                    id: 2,
                    transaction_type: TransactionType::Deposit,
                    client_id: 1,
                    amount: Decimal::from_f64(13.5),
                },
                Transaction {
                    id: 3,
                    transaction_type: TransactionType::Deposit,
                    client_id: 1,
                    amount: Decimal::from_f64(1.3),
                },
                Transaction {
                    id: 4,
                    transaction_type: TransactionType::Deposit,
                    client_id: 2,
                    amount: Decimal::from_f64(10.5563),
                },
            ],
            vec![
                Client {
                    id: 1,
                    available: dec!(27.4797),
                    held: dec!(0.0),
                    total: dec!(27.4797),
                    locked: false,
                },
                Client {
                    id: 2,
                    available: dec!(10.5563),
                    held: dec!(0.0),
                    total: dec!(10.5563),
                    locked: false,
                },
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_deposit_withdraw() {
        test_service(
            &[
                Transaction {
                    id: 0,
                    transaction_type: TransactionType::Deposit,
                    client_id: 1,
                    amount: Decimal::from_f64(10.5563),
                },
                Transaction {
                    id: 1,
                    transaction_type: TransactionType::Deposit,
                    client_id: 1,
                    amount: Decimal::from_f64(2.1234),
                },
                Transaction {
                    id: 2,
                    transaction_type: TransactionType::Deposit,
                    client_id: 1,
                    amount: Decimal::from_f64(13.5),
                },
                Transaction {
                    id: 3,
                    transaction_type: TransactionType::Deposit,
                    client_id: 1,
                    amount: Decimal::from_f64(1.3),
                },
                Transaction {
                    id: 4,
                    transaction_type: TransactionType::Withdrawal,
                    client_id: 1,
                    amount: Decimal::from_f64(5.8367),
                },
                Transaction {
                    id: 5,
                    transaction_type: TransactionType::Deposit,
                    client_id: 2,
                    amount: Decimal::from_f64(10.5563),
                },
                Transaction {
                    id: 6,
                    transaction_type: TransactionType::Deposit,
                    client_id: 3,
                    amount: Decimal::from_f64(2.1234),
                },
                Transaction {
                    id: 7,
                    transaction_type: TransactionType::Deposit,
                    client_id: 2,
                    amount: Decimal::from_f64(13.5),
                },
                Transaction {
                    id: 8,
                    transaction_type: TransactionType::Deposit,
                    client_id: 3,
                    amount: Decimal::from_f64(1.3),
                },
                Transaction {
                    id: 9,
                    transaction_type: TransactionType::Withdrawal,
                    client_id: 2,
                    amount: Decimal::from_f64(5.8367),
                },
                // Withdraw should fail
                Transaction {
                    id: 10,
                    transaction_type: TransactionType::Withdrawal,
                    client_id: 3,
                    amount: Decimal::from_f64(5.8367),
                },
                Transaction {
                    id: 11,
                    transaction_type: TransactionType::Withdrawal,
                    client_id: 1,
                    amount: Decimal::from_f64(5.8367),
                },
            ],
            vec![
                Client {
                    id: 1,
                    available: dec!(15.8063),
                    held: dec!(0.0),
                    total: dec!(15.8063),
                    locked: false,
                },
                Client {
                    id: 2,
                    available: dec!(18.2196),
                    held: dec!(0.0),
                    total: dec!(18.2196),
                    locked: false,
                },
                Client {
                    id: 3,
                    available: dec!(3.4234),
                    held: dec!(0.0),
                    total: dec!(3.4234),
                    locked: false,
                },
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_disputes() {
        test_service(
            &[
                Transaction {
                    id: 0,
                    transaction_type: TransactionType::Deposit,
                    client_id: 1,
                    amount: Decimal::from_f64(10.5563),
                },
                Transaction {
                    id: 1,
                    transaction_type: TransactionType::Deposit,
                    client_id: 1,
                    amount: Decimal::from_f64(2.1234),
                },
                Transaction {
                    id: 2,
                    transaction_type: TransactionType::Deposit,
                    client_id: 1,
                    amount: Decimal::from_f64(13.5),
                },
                Transaction {
                    id: 3,
                    transaction_type: TransactionType::Deposit,
                    client_id: 1,
                    amount: Decimal::from_f64(1.3),
                },
                Transaction {
                    id: 4,
                    transaction_type: TransactionType::Withdrawal,
                    client_id: 1,
                    amount: Decimal::from_f64(5.8367),
                },
                Transaction {
                    id: 5,
                    transaction_type: TransactionType::Deposit,
                    client_id: 2,
                    amount: Decimal::from_f64(10.5563),
                },
                Transaction {
                    id: 6,
                    transaction_type: TransactionType::Deposit,
                    client_id: 3,
                    amount: Decimal::from_f64(2.1234),
                },
                Transaction {
                    id: 7,
                    transaction_type: TransactionType::Deposit,
                    client_id: 2,
                    amount: Decimal::from_f64(13.5),
                },
                Transaction {
                    id: 8,
                    transaction_type: TransactionType::Deposit,
                    client_id: 3,
                    amount: Decimal::from_f64(1.3),
                },
                Transaction {
                    id: 9,
                    transaction_type: TransactionType::Withdrawal,
                    client_id: 2,
                    amount: Decimal::from_f64(5.8367),
                },
                // Withdraw should fail
                Transaction {
                    id: 10,
                    transaction_type: TransactionType::Withdrawal,
                    client_id: 3,
                    amount: Decimal::from_f64(5.8367),
                },
                Transaction {
                    id: 11,
                    transaction_type: TransactionType::Withdrawal,
                    client_id: 1,
                    amount: Decimal::from_f64(5.8367),
                },
                Transaction {
                    id: 3,
                    transaction_type: TransactionType::Dispute,
                    client_id: 1,
                    amount: None,
                },
                Transaction {
                    id: 3,
                    transaction_type: TransactionType::Resolve,
                    client_id: 1,
                    amount: None,
                },
                Transaction {
                    id: 5,
                    transaction_type: TransactionType::Dispute,
                    client_id: 2,
                    amount: None,
                },
                Transaction {
                    id: 5,
                    transaction_type: TransactionType::Chargeback,
                    client_id: 2,
                    amount: None,
                },
                Transaction {
                    id: 8,
                    transaction_type: TransactionType::Dispute,
                    client_id: 3,
                    amount: None,
                },
            ],
            vec![
                Client {
                    id: 1,
                    available: dec!(15.8063),
                    held: dec!(0.0),
                    total: dec!(15.8063),
                    locked: false,
                },
                Client {
                    id: 2,
                    available: dec!(7.6633),
                    held: dec!(0.0),
                    total: dec!(7.6633),
                    locked: true,
                },
                Client {
                    id: 3,
                    available: dec!(2.1234),
                    held: dec!(1.3),
                    total: dec!(3.4234),
                    locked: false,
                },
            ],
        )
        .await;
    }
}
