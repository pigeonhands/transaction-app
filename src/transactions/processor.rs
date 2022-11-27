use super::{Transaction, TransactionType};
use anyhow::Context;
use futures::stream::Stream;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use serde::Serialize;
use sqlx::{sqlite::Sqlite, types::Decimal, FromRow, Pool};

#[derive(Debug, PartialEq, FromRow, Serialize)]
pub struct Client {
    #[serde(rename = "client")]
    pub id: u16,
    pub available: f64,
    pub held: f64,
    pub total: f64,
    pub locked: bool,
}

#[derive(FromRow)]
struct DBTransaction {
    pub id: u32,
    #[sqlx(rename = "type")]
    pub transaction_type: String,
    pub client_id: u16,
    pub amount: Option<f64>,
}

impl Into<Transaction> for DBTransaction {
    fn into(self) -> Transaction {
        Transaction {
            id: self.id,
            transaction_type: TransactionType::from_str(&self.transaction_type)
                .expect("Invalid transaction type"),
            client_id: self.client_id,
            amount: self
                .amount
                .map(|a| Decimal::from_f64(a).expect("Failed to convert f64 to decimal")),
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
        let client =
            sqlx::query_as("SELECT *, (held+available) as total from Clients WHERE id=?  LIMIT 1")
                .bind(client_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(client)
    }

    pub async fn get_clients(&self) -> impl Stream<Item = Result<Client, sqlx::Error>> + '_ {
        sqlx::query_as("SELECT *, (held+available) as total from Clients").fetch(&self.pool)
    }
    pub async fn get_clients_vec(&self) -> Result<Vec<Client>, sqlx::Error> {
        sqlx::query_as("SELECT *, (held+available) as total from Clients").fetch_all(&self.pool).await
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
            sqlx::query_as("SELECT t.* FROM [Disputes] d LEFT JOIN [Transactions] t on t.id = d.transaction_id WHERE transaction_id=? LIMIT 1")
                .bind(transaction_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(client.map(|c| c.into()))
    }

    pub async fn process_transaction(&self, transaction: &Transaction) -> anyhow::Result<()> {
        //sqlite dosent support "decimal" so covert to f64
        let amount_f64 = match transaction.amount {
            Some(a) => Some(
                a.to_f64()
                    .ok_or_else(|| anyhow::anyhow!("Could not convert decimal to f64"))?,
            ),
            None => None,
        };

        let client = self.get_client(transaction.client_id).await?;

        // Ignore locked clients and create client if dosent exist
        let client = match client {
            Some(Client { locked: true, .. }) => {
                return Ok(());
            }
            Some(c) => c,
            None => {
                // "RETRUNING" in sqlite has a bug that converts REAL to INTEGER. Double query as a workaround.
                sqlx::query_as("INSERT INTO Clients VALUES(?, 0, 0, false);SELECT *, (held+available) as total FROM Clients WHERE ID=? LIMIT 1")
                    .bind(transaction.client_id)
                    .bind(transaction.client_id)
                    .fetch_one(&self.pool)
                    .await
                    .context("Failed to create client")?
            }
        };

        let mut tx = self.pool.begin().await?;

        if matches!(
            transaction.transaction_type,
            TransactionType::Deposit | TransactionType::Withdrawal
        ) {
            sqlx::query("INSERT INTO [Transactions] VALUES (?, ?, ?, ?)")
                .bind(transaction.id)
                .bind(transaction.transaction_type.to_str())
                .bind(transaction.client_id)
                .bind(amount_f64)
                .execute(&mut tx)
                .await
                .context("Failed to insert transaction")?;
        }

        match &transaction.transaction_type {
            TransactionType::Deposit => {
                let amount = amount_f64
                    .ok_or_else(|| anyhow::anyhow!("Deposit transaction requires an amount"))?;

                self.process_deposit(&mut tx, client, amount)
                    .await
                    .context("Failed to process deposit")?;
            }
            TransactionType::Withdrawal => {
                let amount = amount_f64
                    .ok_or_else(|| anyhow::anyhow!("Deposit transaction requires an amount"))?;

                self.process_withdraw(&mut tx, transaction.id, client, amount)
                    .await
                    .context("Failed to process withdraw")?;
            }
            TransactionType::Dispute => self
                .process_dispute(&mut tx, transaction.id, client)
                .await
                .context("Failed to process dispute")?,
            TransactionType::Resolve => self
                .process_resolve(&mut tx, transaction.id, client)
                .await
                .context("Failed to process resolve")?,
            TransactionType::Chargeback => self
                .process_chargeback(&mut tx, transaction.id, client)
                .await
                .context("Failed to process chargeback")?,
        }

        tx.commit().await.context("Failed to commit transaction")?;

        Ok(())
    }

    async fn process_deposit<'a>(
        &'a self,
        tx: &mut sqlx::Transaction<'a, Sqlite>,
        client: Client,
        amount: f64,
    ) -> anyhow::Result<()> {
        sqlx::query("UPDATE Clients SET available = (available + ?) WHERE id=?")
            .bind(amount.to_f64())
            .bind(client.id)
            .execute(tx)
            .await?;

        Ok(())
    }

    async fn process_withdraw<'a>(
        &'a self,
        tx: &mut sqlx::Transaction<'a, Sqlite>,
        _transaction_id: u32,
        client: Client,
        amount: f64,
    ) -> anyhow::Result<()> {
        sqlx::query("UPDATE Clients SET available = (available - ?) WHERE id=? AND available >= ?")
            .bind(amount)
            .bind(client.id)
            .bind(amount)
            .execute(tx)
            .await?;
        Ok(())
    }

    async fn process_dispute<'a>(
        &'a self,
        tx: &mut sqlx::Transaction<'a, Sqlite>,
        transaction_id: u32,
        client: Client,
    ) -> anyhow::Result<()> {
        let disputed_transaction = match self.get_transaction(transaction_id).await? {
            Some(t) => t,
            None => return Ok(()),
        };

        let amount_f64 = match disputed_transaction.amount {
            Some(a) => Some(
                a.to_f64()
                    .ok_or_else(|| anyhow::anyhow!("Could not convert decimal to f64"))?,
            ),
            None => None,
        }
        .ok_or_else(|| anyhow::anyhow!("No amount in disputed transaction"))?;

        sqlx::query("UPDATE Clients SET available = (available - ?), held = (held + ?) WHERE id=?")
            .bind(amount_f64)
            .bind(amount_f64)
            .bind(client.id)
            .execute::<&mut sqlx::Transaction<'_, _>>(tx)
            .await?;

        sqlx::query("INSERT INTO Disputes VALUES(?)")
            .bind(transaction_id)
            .execute(tx)
            .await?;

        Ok(())
    }

    async fn process_resolve<'a>(
        &'a self,
        tx: &mut sqlx::Transaction<'a, Sqlite>,
        transaction_id: u32,
        client: Client,
    ) -> anyhow::Result<()> {
        let disputed_transaction = match self.get_dispute(transaction_id).await? {
            Some(t) => t,
            None => return Ok(()),
        };

        let amount_f64 = match disputed_transaction.amount {
            Some(a) => Some(
                a.to_f64()
                    .ok_or_else(|| anyhow::anyhow!("Could not convert decimal to f64"))?,
            ),
            None => None,
        }
        .ok_or_else(|| anyhow::anyhow!("No amount in disputed transaction"))?;

        sqlx::query("UPDATE Clients SET available = available + ?, held = held - ? WHERE id=?")
            .bind(amount_f64)
            .bind(amount_f64)
            .bind(client.id)
            .execute::<&mut sqlx::Transaction<'_, _>>(tx)
            .await?;

        sqlx::query("DELETE FROM Disputes WHERE transaction_id=?")
            .bind(transaction_id)
            .execute(tx)
            .await?;
        Ok(())
    }

    async fn process_chargeback<'a>(
        &'a self,
        tx: &mut sqlx::Transaction<'a, Sqlite>,
        transaction_id: u32,
        client: Client,
    ) -> anyhow::Result<()> {
        let disputed_transaction = match self.get_dispute(transaction_id).await? {
            Some(t) => t,
            None => return Ok(()),
        };

        let amount_f64 = match disputed_transaction.amount {
            Some(a) => Some(
                a.to_f64()
                    .ok_or_else(|| anyhow::anyhow!("Could not convert decimal to f64"))?,
            ),
            None => None,
        }
        .ok_or_else(|| anyhow::anyhow!("No amount in disputed transaction"))?;

        sqlx::query("UPDATE Clients SET held = held - ?, locked=true WHERE id=?")
            .bind(amount_f64)
            .bind(client.id)
            .bind(amount_f64)
            .execute::<&mut sqlx::Transaction<'_, _>>(tx)
            .await?;

        sqlx::query("DELETE FROM Disputes WHERE transaction_id=?")
            .bind(transaction_id)
            .execute(tx)
            .await?;

        Ok(())
    }
}



#[cfg(test)]
mod tests {
    use super::{Transaction, TransactionType, TransactionService, Client};
    use sqlx::sqlite::SqliteConnectOptions;
    use std::{str::FromStr};
    use rust_decimal::{Decimal, prelude::FromPrimitive};


    async fn create_service() -> TransactionService{
        let options =
        SqliteConnectOptions::from_str("sqlite://:memory:").unwrap().create_if_missing(true);
        let db_pool = sqlx::sqlite::SqlitePool::connect_with(options).await.unwrap();
        TransactionService::new(db_pool)
            .await.unwrap()
    }

    async fn test_service(transactions: &[Transaction], mut expected_output: Vec<Client>) {
        let svc = create_service().await;
        for t in transactions {
            svc.process_transaction(t).await.unwrap();
        }

        let mut clients : Vec<Client> = svc.get_clients_vec().await.unwrap();
        clients.sort_by_key(|c| c.id);

        expected_output.sort_by_key(|c| c.id);


        assert_eq!(clients, expected_output);
    }
    #[tokio::test]
    async fn test_deposit() {
        test_service(
            &[
                Transaction{id:0, transaction_type: TransactionType::Deposit, client_id: 1, amount: Decimal::from_f64(10.5563) },
                Transaction{id:1, transaction_type: TransactionType::Deposit, client_id: 1, amount: Decimal::from_f64(2.1234) },
                Transaction{id:2, transaction_type: TransactionType::Deposit, client_id: 1, amount: Decimal::from_f64(13.5) },
                Transaction{id:3, transaction_type: TransactionType::Deposit, client_id: 1, amount: Decimal::from_f64(1.3) },

                Transaction{id:4, transaction_type: TransactionType::Deposit, client_id: 2, amount: Decimal::from_f64(10.5563) },
            ],
            vec![
                Client { id: 1, available: 27.4797, held: 0.0, total: 27.4797, locked: false },
                Client { id: 2, available: 10.5563, held: 0.0, total: 10.5563, locked: false }
            ]
        ).await;
    }

    #[tokio::test]
    async fn test_deposit_withdraw() {
        test_service(
            &[
                Transaction{id:0, transaction_type: TransactionType::Deposit, client_id: 1, amount: Decimal::from_f64(10.5563) },
                Transaction{id:1, transaction_type: TransactionType::Deposit, client_id: 1, amount: Decimal::from_f64(2.1234) },
                Transaction{id:2, transaction_type: TransactionType::Deposit, client_id: 1, amount: Decimal::from_f64(13.5) },
                Transaction{id:3, transaction_type: TransactionType::Deposit, client_id: 1, amount: Decimal::from_f64(1.3) },
                Transaction{id:4, transaction_type: TransactionType::Withdrawal, client_id: 1, amount: Decimal::from_f64(5.8367)},

                Transaction{id:5, transaction_type: TransactionType::Deposit, client_id: 2, amount: Decimal::from_f64(10.5563) },
                Transaction{id:6, transaction_type: TransactionType::Deposit, client_id: 3, amount: Decimal::from_f64(2.1234)},
                Transaction{id:7, transaction_type: TransactionType::Deposit, client_id: 2, amount: Decimal::from_f64(13.5) },
                Transaction{id:8, transaction_type: TransactionType::Deposit, client_id: 3, amount: Decimal::from_f64(1.3) },
                Transaction{id:9, transaction_type: TransactionType::Withdrawal, client_id: 2, amount: Decimal::from_f64(5.8367) },
                // Withdraw should fail
                Transaction{id:10, transaction_type: TransactionType::Withdrawal, client_id: 3, amount: Decimal::from_f64(5.8367) },

                Transaction{id:11, transaction_type: TransactionType::Withdrawal, client_id: 1, amount: Decimal::from_f64(5.8367) },

            ],
            vec![
                Client { id: 1, available: 15.8063, held: 0.0, total: 15.8063, locked: false },
                Client { id: 2, available: 18.2196, held: 0.0, total: 18.2196, locked: false },
                Client { id: 3, available: 3.4234, held: 0.0, total: 3.4234, locked: false },
            ]
        ).await;
    }

    #[tokio::test]
    async fn test_disputes() {
        test_service(
            &[
                Transaction{id:0, transaction_type: TransactionType::Deposit, client_id: 1, amount: Decimal::from_f64(10.5563) },
                Transaction{id:1, transaction_type: TransactionType::Deposit, client_id: 1, amount: Decimal::from_f64(2.1234) },
                Transaction{id:2, transaction_type: TransactionType::Deposit, client_id: 1, amount: Decimal::from_f64(13.5) },
                Transaction{id:3, transaction_type: TransactionType::Deposit, client_id: 1, amount: Decimal::from_f64(1.3) },
                Transaction{id:4, transaction_type: TransactionType::Withdrawal, client_id: 1, amount: Decimal::from_f64(5.8367)},

                Transaction{id:5, transaction_type: TransactionType::Deposit, client_id: 2, amount: Decimal::from_f64(10.5563) },
                Transaction{id:6, transaction_type: TransactionType::Deposit, client_id: 3, amount: Decimal::from_f64(2.1234)},
                Transaction{id:7, transaction_type: TransactionType::Deposit, client_id: 2, amount: Decimal::from_f64(13.5) },
                Transaction{id:8, transaction_type: TransactionType::Deposit, client_id: 3, amount: Decimal::from_f64(1.3) },
                Transaction{id:9, transaction_type: TransactionType::Withdrawal, client_id: 2, amount: Decimal::from_f64(5.8367) },
                // Withdraw should fail
                Transaction{id:10, transaction_type: TransactionType::Withdrawal, client_id: 3, amount: Decimal::from_f64(5.8367) },

                Transaction{id:11, transaction_type: TransactionType::Withdrawal, client_id: 1, amount: Decimal::from_f64(5.8367) },

                Transaction{id:3, transaction_type: TransactionType::Dispute, client_id: 1, amount: None },
                Transaction{id:3, transaction_type: TransactionType::Resolve, client_id: 1, amount: None },

                Transaction{id:5, transaction_type: TransactionType::Dispute, client_id: 2, amount: None },
                Transaction{id:5, transaction_type: TransactionType::Chargeback, client_id: 2, amount: None },

                Transaction{id:8, transaction_type: TransactionType::Dispute, client_id: 3, amount: None },
            ],
            vec![
                Client { id: 1, available: 15.8063, held: 0.0, total: 15.8063, locked: false },
                Client { id: 2, available:7.6633, held: 0.0, total: 7.6633, locked: true },
                Client { id: 3, available: 2.1234, held: 1.3, total: 3.4234, locked: false },
            ]
        ).await;
    }


}
