#![forbid(unsafe_code)]
mod transactions;

use anyhow::Context;
use futures::TryStreamExt;
use sqlx::sqlite::SqliteConnectOptions;
use std::io;
use std::{fs::File, str::FromStr};

use transactions::{TransactionReader, TransactionService};

async fn print_client_csv(transaction_svc: &mut TransactionService) -> anyhow::Result<()> {
    let stdout = io::stdout().lock();

    let mut w = csv::Writer::from_writer(stdout);
    let mut client_stream = transaction_svc.get_clients().await;
    while let Some(c) = client_stream.try_next().await? {
        w.serialize(c)?;
    }

    Ok(())
}

fn get_transaction_reader() -> anyhow::Result<TransactionReader<std::io::BufReader<std::fs::File>>>
{
    let transaction_file = match std::env::args().skip(1).next() {
        Some(f) => f,
        None => {
            anyhow::bail!("Usage: {}.exe <transaction-file>", env!("CARGO_PKG_NAME"));
        }
    };

    let f = File::open(&transaction_file).map_err(|_| {
        anyhow::format_err!(
            "Could not locate the transaction file \"{}\"",
            &transaction_file
        )
    })?;
    Ok(TransactionReader::new(io::BufReader::new(f)))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut transaction_reader = get_transaction_reader()?;

    let mut transaction_svc = {
        let options = SqliteConnectOptions::from_str("sqlite://:memory:")?.create_if_missing(true);
        let db_pool = sqlx::sqlite::SqlitePool::connect_with(options).await?;
        TransactionService::new(db_pool)
            .await
            .context("Failed to get transaction service")?
    };

    for transaction in transaction_reader.transactions() {
        let transaction = transaction?;
        transaction_svc.process_transaction(&transaction).await?;
    }

    print_client_csv(&mut transaction_svc).await?;

    Ok(())
}
