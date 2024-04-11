use std::time::Duration;

use jsonrpsee::{
    core::client::{Subscription, SubscriptionClientT},
    ws_client::{PingConfig, WsClientBuilder},
};

use jsonrpsee::core::params;
use serde_json::Value;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter};

fn init_tracing(label: &str) -> WorkerGuard {
    let file_appender = tracing_appender::rolling::hourly(
        format!("{}_logs/", label),
        format!("{}_rolling.log", label),
    );
    let (non_blocking_appender, _guard) = tracing_appender::non_blocking(file_appender);

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking_appender)
        .with_ansi(false);

    let stdout_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_thread_ids(true);

    let subscriber = tracing_subscriber::registry()
        .with(file_layer)
        .with(stdout_layer)
        .with(EnvFilter::from_default_env().add_directive("info".parse().unwrap()));

    let _ = tracing::subscriber::set_global_default(subscriber);

    _guard
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _guard = init_tracing(&"test");

    let token = std::env::var("CHAINSTREAM_TOKEN").expect("CHAINSTREAM_TOKEN env var not provided");

    let url = format!("wss://api.syndica.io/api-token/{}", token);

    let mut params = params::ObjectParams::new();
    params.insert("network", "solana-mainnet").unwrap();
    params.insert("verified", false).unwrap();

    let pubkeys = vec!["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"];
    let all_strings = pubkeys
        .iter()
        .map(|pk| pk.to_string())
        .collect::<Vec<String>>();

    let exclude = vec![
        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",
        "SysvarRent111111111111111111111111111111111",
        "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY",
        "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
        "SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f",
        "SAGEqqFewepDHH6hMDcmWy7yjHPpyKLDnRXKb3Ki8e6",
        "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
        "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
        "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB",
        "FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X",
        "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
        "481s8XFACzEbQokBjoZWUv976233jGP9k6SzN2Lu8fLt",
        "Dooar9JkhdZ7J3LHN3A7YCuoGRUggXhQaG4kijfLGU2j",
        "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c",
        "GJRs4FwHtemZ5ZE9x3FNvJ8TMwitKTh21yxdRPqn7npE",
        "AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2",
        "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9",
        "BmFdpraQhkiDQE6SnfG5omcA1VwzqfXrwtNYBwWTymy6",
    ]
    .iter()
    .map(|pk| pk.to_string())
    .collect::<Vec<String>>();
    let filter_params = serde_json::json!({
        // "excludeVotes": true,
        "accountKeys": {
            "all": all_strings,
            "exclude": exclude
        }
    });

    params.insert("filter", filter_params)?;

    let mut total_updates = 0_u64;
    let mut failures = 0_u64;

    let mut stats_interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        let ws_client = WsClientBuilder::default()
            // .set_headers(headers.clone())
            .max_buffer_capacity_per_subscription(100)
            .enable_ws_ping(PingConfig::default().ping_interval(Duration::from_secs(5)))
            .build(&url)
            .await?;

        let mut transaction_sub: Subscription<Value> = ws_client
            .subscribe(
                "chainstream.transactionsSubscribe",
                params.clone(),
                "chainstream.transactionsUnsubscribe",
            )
            .await?;

        // let last_msg_ts = std::time::Instant::now();

        let mut last_count = 0;
        'inner: loop {
            tokio::select! {
                _ = stats_interval.tick() => {
                    tracing::info!(updates_over_last_5s=?last_count, total_update=?total_updates, failures=?failures, "subscription stats");
                    last_count = 0;
                }
                next = transaction_sub.next() => {
                    match next {
                        Some(Ok(_)) => {
                            // Note: Do not do any hard work here. Use message passing to move updates to another thread.
                            total_updates += 1;
                            last_count += 1;
                        }
                        Some(Err(err)) => {
                            tracing::error!("Error: {:#?}", err);
                            failures += 1;
                            break 'inner;
                        }
                        None => {
                            tracing::error!("Received None from subscription stream. Exiting...");
                            failures += 1;
                            break 'inner;
                        }
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
        tracing::warn!("Restarting subscription...");
    }
}
