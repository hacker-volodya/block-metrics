mod block_fetcher;
mod metrics;
mod web;

use std::str::FromStr;
use std::sync::Arc;
use std::path::PathBuf;

use block_fetcher::{BlockFetcher, BlockSubscriber};
use clap::Parser;
use metrics::BlockMetrics;
use tokio::fs::read_to_string;
use tokio::sync::RwLock;
use ton_block::Block;
use ton_networkconfig::ConfigGlobal;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let logger = tracing_subscriber::fmt().with_env_filter(
        EnvFilter::builder()
            .with_default_directive(tracing::Level::INFO.into())
            .from_env_lossy(),
    );
    logger.init();
    let args = Args::parse();
    let config_json = if let Some(config) = args.config {
        read_to_string(config).await?
    } else {
        download_config(args.testnet)?
    };
    let config: ConfigGlobal = ConfigGlobal::from_str(&config_json)?;
    let metrics = Arc::new(RwLock::new(BlockMetrics::new()?));
    let subscriber = Subscriber::new(metrics.clone());
    let fetcher = BlockFetcher::new(config, subscriber);
    web::run(metrics).await?;
    loop {
        tracing::info!("Starting fetch...");
        if let Err(e) = fetcher.run().await {
            tracing::error!("Error while fetching blocks: {e}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Local network config from file
    #[clap(
        short,
        long,
        parse(from_os_str),
        value_name = "FILE",
        group = "config-group"
    )]
    config: Option<PathBuf>,
    /// Use testnet config, if not provided use mainnet config
    #[clap(short, long, parse(from_flag), group = "config-group")]
    testnet: bool,
}

fn download_config(testnet: bool) -> Result<String> {
    let url = if testnet {
        "https://ton.org/testnet-global.config.json"
    } else {
        "https://ton.org/global.config.json"
    };
    // actually it is sync, but we don't care
    let response = ureq::get(url).call()
        .map_err(|e| anyhow!("Error occurred while fetching config from {}: {:?}. Use --config if you have local config.", url, e))?;
    if response.status() != 200 {
        return Err(anyhow!(
            "Url {} responded with error code {}. Use --config if you have local config.",
            url,
            response.status()
        )
        .into());
    }
    Ok(response.into_string()?)
}

pub struct Subscriber(Arc<RwLock<BlockMetrics>>);

impl Subscriber {
    pub fn new(metrics: Arc<RwLock<BlockMetrics>>) -> Self {
        Self(metrics)
    }
}

#[async_trait]
impl BlockSubscriber for Subscriber {
    async fn visit_mc_block(&self, block: &Block) {
        let _ = self.0.write().await.update_shard_list(&block.read_extra().unwrap().read_custom().unwrap().unwrap());
        self.0.read().await.process_block(block).unwrap();
    }

    async fn visit_shard_block(&self, block: &Block) {
        self.0.read().await.process_block(block).unwrap();
    }
}