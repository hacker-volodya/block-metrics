use std::{collections::HashMap, error::Error, future::Future, net::SocketAddrV4, sync::Arc, time::Duration};

use adnl::{AdnlPeer, AdnlRawPublicKey};
use tokio::{net::TcpStream, sync::Mutex, task::JoinSet};
use tokio_tower::multiplex::{self, Client};
use ton_block::{Block, Deserializable, ShardDescr, ShardIdent};
use ton_liteapi::{layers::{WrapMessagesLayer, WrapService}, peer::LitePeer, tl::{adnl::Message, common::{BlockId, BlockIdExt}, request::{GetBlock, LookupBlock, Request, WaitMasterchainSeqno, WrappedRequest}, response::Response}};
use ton_networkconfig::{ConfigGlobal, ConfigPublicKey};
use anyhow::{Result, anyhow};
use ton_types::deserialize_tree_of_cells;
use tower::{Service, ServiceBuilder, ServiceExt};
use async_trait::async_trait;

#[async_trait]
pub trait BlockSubscriber {
    fn visit_mc_block(&self, block: &Block);
}

pub struct BlockFetcher<S: BlockSubscriber> {
    subscriber: S,
    nodes: Vec<Arc<Mutex<Node>>>,
}

impl<S: BlockSubscriber> BlockFetcher<S> {
    pub fn new(config: ConfigGlobal, subscriber: S) -> Self {
        let mut nodes = Vec::new();
        for liteserver in config.liteservers.iter() {
            let ConfigPublicKey::Ed25519 { key } = liteserver.id;
            nodes.push(Arc::new(Mutex::new(Node::new(key, liteserver.socket_addr()))));
        }
        Self { nodes, subscriber }
    }

    pub fn concurrent_query<T: Send + 'static, Fut: Send + 'static + Future<Output=T>, F: Fn(Arc<Mutex<Node>>) -> Fut>(&self, f: F) -> JoinSet<T> {
        let mut futures = JoinSet::new();
        for node in self.nodes.iter() {
            futures.spawn(f(node.clone()));
        }
        return futures
    }

    pub async fn get_last_seqno(&self) -> u32 {
        loop {
            let mut mc_last_task = self.concurrent_query(|node| async move {
                let mut n = node.lock().await;
                n.get_latest_mc_block_id().await.seqno
            });
            let mut last_seqno = 0;
            while let Some(result) = mc_last_task.join_next().await {
                if let Ok(seqno) = result {
                    if seqno > last_seqno {
                        last_seqno = seqno;
                    }
                }
            }
            if last_seqno == 0 {
                tokio::time::sleep(Duration::from_millis(300)).await;
                continue
            }
            return last_seqno
        }
    }

    pub async fn next_mc_block_id(&self, last_seqno: u32) -> BlockIdExt {
        loop {
            let mut task = self.concurrent_query(|node| async move {
                node.lock().await.find_mc_block(last_seqno + 1).await
            });
            while let Some(result) = task.join_next().await {
                if let Ok(block_id) = result {
                    return block_id
                }
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    pub async fn get_block(&self, block_id: BlockIdExt) -> Block {
        loop {
            let mut task = self.concurrent_query(|node| {
                let id = block_id.clone();
                async move {
                    node.lock().await.get_block(id).await
                }
            });
            while let Some(result) = task.join_next().await {
                if let Ok(block) = result {
                    return block
                }
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    pub fn get_mc_shards(mc_block: Block) -> Result<Vec<(ShardIdent, ShardDescr)>> {
        let custom = mc_block.read_extra()?.read_custom()?.ok_or(anyhow!("no custom"))?;
        let mut shards = Vec::new();
        custom.shards().iterate_shards(|ident, desc| {
            shards.push((ident, desc));
            Ok(true)
        })?;
        Ok(shards)
    }

    pub async fn start(&self) -> Result<()> {
        let mut last_seqno = tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => return Err(anyhow!("Timeout while fetching last seqno")),
            seqno = self.get_last_seqno() => seqno,
        };
        let mut shards_last_seqno = HashMap::new();
        loop {
            // download last masterchain block with shards
            let mc_block_id = self.next_mc_block_id(last_seqno).await;
            last_seqno = mc_block_id.seqno;
            let mc_block = self.get_block(mc_block_id).await;
            self.subscriber.visit_mc_block(&mc_block).await;
            if let Ok(shards) = Self::get_mc_shards(mc_block) {
                for (ident, desc) in shards {
                    shards_last_seqno.insert(ident, desc.seq_no);
                }
            }
        }
    }
}

struct Node {
    public: AdnlRawPublicKey,
    addr: SocketAddrV4,
    service: Option<WrapService<Client<LitePeer<AdnlPeer<TcpStream>>, Box<dyn Error + Sync + Send>, Message>>>,
}

impl Node {
    pub fn new(public: [u8; 32], addr: SocketAddrV4) -> Self {
        Self { public: AdnlRawPublicKey::from(public), addr, service: None }
    }

    async fn connect(&mut self) -> Result<()> {
        let adnl = AdnlPeer::connect(&self.public, self.addr).await?;
        let lite = LitePeer::new(adnl);
        self.service = Some(ServiceBuilder::new()
            .layer(WrapMessagesLayer)
            .service(multiplex::Client::<_, Box<dyn std::error::Error + Send + Sync + 'static>, _>::new(lite)));
        Ok(())
    }

    async fn call(&mut self, req: WrappedRequest) -> Response {
        loop {
            while self.service.is_none() {
                let result = self.connect().await;
                if result.is_err() {
                    tokio::time::sleep(Duration::from_millis(300));
                }
            }
            let ready_service = match self.service.as_mut().unwrap().ready().await {
                Ok(x) => x,
                Err(_) => {
                    self.service = None;
                    tokio::time::sleep(Duration::from_millis(300));
                    continue;
                },
            };
            if let Ok(response) = ready_service.call(req.clone()).await {
                return response;
            } else {
                self.service = None;
                tokio::time::sleep(Duration::from_millis(300));
            }
        }
    }

    pub async fn get_latest_mc_block_id(&mut self) -> BlockIdExt {
        loop {
            let result = self.call(WrappedRequest{ wait_masterchain_seqno: None, request: Request::GetMasterchainInfo }).await;
            if let Response::MasterchainInfo(info) = result {
                return info.last;
            }
            tokio::time::sleep(Duration::from_millis(300));
        }
    }

    pub async fn find_mc_block(&mut self, seqno: u32) -> BlockIdExt {
        loop {
            let result = self.call(WrappedRequest {
                wait_masterchain_seqno: Some(WaitMasterchainSeqno { seqno, timeout_ms: 10000  }), 
                request: Request::LookupBlock(LookupBlock { mode: (), id: BlockId { workchain: -1, shard: 0x8000_0000_0000_0000, seqno }, lt: None, utime: None  }) 
            }).await;
            if let Response::BlockHeader(header) = result {
                return header.id
            }
            tokio::time::sleep(Duration::from_millis(300));
        }
    }

    pub async fn get_block(&mut self, id: BlockIdExt) -> Block {
        loop {
            let result = self.call(WrappedRequest {
                wait_masterchain_seqno: None, 
                request: Request::GetBlock(GetBlock { id: id.clone() }) 
            }).await;
            if let Response::BlockData(block) = result {
                if let Ok(block_cell) = deserialize_tree_of_cells(&mut block.data.as_slice()) {
                    if let Ok(block) = Block::construct_from_cell(block_cell) {
                        return block
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(300));
        }
    }
}