use std::{collections::{HashSet, VecDeque}, error::Error, future::Future, net::SocketAddrV4, sync::Arc, time::Duration};

use adnl::{AdnlPeer, AdnlRawPublicKey};
use tokio::{net::TcpStream, sync::Mutex};
use tokio_tower::multiplex::{self, Client};
use ton_block::{Block, Deserializable, ShardDescr, ShardIdent, UnixTime32};
use ton_liteapi::{layers::{WrapMessagesLayer, WrapService}, peer::LitePeer, tl::{adnl::Message, common::{BlockId, BlockIdExt, Int256}, request::{GetBlock, LookupBlock, Request, WaitMasterchainSeqno, WrappedRequest}, response::Response}};
use ton_networkconfig::{ConfigGlobal, ConfigPublicKey};
use anyhow::{Result, anyhow};
use ton_types::deserialize_tree_of_cells;
use tower::{Service, ServiceBuilder, ServiceExt};
use async_trait::async_trait;
use futures::{future, stream::{FuturesUnordered, StreamExt}, Stream};

#[async_trait]
pub trait BlockSubscriber {
    async fn visit_mc_block(&self, block: &Block);
    async fn visit_shard_block(&self, block: &Block);
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

    fn concurrent_query<T: Send + 'static, Fut: Send + 'static + Future<Output=Result<T>>, F: Fn(Arc<Mutex<Node>>) -> Fut>(&self, f: F) -> impl Stream<Item = Result<T>> {
        let futures = FuturesUnordered::new();
        for node in self.nodes.iter() {
            futures.push(f(node.clone()));
        }
        futures
    }

    async fn query_first<T: Send + 'static, Fut: Send + 'static + Future<Output=Result<T>>, F: Fn(Arc<Mutex<Node>>) -> Fut>(&self, f: F) -> Result<T> {
        self.concurrent_query(f).filter_map(|x| future::ready(x.ok())).next().await.ok_or(anyhow!("No results"))
    }

    pub async fn get_last_seqno(&self) -> Result<u32> {
        let mut stream = self.concurrent_query(|node| async move {
            let mut n = node.lock().await;
            n.get_latest_mc_block_id().await
        });
        let mut last_seqno = 0;
        let mut timeout = Duration::from_secs(10);
        loop {
            tokio::select! {
                maybe_result = stream.next() => {
                    match maybe_result {
                        Some(Ok(mc_block_id)) => {
                            last_seqno = last_seqno.max(mc_block_id.seqno);
                            timeout = Duration::from_secs(1);
                        },
                        Some(Err(_)) => (), // Ignore errors
                        None => break, // All futures have been processed
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    break;
                }
            }
        }

        if last_seqno > 0 {
            Ok(last_seqno)
        } else {
            Err(anyhow!("Could not fetch latest masterchain seqno (no liteserver is responding?)"))
        }
    }

    pub async fn next_mc_block_id(&self, last_seqno: u32) -> Result<BlockIdExt> {
        self.query_first(|node| async move {
            node.lock().await.find_mc_block(last_seqno + 1).await
        }).await
    }

    pub async fn get_block(&self, block_id: &BlockIdExt) -> Result<Block> {
        self.query_first(|node| {
            let id = block_id.clone();
            async move {
                node.lock().await.get_block(id).await
            }
        }).await
    }

    pub fn get_mc_shards(mc_block: &Block) -> Result<Vec<(ShardIdent, ShardDescr)>> {
        let custom = mc_block.read_extra()?.read_custom()?.ok_or(anyhow!("no custom"))?;
        let mut shards = Vec::new();
        custom.shards().iterate_shards(|ident, desc| {
            shards.push((ident, desc));
            Ok(true)
        })?;
        Ok(shards)
    }

    pub fn get_parents(block: &Block) -> Result<Vec<BlockIdExt>> {
        let info = block.read_info()?;
        let ids = info.read_prev_ids()?;
        Ok(ids.iter().map(|id| BlockIdExt {
            workchain: id.shard_id.workchain_id(),
            shard: id.shard_id.shard_prefix_with_tag(),
            seqno: id.seq_no,
            root_hash: Int256(*id.root_hash.as_array()),
            file_hash: Int256(*id.file_hash.as_array()),
        }).collect())
    }

    pub async fn fetch_next_mc_block(&self, mc_last_seqno: u32, shards_last_seqno: HashSet<BlockIdExt>) -> Result<HashSet<BlockIdExt>> {
        let mut visited = shards_last_seqno;
        let mc_block_id = self.next_mc_block_id(mc_last_seqno).await?;
        let mc_block = self.get_block(&mc_block_id).await?;
        let latency = UnixTime32::now().as_u32() - mc_block.read_info()?.gen_utime().as_u32();
        tracing::info!(latency, id=%mc_block_id, "received mc block");
        if latency > 120 {
            return Err(anyhow!("out of sync: block was generated {latency} sec ago"));
        }
        let shards = Self::get_mc_shards(&mc_block)?;
        let mut to_visit = VecDeque::new();
        let mut fetched_blocks = Vec::new();
        let traverse_old_blocks = !visited.is_empty();
        for (ident, desc) in shards {
            to_visit.push_back(BlockIdExt {
                workchain: ident.workchain_id(),
                shard: ident.shard_prefix_with_tag(),
                seqno: desc.seq_no,
                root_hash: Int256(*desc.root_hash.as_array()),
                file_hash: Int256(*desc.file_hash.as_array()),
            });
        }
        let shards_last_seqno = HashSet::from_iter(to_visit.clone().into_iter());
        while let Some(block_id) = to_visit.pop_front() {
            if visited.contains(&block_id) {
                continue
            }
            let block = self.get_block(&block_id).await?;
            let shard_latency = UnixTime32::now().as_u32() - block.read_info()?.gen_utime().as_u32();
            if shard_latency - latency > 30 {
                tracing::warn!("possible shard leak!");
                continue
            }
            tracing::info!(latency=shard_latency, id=%block_id, "received shard block");
            if traverse_old_blocks {
                visited.insert(block_id);
                for id in Self::get_parents(&block)? {
                    if !visited.contains(&id) {
                        to_visit.push_back(id);
                    }
                }
            }
            fetched_blocks.push(block);
        }
        self.subscriber.visit_mc_block(&mc_block).await;
        // from oldest to newest
        for block in fetched_blocks.iter().rev() {
            self.subscriber.visit_shard_block(block).await;
        }
        Ok(shards_last_seqno)
    }

    pub async fn run(&self) -> Result<()> {
        let mut last_seqno = self.get_last_seqno().await?;
        let mut shards_last_seqno = HashSet::new();
        loop {
            shards_last_seqno = tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(20)) => return Err(anyhow!("Timeout while fetching next masterchain block")),
                result = self.fetch_next_mc_block(last_seqno, shards_last_seqno) => result,
            }?;
            last_seqno += 1;
        }
    }
}

type NodeService = WrapService<Client<LitePeer<AdnlPeer<TcpStream>>, Box<dyn Error + Sync + Send>, Message>>;

macro_rules! unwrap_response {
    ($variant:ident, $result:expr) => {
        match $result {
            Response::$variant(inner) => Ok(inner),
            _ => Err(anyhow!("Liteserver query: Expected {} but got unexpected response {:?}", stringify!($variant), $result)),
        }
    };
}

struct Node {
    public: AdnlRawPublicKey,
    addr: SocketAddrV4,
    service: Option<NodeService>,
}

impl Node {
    pub fn new(public: [u8; 32], addr: SocketAddrV4) -> Self {
        Self { public: AdnlRawPublicKey::from(public), addr, service: None }
    }

    async fn connect(&mut self) -> Result<&mut NodeService> {
        if self.service.is_none() {
            let adnl = AdnlPeer::connect(&self.public, self.addr).await?;
            let lite = LitePeer::new(adnl);
            let service = ServiceBuilder::new()
                .layer(WrapMessagesLayer)
                .service(multiplex::Client::<_, Box<dyn std::error::Error + Send + Sync + 'static>, _>::new(lite));
            self.service = Some(service);
        }
        Ok(self.service.as_mut().unwrap())
    }

    async fn call(&mut self, req: WrappedRequest) -> Result<Response> {
        let ready_service = self.connect().await?.ready().await?;
        Ok(ready_service.call(req.clone()).await?)
    }

    pub async fn get_latest_mc_block_id(&mut self) -> Result<BlockIdExt> {
        let result = self.call(WrappedRequest { wait_masterchain_seqno: None, request: Request::GetMasterchainInfo }).await?;
        Ok(unwrap_response!(MasterchainInfo, result)?.last)
    }

    pub async fn find_mc_block(&mut self, seqno: u32) -> Result<BlockIdExt> {
        let result = self.call(WrappedRequest {
            wait_masterchain_seqno: Some(WaitMasterchainSeqno { seqno, timeout_ms: 10000  }), 
            request: Request::LookupBlock(LookupBlock { mode: (), id: BlockId { workchain: -1, shard: 0x8000_0000_0000_0000, seqno }, seqno: Some(()), lt: None, utime: None  }) 
        }).await?;
        Ok(unwrap_response!(BlockHeader, result)?.id)
    }

    pub async fn get_block(&mut self, id: BlockIdExt) -> Result<Block> {
        let result = self.call(WrappedRequest {
            wait_masterchain_seqno: None, 
            request: Request::GetBlock(GetBlock { id: id.clone() }) 
        }).await?;
        let block = unwrap_response!(BlockData, result)?;
        let block_cell = deserialize_tree_of_cells(&mut block.data.as_slice())?;
        Ok(Block::construct_from_cell(block_cell)?)
    }
}