use std::collections::{HashMap, HashSet};

use anyhow::{bail, Result};
use prometheus::{
    core::Collector, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts,
};
use ton_block::{
    Block, HashmapAugType, InMsg, InMsgDescr, McBlockExtra, OutMsg, OutMsgDescr,
    ShardAccountBlocks, ShardIdent,
};

#[derive(Debug)]
pub struct BlockMetrics {
    shards: HashMap<ShardIdent, ShardMetrics>,
}

impl BlockMetrics {
    pub fn new() -> Result<Self> {
        Ok(Self {
            shards: HashMap::<_, _>::from([(
                ShardIdent::masterchain(),
                ShardMetrics::new(&ShardIdent::masterchain())?,
            )]),
        })
    }

    pub fn update_shard_list(&mut self, extra: &McBlockExtra) -> Result<()> {
        let mut unvisited_shards = self
            .shards
            .keys()
            .copied()
            .filter(|k| k != &ShardIdent::masterchain())
            .collect::<HashSet<_>>();
        extra.shards().iterate_shards(|ident, _| {
            unvisited_shards.remove(&ident);
            if let std::collections::hash_map::Entry::Vacant(e) = self.shards.entry(ident) {
                e.insert(ShardMetrics::new(&ident)?);
            }
            Ok(true)
        })?;
        for ident in unvisited_shards {
            self.shards.remove(&ident);
        }
        Ok(())
    }

    pub fn process_block(&self, block: &Block) -> Result<()> {
        let info = block.read_info()?;
        if let Some(shard) = self.shards.get(info.shard()) {
            shard.process_shard_block(block)?;
        } else {
            tracing::warn!("no shard {} in hashmap", info.shard());
        }
        Ok(())
    }
}

impl Collector for BlockMetrics {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        let mut result = Vec::new();
        for shard in self.shards.values() {
            result.append(&mut shard.desc());
        }
        result
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        let mut result = Vec::new();
        for shard in self.shards.values() {
            result.append(&mut shard.collect());
        }
        result
    }
}

#[derive(Debug)]
pub struct ShardMetrics {
    in_msg: InMsgMetrics,
    out_msg: OutMsgMetrics,
    account_blocks: AccountBlockMetrics,
}

impl ShardMetrics {
    pub fn new(shard: &ShardIdent) -> Result<Self> {
        Ok(Self {
            in_msg: InMsgMetrics::with_shard(shard)?,
            out_msg: OutMsgMetrics::with_shard(shard)?,
            account_blocks: AccountBlockMetrics::with_shard(shard)?,
        })
    }

    pub fn process_shard_block(
        &self,
        block: &Block,
    ) -> Result<()> {
        let extra = block.read_extra()?;
        self.in_msg
            .process_in_msg_descr(&extra.read_in_msg_descr()?)?;
        self.out_msg
            .process_out_msg_descr(&extra.read_out_msg_descr()?)?;
        self.account_blocks
            .process_account_blocks(&extra.read_account_blocks()?)?;
        Ok(())
    }
}

impl Collector for ShardMetrics {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        let mut result = self.in_msg.desc();
        result.append(&mut self.out_msg.desc());
        result.append(&mut self.account_blocks.desc());
        result
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        let mut result = self.in_msg.collect();
        result.append(&mut self.out_msg.collect());
        result.append(&mut self.account_blocks.collect());
        result
    }
}

#[derive(Debug)]
pub struct AccountBlockMetrics {
    tx_counter: IntCounter,
    tx_latency: Histogram,
}

impl AccountBlockMetrics {
    pub fn with_shard(shard: &ShardIdent) -> Result<Self> {
        let counter_vec = IntCounterVec::new(
            Opts::new(
                "account_block_tx_count",
                "ShardAccountBlocks transactions counter",
            ),
            &["shard"],
        )?;
        let hist_vec = HistogramVec::new(
            HistogramOpts::new("account_block_tx_latency", "ShardAccountBlocks transactions latency histogram (seconds from imported message utime to transaction utime)")
                    .buckets(vec![0., 5., 10., 15., 20., 30., 40., 50., 60., 70., 80., 90., 100., 110., 120., 150., 180., 210., 240., 270., 300., 360., 420., 480., 540., 600., 900., 1200., 1500., 1800., 2400., 3000., 3600.])
            , &["shard"])?;
        let shard = shard.to_string();
        Ok(Self {
            tx_counter: counter_vec.get_metric_with_label_values(&[&shard])?,
            tx_latency: hist_vec.get_metric_with_label_values(&[&shard])?,
        })
    }

    pub fn process_account_blocks(&self, account_blocks: &ShardAccountBlocks) -> Result<()> {
        account_blocks.iterate_objects(|account_block| {
            account_block.transaction_iterate(|tx| {
                self.tx_counter.inc();
                if let Some(in_msg) = tx.read_in_msg()? {
                    if let Some((utime, _)) = in_msg.at_and_lt() {
                        self.tx_latency.observe((tx.now - utime) as f64)
                    }
                }
                Ok(true)
            })?;
            Ok(true)
        })?;
        Ok(())
    }
}

impl Collector for AccountBlockMetrics {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        let mut result = self.tx_counter.desc();
        result.append(&mut self.tx_latency.desc());
        result
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        let mut result = self.tx_counter.collect();
        result.append(&mut self.tx_latency.collect());
        result
    }
}

#[derive(Debug)]
pub struct InMsgMetrics {
    counter_vec: IntCounterVec,
    external_counter: IntCounter,
    ihr_counter: IntCounter,
    immediate_counter: IntCounter,
    final_counter: IntCounter,
    transit_counter: IntCounter,
    discarded_final_counter: IntCounter,
    discarded_transit_counter: IntCounter,
}

impl InMsgMetrics {
    pub fn with_shard(shard: &ShardIdent) -> Result<Self> {
        let vec = IntCounterVec::new(
            Opts::new("in_msg_descr", "InMsgDescr message counter"),
            &["shard", "type"],
        )?;
        let shard = shard.to_string();
        let external_counter = vec.get_metric_with_label_values(&[&shard, "External"])?;
        let ihr_counter = vec.get_metric_with_label_values(&[&shard, "IHR"])?;
        let immediate_counter = vec.get_metric_with_label_values(&[&shard, "Immediate"])?;
        let final_counter = vec.get_metric_with_label_values(&[&shard, "Final"])?;
        let transit_counter = vec.get_metric_with_label_values(&[&shard, "Transit"])?;
        let discarded_final_counter =
            vec.get_metric_with_label_values(&[&shard, "Discarded Final"])?;
        let discarded_transit_counter =
            vec.get_metric_with_label_values(&[&shard, "Discarded Transit"])?;
        Ok(Self {
            counter_vec: vec,
            external_counter,
            ihr_counter,
            immediate_counter,
            final_counter,
            transit_counter,
            discarded_final_counter,
            discarded_transit_counter,
        })
    }

    pub fn process_in_msg_descr(&self, descr: &InMsgDescr) -> Result<()> {
        descr.iterate_objects(|msg| {
            let counter = match msg {
                InMsg::None => bail!("invalid message type None"),
                InMsg::External(_) => &self.external_counter,
                InMsg::IHR(_) => &self.ihr_counter,
                InMsg::Immediate(_) => &self.immediate_counter,
                InMsg::Final(_) => &self.final_counter,
                InMsg::Transit(_) => &self.transit_counter,
                InMsg::DiscardedFinal(_) => &self.discarded_final_counter,
                InMsg::DiscardedTransit(_) => &self.discarded_transit_counter,
            };
            counter.inc();
            Ok(true)
        })?;
        Ok(())
    }
}

impl Collector for InMsgMetrics {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        self.counter_vec.desc()
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.counter_vec.collect()
    }
}

#[derive(Debug)]
pub struct OutMsgMetrics {
    counter_vec: IntCounterVec,
    external_counter: IntCounter,
    new_counter: IntCounter,
    immediate_counter: IntCounter,
    transit_counter: IntCounter,
    dequeue_immediate: IntCounter,
    dequeue: IntCounter,
    dequeue_short: IntCounter,
    transit_requeued: IntCounter,
}

impl OutMsgMetrics {
    pub fn with_shard(shard: &ShardIdent) -> Result<Self> {
        let vec = IntCounterVec::new(
            Opts::new("out_msg_descr", "OutMsgDescr message counter"),
            &["shard", "type"],
        )?;
        let shard = shard.to_string();
        let external_counter = vec.get_metric_with_label_values(&[&shard, "External"])?;
        let new_counter = vec.get_metric_with_label_values(&[&shard, "New"])?;
        let immediate_counter = vec.get_metric_with_label_values(&[&shard, "Immediate"])?;
        let transit_counter = vec.get_metric_with_label_values(&[&shard, "Transit"])?;
        let dequeue_immediate = vec.get_metric_with_label_values(&[&shard, "Deque Immediate"])?;
        let dequeue = vec.get_metric_with_label_values(&[&shard, "Dequeue"])?;
        let dequeue_short = vec.get_metric_with_label_values(&[&shard, "Dequeue Short"])?;
        let transit_requeued = vec.get_metric_with_label_values(&[&shard, "Transit Requeued"])?;
        Ok(Self {
            counter_vec: vec,
            external_counter,
            new_counter,
            immediate_counter,
            transit_counter,
            dequeue_immediate,
            dequeue,
            dequeue_short,
            transit_requeued,
        })
    }

    pub fn process_out_msg_descr(&self, descr: &OutMsgDescr) -> Result<()> {
        descr.iterate_objects(|msg| {
            let counter = match msg {
                OutMsg::None => bail!("invalid message type None"),
                OutMsg::External(_) => &self.external_counter,
                OutMsg::New(_) => &self.new_counter,
                OutMsg::Immediate(_) => &self.immediate_counter,
                OutMsg::Transit(_) => &self.transit_counter,
                OutMsg::DequeueImmediate(_) => &self.dequeue_immediate,
                OutMsg::Dequeue(_) => &self.dequeue,
                OutMsg::DequeueShort(_) => &self.dequeue_short,
                OutMsg::TransitRequeued(_) => &self.transit_requeued,
            };
            counter.inc();
            Ok(true)
        })?;
        Ok(())
    }
}

impl Collector for OutMsgMetrics {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        self.counter_vec.desc()
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.counter_vec.collect()
    }
}
