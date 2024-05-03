# Prometheus TON block metrics exporter

## Metrics

1. `in_msg_descr` - counts incoming messages by type (Transit, Final, Discarded Final, Immediate, Discarded Transit, External, IHR)
2. `out_msg_descr` - counts outcoming messages by type (Transit Requeued, Immediate, External, Deque Immediate, New, Dequeue, Dequeue Short, Transit)
3. `account_block_tx_latency` - transactions latency histogram (seconds from imported message utime to transaction utime)

## Usage

```bash
docker run -p 3000:3000 ghcr.io/hacker-volodya/block-metrics
docker run -p 3000:3000 ghcr.io/hacker-volodya/block-metrics --testnet
docker run -p 3000:3000 -v $PWD/global-config.json:./global-config.json ghcr.io/hacker-volodya/block-metrics --config ./global-config.json
```

Metrics are available at http://127.0.0.1:3000/metrics
