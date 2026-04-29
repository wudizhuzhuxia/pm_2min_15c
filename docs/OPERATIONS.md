# 运维说明

## 启动

```bash
cargo run --release -- config/pm-alpha.toml
```

或者：

```bash
./target/release/pm_alpha_1_0 config/pm-alpha.toml
```

## 停止

如果你是前台运行，直接 `Ctrl+C`。

如果你是 `systemd` 运行：

```bash
sudo systemctl stop pm-alpha-15m.service
```

请把服务名替换成你实际使用的 unit name。

## 看日志

前台运行时直接看终端。

`systemd` 运行时：

```bash
journalctl -u pm-alpha-15m -f
```

## 建议关注的日志关键词

- `tracking future round`
- `split completed for managed round`
- `submitting pre-open CLOB orders`
- `order accepted by CLOB`
- `order was not resting after batch submission`
- `processed pre-open cancel for resting orders`
- `refreshed PM reference price snapshot`
- `first maker match observed; canceling the opposite leg`
- `merged cancel-time full-set balance back to collateral`
- `redeemed settled round balances`

## 常见排查思路

### 1. 服务在跑，但没有挂单

先确认这几个点：

- 当前是否还在 `dry_run = true`
- `market.series_slug` 和 `strategy.round_interval_secs` 是否匹配
- 启动时是否已经错过下一轮的撤单时点
- `split` 是否失败，导致卖单模式没有可卖仓位
- API key / relayer / signer / funder 环境变量是否配置完整

### 2. 一边挂上，一边没挂上

当前代码会把批量返回按“每条订单”分别处理，所以常见原因是：

- 其中一边 `post-only` 穿价，CLOB 直接拒绝 resting
- 另一边仍然成功成为 live order

这不再会把整轮直接判死，但你会在日志里看到一边 `order accepted by CLOB`，另一边 `order was not resting after batch submission`。

### 3. 开盘前撤单之后还有仓位

这是正常现象，只说明：

- 有一部分已经成交
- 程序撤掉的是“未成交部分”

后续行为取决于模式：

- `pre_split_dual_sell` 会先尝试 merge 可配对余额
- 剩余单边仓位会等结算后 redeem
- `pre_open_dual_buy` 则直接持有到结算后 redeem

### 4. 开盘后价格条件模式提前撤单

如果你跑的是 `open_post_dual_buy_price_guard`，日志里出现下面这些是正常的：

- `refreshed PM reference price snapshot`
- `PM price drift exceeded configured threshold; canceling all resting orders`
- `first maker match observed; canceling the opposite leg`

对应含义分别是：

- 程序拿到了 PM 自身当前价格和开盘价格
- 当前价格相对开盘价偏离超过阈值，例如 `50 美元`
- 任意一边已经有部分成交，另一边正在被撤掉

### 5. 已结算但余额没回来

先看主日志里是否有：

- `redeemed settled round balances`

如果没有，再确认：

- `execution.settled_redeem_scan_enabled` 是否开启
- `POLYGON_RPC_URL` 是否可用
- relayer 或 RPC 模式是否和你的账户类型匹配
