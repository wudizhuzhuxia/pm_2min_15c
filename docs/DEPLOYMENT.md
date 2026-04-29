# 部署说明

## 1. 准备配置

```bash
cp config/pm-alpha.example.toml config/pm-alpha.toml
```

如果你要直接跑这次的 BTC 5m 开盘后双边 `15c` 策略，建议直接用：

```bash
cp config/pm-btc-5m-open-post.example.toml config/pm-alpha.toml
```

先根据你要跑的市场修改这几个核心字段：

- `market.series_slug`
- `strategy.mode`
- `strategy.round_interval_secs`
- `strategy.quote_start_before_open_secs`
- `strategy.quote_cancel_before_open_ms`
- `strategy.pre_split_before_open_secs`
- `strategy.quote_start_after_open_secs`
- `strategy.quote_cancel_after_open_secs`
- `strategy.order_size`
- `strategy.yes_price`
- `strategy.no_price`
- `strategy.open_price_max_deviation`

完整说明见：

- [docs/PARAMETERS_CN.md](/Users/zanjunlong/Desktop/vibe-coding/pm_51_51_open_after_2m/docs/PARAMETERS_CN.md)

## 2. 准备环境变量

至少需要：

```bash
export POLYGON_RPC_URL="https://your-polygon-rpc"
export PM_ACC1_PRIVATE_KEY="0x..."
export PM_ACC1_API_KEY="..."
export PM_ACC1_API_SECRET="..."
export PM_ACC1_API_PASSPHRASE="..."
```

如果你走 `relayer` / `browser_proxy`：

```bash
export PM_ACC1_RELAYER_API_KEY="..."
export PM_ACC1_RELAYER_API_KEY_ADDRESS="0x..."
```

如果启用 Telegram：

```bash
export PM_TELEGRAM_BOT_TOKEN="..."
```

## 3. 编译

```bash
cargo build --release
```

## 4. 启动

```bash
./target/release/pm_alpha_1_0 config/pm-alpha.toml
```

## 5. systemd 建议

如果要长期运行，建议交给 `systemd`：

- `WorkingDirectory` 指向仓库目录
- `ExecStart` 指向 `target/release/pm_alpha_1_0 config/pm-alpha.toml`
- 环境变量放进单独的 env 文件
- 重启策略建议使用 `Restart=always`
- 推荐加上 `LimitNOFILE=65535`
- 推荐用 `log_json = true`，方便 `journalctl` / Loki / ELK 收集

## 6. 生产环境建议

- 先用 `dry_run = true` 跑一段时间，确认轮次、开盘时点、撤单时点正确
- 再切到 `dry_run = false`
- `execution.settled_redeem_scan_enabled` 建议开启
- 如果你跑 15 分钟市场，记得把 `strategy.round_interval_secs` 改成 `900`
- 对于 `open_post_dual_buy_price_guard` 模式，重点确认：
  - `yes_price / no_price = 0.15`
  - `quote_cancel_after_open_secs = 120`
  - `open_price_max_deviation = 50`
  - 日志里能看到 PM 价格快照刷新和部分成交撤另一边
