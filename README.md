# PM Alpha

这是一个面向 Polymarket 短周期市场的 Rust 机器人。当前版本已经改成“滚动管理未来两轮”的执行模型，不再使用旧的单轮状态机或 `probe`。`pre_open_dual_buy_taker_flip` 会启用 `user ws` / `market ws` 做 reactive 触发；`pre_open_dual_buy_paper_tpsl` 会启用实时盘口做 paper trading 模拟，并输出复盘日志和净值图表；`open_post_dual_buy_price_guard` 会在开盘后双边挂买单，并根据 PM 自身价格源做 2 分钟 / 50 美元条件约束。

## 当前支持的五种模式

### `pre_split_dual_sell`

流程是：

1. 提前对未来轮次执行 `splitPosition`
2. 在开盘前挂 YES / NO 双边卖单
3. 到撤单时点，撤掉所有未成交部分
4. 对剩余可配对的 YES / NO 尽量 `mergePositions`
5. 对剩余单边仓位持有到结算后 `redeemPositions`

### `pre_open_dual_buy`

流程是：

1. 不做 split
2. 在开盘前挂 YES / NO 双边买单
3. 到撤单时点，撤掉所有未成交部分
4. 已成交部分持有到结算后 `redeemPositions`

### `pre_open_dual_buy_taker_flip`

流程是：

1. 开盘前双边挂 YES / NO 的 maker 买单，例如 46c
2. 只有某一边“整单成交”时，才触发反向动作
3. 立刻按实时 `best ask` 加可配置 tick 滑点，用 `taker` 买入另一边固定金额，默认 `2 USDC`
4. 同时用 `taker` 卖出刚才整单成交的这一边
5. 如果只是部分成交，不触发反向逻辑，直接持有到结算
6. 开盘前把所有未成交部分撤掉
7. 剩余仓位持有到结算后 `redeemPositions`

其中：

- maker 整单成交优先由 `user ws` 实时触发
- 反向买价优先参考 `market ws` 的实时 `best ask`
- `reactive_buy_slippage_ticks` 用来控制在 `best ask` 基础上最多愿意多追几个 tick

### `pre_open_dual_buy_paper_tpsl`

流程是：

1. 盘前双边挂 YES / NO 的 `46c` maker 买单，但只做模拟，不发真实单
2. 只有某一边“整单成交”时，才触发 paper taker 逻辑
3. 立刻模拟 taker 买入相反方向的 `N + 10` 份，其中 `N` 是 maker 成交数量
4. 把这次 taker 买入拆成两部分看：
5. `N` 份用来和原先 maker 成交的那一边配平成完整仓位，拿到结算
6. 额外 `10` 份作为主动仓位，按 `+5% / +10% / +15% / +20%` 四套并行分支分别测试
7. 每套分支在达到对应 TP 时，一次性卖出完整的额外仓位；如果价格跌破 `0.50`，则一次性止损卖出
8. 运行过程中会持续记录 `jsonl` 事件日志、CSV 汇总和两版净值图：

其中：

- 第一版图表按正常手续费口径计算
- 第二版图表按 `30%` 手续费返还口径计算
- 输出文件会落在 `strategy.paper_output_dir/<启动时间>/`

### `open_post_dual_buy_price_guard`

流程是：

1. 新一轮开盘后立即双边挂 YES / NO 的 maker 买单，例如 `15c`
2. 只要当前时间仍在开盘后窗口内，就继续保留挂单
3. 同时轮询 PM 市场自身的价格快照，比较“当前参考价格”和“本场开盘参考价格”
4. 只要价格偏离仍在阈值内，例如 `50 美元`，挂单继续保留
5. 任意一边只要出现部分成交，就立刻撤掉另一边未完成挂单
6. 触发时间条件或价格偏离条件时，撤掉所有仍未成交的挂单
7. 已成交部分持有到结算后 `redeemPositions`

其中：

- 当前版本优先读取 PM 返回的 `openValue / openPrice / openPx` 作为开盘参考价格
- 当前价格优先读取 `currentValue / currentPrice / currentPx`
- 如果接口字段缺失，会回退到问题文本和 `xAxisValue / yAxisValue` 做兜底识别
- 部分成交即视为触发，程序会立即撤掉另一边，但已成交这一边的剩余未成交部分会继续留到窗口结束或被价格条件打掉

## 滚动窗口行为

机器人只维护未来 `2` 轮。

例子：

- 当前时间 `10:20:35`
- 当前场次是 `10:20-10:25`
- 机器人不会参与这轮
- 它会去管理 `10:25-10:30` 和 `10:30-10:35`
- 当 `10:25-10:30` 到了撤单时点并退出窗口后，会自动补进 `10:35-10:40`

这正对应你要的“永远只看下一轮和下下轮”的模式。

## 当前代码的关键变化

- 删除了旧的 `probe` 和通用旧版 `ws` 驱动
- 删除了旧策略里不再生效的 `tp1/tp2/wrong_leg` 参数
- 批量下单响应现在按“每笔订单”分别处理，不再因为一边成功一边失败就整轮报错
- 新增了“maker 整单成交后自动反手 taker”的策略模式，支持双边独立触发
- `pre_open_dual_buy_taker_flip` 重新引入 `user ws` / `market ws`，用实时成交和实时盘口提升 reactive 触发速度
- 新增 `reactive_buy_slippage_ticks`，用于控制 reactive taker buy 的追价容忍
- 新增 `pre_open_dual_buy_paper_tpsl` 实时模拟盘模式，用盘口深度模拟 maker/taker 成交、TP/SL 和结算
- 新增 `open_post_dual_buy_price_guard`，支持开盘后双边挂买、部分成交撤另一边，以及按 PM 自身价格源做时间/价格双条件撤单
- 新增 paper 事件日志、CSV 汇总，以及“正常手续费 / 30% 手续费返还”两套净值图表输出
- 开盘前撤单后，会基于真实链上余额判断是否可以 merge、是否需要等待结算后 redeem
- 保留后台 `redeem` 补扫，防止主流程偶发漏领

## 配置

示例配置：

- [config/pm-alpha.example.toml](/Users/zanjunlong/Desktop/vibe-coding/pm_51_51/config/pm-alpha.example.toml)

完整中文参数说明：

- [docs/PARAMETERS_CN.md](/Users/zanjunlong/Desktop/vibe-coding/pm_51_51/docs/PARAMETERS_CN.md)

## 运行

复制示例配置后运行：

```bash
cp config/pm-alpha.example.toml config/pm-alpha.toml
cargo run --release -- config/pm-alpha.toml
```

或者先编译再启动：

```bash
cargo build --release
./target/release/pm_alpha_1_0 config/pm-alpha.toml
```

## 环境变量

至少需要准备这些环境变量：

- `POLYGON_RPC_URL`
- `PM_ACC1_PRIVATE_KEY`
- `PM_ACC1_API_KEY`
- `PM_ACC1_API_SECRET`
- `PM_ACC1_API_PASSPHRASE`

如果你使用 relayer / SAFE，还需要：

- `PM_ACC1_RELAYER_API_KEY`
- `PM_ACC1_RELAYER_API_KEY_ADDRESS`

如果启用 Telegram，还需要：

- `PM_TELEGRAM_BOT_TOKEN`

这些变量的详细含义都已经写进 [docs/PARAMETERS_CN.md](/Users/zanjunlong/Desktop/vibe-coding/pm_51_51/docs/PARAMETERS_CN.md)。
