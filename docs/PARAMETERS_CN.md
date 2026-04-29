# 参数说明

这份文档只解释当前代码里真正还会被读取和生效的参数。旧策略里那些 `tp1/tp2/wrong_leg/probe/ws` 相关字段已经从配置模型中删除，不需要再保留。

## 设计原则

- 机器人只管理“未来轮次”，不会接管当前已经开盘的场次。
- 默认只维护未来 `2` 轮，也就是一个滚动窗口。
- 每一轮只做三件事：提前准备、开盘前撤未成交、结算后 redeem。
- `pre_split_dual_sell` 模式额外会在挂卖前先 split，并在撤单后尽量 merge 回完整对冲仓。
- `pre_open_dual_buy` 模式不 split，不 merge，只处理挂买、撤单、持有到结算、redeem。

## `[app]`

### `instance_name`

这是实例名称，只用于日志和通知中区分进程。它不会影响交易逻辑，也不会影响下单账户。你可以把它理解成“机器人别名”。

适合什么时候改：

- 同一台机器跑多套机器人时。
- 你想在 `journalctl` 或 Telegram 里一眼区分 5m、15m、测试版、生产版时。

### `dry_run`

这个开关决定机器人是否真的执行交易动作。

- `false`：真实执行 `split / 下单 / 撤单 / merge / redeem`
- `true`：只打印计划动作，不会真的动链上和 CLOB

适合什么时候开：

- 刚改完配置，想先确认轮次滚动和时间点是否符合预期。
- 想在生产环境演练日志，但不碰资金。

注意点：

- `dry_run` 下不会产生真实持仓，所以后续的 merge / redeem 也只是模拟。

### `log_level`

控制日志详细程度。常见可选值是 `info`、`debug`、`warn`。

- `info`：日常运行推荐
- `debug`：排查时间点、接口返回、订单细节时更有用
- `warn`：只看警告和错误

## `[network]`

### `clob_rest_url`

Polymarket CLOB REST 接口地址。挂单、撤单、盘口查询都走这里。除非官方地址变更，否则一般不用改。

### `relayer_rest_url`

Polymarket relayer 地址。只有在 `execution.onchain_execution_mode = "relayer"` 或代理钱包场景下才真正重要，因为 gasless 的 `approve / split / merge / redeem` 都要走它。

### `gamma_rest_url`

市场发现接口地址。机器人会从这里发现未来轮次，并据此构建“只维护未来两轮”的滚动窗口。

### `data_api_url`

后台补扫已结算仓位时使用的接口地址。它主要用于兜底场景，比如主流程漏掉了某个已结算仓位的 `redeemPositions`，后台扫到后会再补领一次。

### `polygon_rpc_url_env`

这里填的不是 RPC 地址本身，而是“RPC 地址存在哪个环境变量里”。程序会在启动时读取这个环境变量。

这样设计的原因是：

- 避免把 RPC 和密钥直接写进配置文件。
- 方便不同机器通过环境变量切换节点。

### `prefer_http2`

是否优先使用 HTTP/2。开启后更容易复用连接，通常更适合这种频繁请求的机器人场景。一般保持 `true` 就可以。

### `connect_timeout_ms`

连接建立超时时间，单位毫秒。太小会让网络稍微抖动时就频繁报错，太大又会拖慢失败重试。当前默认值偏低，是为了偏向低延迟交易。

### `request_timeout_ms`

单次 HTTP 请求总超时，单位毫秒。它影响挂单、撤单、查盘口等 REST 请求。这个值比 `connect_timeout_ms` 大一些是正常的，因为请求除了建连还要等待返回。

### `keepalive_interval_secs`

HTTP 长连接保活间隔。它影响底层连接复用的稳定性，一般不用频繁调整。

### `transaction_timeout_secs`

链上或 relayer 交易确认的等待超时。`split / merge / redeem` 都会受它影响。

你需要知道的关键点：

- 值太小：网络稍慢就容易误判为超时
- 值太大：真出问题时，机器人会卡更久才继续

### `tcp_nodelay`

是否关闭 Nagle 算法，通常用于减少小包发送延迟。对于交易程序，默认保持 `true` 更合理。

## `[market]`

### `series_slug`

要交易的市场系列。例如：

- `btc-5m`
- `btc-15m`

这个字段必须和 `strategy.round_interval_secs` 对应起来。

正确示例：

- `series_slug = "btc-5m"` 配 `round_interval_secs = 300`
- `series_slug = "btc-15m"` 配 `round_interval_secs = 900`

如果两者不一致，机器人会在错误的时间窗口里挂单和撤单。

### `discovery_lookahead_secs`

发现未来市场时，向前看多远。这个值必须足够覆盖你想维护的窗口大小。

比如：

- 你只维护未来两轮
- 每轮 5 分钟
- 那至少要能覆盖未来 10 分钟以上

当前默认 `1200` 秒比较保守，够大多数 5m 和 15m 场景使用。

## `[strategy]`

### `mode`

这是最核心的模式开关，目前有三种：

#### `pre_split_dual_sell`

执行逻辑是：

1. 在开盘前更早的时点先 `split`
2. 开盘前挂 YES 和 NO 的双边卖单
3. 到撤单时点，把所有未成交部分撤掉
4. 如果 YES/NO 两边仍有可配对余额，就 `merge`
5. 剩余单边仓位持有到结算，然后 `redeemPositions`

适用场景：

- 你已经接受“先拿到双边仓位，再卖出去”的资金路径
- 你想做类似“split 后挂双边 54c 卖”的模式

#### `pre_open_dual_buy`

执行逻辑是：

1. 不做 `split`
2. 直接在开盘前挂 YES 和 NO 的双边买单
3. 到撤单时点，把所有未成交部分撤掉
4. 已成交的部分持有到结算
5. 结算后 `redeemPositions`

适用场景：

- 你想模仿“开盘前双边挂 46c 买”的模式
- 你不想走 split / merge 这条链上流程

#### `pre_open_dual_buy_taker_flip`

执行逻辑是：

1. 开盘前双边挂 YES 和 NO 的 maker 买单，通常是 `46c`
2. 只有当某一边 maker 买单“整单成交”时，才触发反向动作
3. 立刻用 `taker` 买入另一边固定金额，默认 `2 USDC`
4. 同时用 `taker` 卖掉刚才整单成交的这一边
5. 如果只是部分成交，不触发反向逻辑
6. 部分成交和 taker 后残留仓位都直接持有到结算
7. 到撤单时点，把本场次所有未成交部分撤掉
8. 结算后 `redeemPositions`

这正对应你现在要测试的新模式。

这个模式现在还有两个额外特征：

- maker 整单成交优先通过 `user ws` 的实时订单更新触发，不再只靠 REST 轮询
- 反向 taker 买入会优先参考 `market ws` 的实时 `best ask`，并在此基础上叠加可配置的 tick 滑点

这个模式的核心思想不是“提前挂单后等全部拿到结算”，而是：

- 用双边 `46c` maker 去等便宜筹码
- 某一边整单拿到之后，立刻切到另一边
- 原来那一边马上用 taker 出掉
- 只保留你要留下的反向仓位

#### `pre_open_dual_buy_paper_tpsl`

执行逻辑是：

1. 盘前双边挂 YES 和 NO 的 `46c` maker 买单，但只做模拟，不发真实订单
2. 只有某一边 maker 买单“整单成交”时，才触发后续 paper taker 逻辑
3. 立刻模拟 taker 买入相反方向的 `N + extra` 份，`N` 是盘前 maker 成交数量
4. 其中 `N` 份和原先 maker 成交的那一边形成完整仓位，拿到结算
5. 额外那部分仓位单独作为主动仓位，按多组 TP 参数并行测试
6. 每组 TP 分支在达到目标涨幅时，一次性把额外仓位全部卖掉
7. 如果价格跌破统一止损价，则把剩余额外仓位一次性市价卖掉
8. 运行过程中持续输出事件日志、CSV 汇总和两套净值图表

这个模式的特点是：

- 用真实盘口和真实时间驱动模拟
- 不发真实订单，不动真实资金
- maker 成交按“触价 + 可见深度足够”的规则模拟
- taker 买卖按真实 order book 深度估算均价和手续费
- 自动生成两套净值曲线：正常手续费 / 含 30% 手续费返还

#### `open_post_dual_buy_price_guard`

执行逻辑是：

1. 新场次开盘后立即双边挂 YES / NO 买单
2. 只在“开盘后允许窗口”内保留这些挂单
3. 程序持续读取 PM 市场自身的价格快照
4. 如果 PM 当前参考价格相对本场开盘参考价格偏离超过阈值，就撤掉所有未成交挂单
5. 任意一边只要发生部分成交，就立刻撤掉另一边未完成挂单
6. 触发时间上限后，再把剩余未成交部分全部撤掉
7. 已成交部分继续持有到结算并 `redeem`

当前实现里，价格字段优先级是：

- 开盘参考价优先看 `openValue / openPrice / openPx`
- 当前参考价优先看 `currentValue / currentPrice / currentPx`
- 如果这些字段缺失，则回退到问题文案中的价格以及 `xAxisValue / yAxisValue`

### `round_interval_secs`

市场每轮长度，单位秒。

常见值：

- `300`：5 分钟轮次
- `900`：15 分钟轮次

它直接决定：

- 下一轮什么时候开始
- 挂单窗口如何计算
- 撤单时点如何计算
- 机器人如何补充“下下轮”

### `window_size_rounds`

滚动窗口大小。当前你的需求就是未来两轮，所以默认值是 `2`。

行为上可以理解为：

- 只要窗口里掉出一轮
- 机器人就去再补下一轮进来

如果以后你想挂得更远，可以把它改大，但这会增加资金占用和管理复杂度。

### `quote_start_before_open_secs`

距离开盘多少秒开始挂单。它定义的是“最早可以挂单的时刻”。

例子：

- 开盘时间 `10:25:00`
- 参数是 `180`
- 那挂单开始时点就是 `10:22:00`

这个值越大，挂单越早，越可能排到更靠前的队列位置，但也更早暴露在盘口变化里。

### `quote_cancel_before_open_ms`

距离开盘多少毫秒撤单。它定义的是“本场次未成交部分的最后退出时刻”。

例子：

- 开盘时间 `10:25:00.000`
- 参数是 `1000`
- 那撤单目标时刻就是 `10:24:59.000`

你的需求里，重点就是：

- 已成交的部分继续持有
- 未成交的部分必须在开盘前撤掉

这个参数就是专门控制那一刀撤单时间的。

### `pre_split_before_open_secs`

只在 `pre_split_dual_sell` 模式下生效。表示距离开盘多少秒开始先做 `split`。

为什么它通常应该大于等于 `quote_start_before_open_secs`：

- 因为只有 split 成功了，你才有 YES / NO 代币可卖
- 如果 split 比挂单还晚，可能会来不及在开盘前把卖单挂上去

默认值 `240` 秒，配合 `quote_start_before_open_secs = 180` 的意思是：

- 提前 4 分钟先 split
- 提前 3 分钟开始挂卖单

### `quote_start_after_open_secs`

只在 `open_post_dual_buy_price_guard` 模式下生效。

表示开盘后多少秒开始挂单。默认 `0`，也就是新场次一开盘就挂。

例如：

- 开盘时间 `10:25:00`
- 参数是 `0`
- 那挂单开始时点就是 `10:25:00`

### `quote_cancel_after_open_secs`

只在 `open_post_dual_buy_price_guard` 模式下生效。

表示开盘后最多保留挂单多久。

例如：

- 开盘时间 `10:25:00`
- 参数是 `120`
- 那撤单目标时刻就是 `10:27:00`

你的 BTC 5m 需求里，这个参数就对应“开盘后 2 分钟内”。

### `order_size`

每条腿的挂单数量。

在不同模式下的含义略有不同：

- `pre_split_dual_sell`：表示 YES 卖单数量和 NO 卖单数量，同时也决定默认 split 数量
- `pre_open_dual_buy`：表示 YES 买单数量和 NO 买单数量
- `pre_open_dual_buy_taker_flip`：表示 YES / NO 两边提前挂出去的 maker 买单数量
- `pre_open_dual_buy_paper_tpsl`：表示纸上模拟的 YES / NO 两边盘前 maker 买单数量
- `open_post_dual_buy_price_guard`：表示 YES / NO 两边开盘后挂出去的 maker 买单数量

这个值影响的是“代币数量”，不是总 USDC 预算。真实资金占用还要乘以价格。

### `yes_price`

YES 腿的挂单价格。

在 `pre_split_dual_sell` 模式里，它是挂卖价。
在 `pre_open_dual_buy`、`pre_open_dual_buy_taker_flip`、`pre_open_dual_buy_paper_tpsl` 和 `open_post_dual_buy_price_guard` 模式里，它是挂买价。

如果你未来想做不对称配置，比如更偏向 YES，可以单独把这个值改掉。

### `no_price`

NO 腿的挂单价格，含义和 `yes_price` 相同，只是作用在 NO 腿。

如果未来你想做“YES 0.46、NO 0.45”或者“YES 0.54、NO 0.55”这种不对称策略，就是改这里。

### `open_price_max_deviation`

这个参数只在 `open_post_dual_buy_price_guard` 模式下生效。

它表示：

- PM 当前参考价格
- 相对本场开盘参考价格
- 最多允许偏离多少美元

例如：

- 本场开盘参考价格是 `95000`
- 当前参考价格是 `95042`
- 参数是 `50`
- 那么挂单继续保留

如果当前参考价格变成 `95060`，偏离达到 `60`，程序就会立即撤掉所有未成交挂单。

### `reactive_opposite_taker_usdc`

这个参数只在 `pre_open_dual_buy_taker_flip` 模式下生效。

它表示：

- 当 YES 或 NO 的 maker 买单“整单成交”以后
- 立刻去用 taker 买入另一边
- 这笔 taker 买入要花多少 USDC

例如：

- 你提前双边挂 `46c`
- `YES` 整单成交了
- 如果这个参数是 `2.0`
- 那么程序会立刻用 taker 去买 `2 USDC` 的 `NO`
- 同时再用 taker 把刚拿到的 `YES` 卖掉

你可以把这个参数理解成“反手切仓的固定力度”。

值变大意味着：

- 反手保留的另一边仓位更大
- 对盘口流动性的要求也更高

值变小意味着：

- 反手动作更轻
- 对成交深度要求更低

### `reactive_buy_slippage_ticks`

这个参数只在 `pre_open_dual_buy_taker_flip` 模式下生效。

它表示：

- 当某一边 maker 买单整单成交以后
- 程序准备对另一边发起 reactive taker buy
- 会先取实时盘口里的 `best ask`
- 然后在 `best ask` 基础上额外加几个 `tick` 作为滑点容忍

例如：

- 实时 `best ask = 0.51`
- 该 token 的 `tick_size = 0.01`
- `reactive_buy_slippage_ticks = 2`
- 那么最终提交的 taker buy `limit_price = 0.53`

你可以这样理解这个参数：

- `0`：几乎不追价，最省，但最容易因为卖单瞬间撤走而 FAK 失败
- `1-2`：通常是比较实用的范围，愿意小幅追价换成交率
- `3`：更激进，适合盘口薄、撤单快的场景

当前实现里：

- 优先使用 `market ws` 推送的实时 `best ask`
- 如果实时盘口暂时不可用，再回退到 REST 盘口估算
- maker 成交通知优先走 `user ws`，REST 轮询仍作为兜底

### `paper_extra_shares`

这个参数只在 `pre_open_dual_buy_paper_tpsl` 模式下生效。

它表示：

- 当某一边盘前 maker 买单整单成交 `N` 份以后
- paper taker 不只是去买相反方向的 `N` 份
- 而是去买 `N + paper_extra_shares` 份

例如：

- maker 成交 `5` 份 YES
- `paper_extra_shares = 10`
- 那么 paper taker 会模拟买入 `15` 份 NO

这 `15` 份里：

- `5` 份会和原先的 `5` 份 YES 形成完整仓位，拿到结算
- 额外 `10` 份才是主动仓位，用于后续 TP / SL 模拟

### `paper_stop_loss_price`

这个参数只在 `pre_open_dual_buy_paper_tpsl` 模式下生效。

它表示额外主动仓位的统一止损价。

例如：

- `paper_stop_loss_price = 0.50`
- 那么当实时 `best bid` 跌到 `0.50` 或以下时
- 模拟盘会把剩余的额外仓位按 taker 市价卖出

注意：

- 这个止损是“绝对价格”
- 不是相对买入价的百分比回撤

### `paper_take_profit_percents`

这个参数只在 `pre_open_dual_buy_paper_tpsl` 模式下生效。

它不是“分批止盈”，而是“并行多组模拟分支”。

例如：

- `paper_take_profit_percents = [5, 10, 15, 20]`

表示程序会同时维护 4 套独立的 paper 分支：

- TP `+5%`
- TP `+10%`
- TP `+15%`
- TP `+20%`

每套分支都是：

- 当价格达到对应涨幅
- 就把那套分支里的额外仓位一次性全部卖掉

也就是说：

- 不是一笔交易里卖 4 次
- 而是同一次入场，同时测试 4 组 TP 参数

### `paper_fee_rebate_rate`

这个参数只在 `pre_open_dual_buy_paper_tpsl` 模式下生效。

它表示第二套模拟净值口径里，手续费返还比例是多少。

例如：

- `0.30` 表示返还 `30%` 手续费

程序会同时输出两套结果：

- 正常手续费净值
- 含手续费返还后的净值

### `paper_output_dir`

这个参数只在 `pre_open_dual_buy_paper_tpsl` 模式下生效。

它表示 paper 模拟结果的输出目录。

程序启动后会在这个目录下再创建一个按启动时间命名的 session 子目录，里面通常会有这些文件：

- `paper_events.jsonl`：逐笔事件日志，方便回看和复盘
- `paper_branch_summaries.csv`：每场、每个 TP 分支的汇总结果
- `paper_pnl_normal.svg`：正常手续费口径的累计净值图
- `paper_pnl_rebate.svg`：含手续费返还口径的累计净值图

## `[execution]`

### `max_batch_orders`

单次批量下单允许的最大订单数。当前 Polymarket 上限是 `15`。由于当前每轮只挂 YES 和 NO 两张单，所以默认 `15` 很宽裕。

### `clob_execution_mode`

控制 CLOB 下单走哪条实现路径：

- `rust`：当前项目内置的 Rust 签名和下单实现
- `python_helper`：调用 `scripts/clob_helper.py`

一般建议：

- 能稳定跑就优先 `rust`
- 如果你遇到代理钱包兼容性问题，再切 `python_helper`

### `clob_helper_python_bin`

只有 `python_helper` 模式才用得上，指定 Python 解释器路径。

### `clob_helper_script`

只有 `python_helper` 模式才用得上，指定 helper 脚本路径。

### `onchain_execution_mode`

控制链上动作走哪条路径：

- `rpc`：本地私钥直发链上交易，适合 EOA
- `relayer`：走 Polymarket relayer，适合代理钱包 / SAFE

这个参数影响：

- `splitPosition`
- `mergePositions`
- `redeemPositions`

### `refresh_metadata_on_start`

启动时是否去刷新市场元数据，例如 tick size、fee rate、neg risk。

建议保持 `true`，除非你确定自己要用本地静态值兜底。

### `auto_approve_ctf`

是否自动设置 CTF operator 授权。如果没授权，split 后也可能无法后续操作。通常建议保持开启。

### `auto_approve_collateral`

是否自动设置抵押品 allowance。对需要 `split` 的模式很重要，因为没 allowance 就没法把 USDC 拆成 YES/NO。

### `relayer_require_safe_deployed`

在 `relayer` 模式下，是否强制要求 SAFE 已部署。

保持 `true` 的好处是：

- 启动时就能尽早发现账户环境不完整
- 避免你以为机器人会跑，实际到了 split 才发现 SAFE 根本没准备好

### `relayer_poll_interval_ms`

轮询 relayer 交易状态的间隔，单位毫秒。它同时也会影响后台 redeem watcher 的轮询频率。

### `tick_size`

当 `refresh_metadata_on_start = false` 时，本地兜底使用的最小跳价单位。默认 `0.01`，也就是一分钱一档。

### `neg_risk`

当 `refresh_metadata_on_start = false` 时，本地兜底使用的 `neg_risk` 标记。大多数情况下不建议手动改，除非你非常清楚自己在处理哪类市场。

### `fee_rate_bps`

当 `refresh_metadata_on_start = false` 时，本地兜底使用的 fee rate，单位是 bps。一般也建议让程序自动拉取，不要手填。

### `settled_redeem_scan_enabled`

是否开启后台补扫已结算仓位。

这个功能的意义很大：

- 主流程会在每轮结束后尝试自动 redeem
- 但如果那一刻网络异常、接口超时、服务重启，可能漏掉
- 开启后台补扫后，程序会周期性去捡这些漏网仓位

建议生产环境保持 `true`。

### `settled_redeem_scan_interval_secs`

后台补扫执行频率，单位秒。值越小，补救越快，但请求频率也越高。

### `settled_redeem_scan_lookback_secs`

后台补扫最多回看多久内的已结算轮次。值太小可能漏掉重启期间的旧仓位，值太大则会多扫一些无关轮次。

## `[routing]`

### `primary_account`

默认主账号名，必须对应某个 `[[accounts]].name`。当前版本虽然保留了多账号结构，但实际执行仍然是“以主账号为准”的单账号模式。

## `[telemetry]`

### `log_json`

是否输出 JSON 日志。

- `true`：适合服务器、`journalctl`、日志平台
- `false`：适合本地终端手看

## `[telegram]`

### `enabled`

是否启用 Telegram 通知。关闭后，机器人不会发送启动、退出和错误消息。

### `bot_token_env`

Telegram bot token 所在的环境变量名。

### `chat_ids`

通知目标 chat id 列表。启用 Telegram 时必须至少填一个。

### `send_startup`

是否在进程启动时发送通知。

### `send_shutdown`

是否在进程退出时发送通知。

### `send_errors`

是否在主流程报错时发送通知。这个建议保留开启，因为未来两轮策略更依赖时间点，错误越早看到越好。

### `disable_link_preview`

是否关闭 Telegram 链接预览。通常设为 `true` 更干净。

### `parse_mode`

消息格式化模式。留空表示纯文本，如果以后要发 Markdown 或 HTML，再按 Telegram 规范改。

## `[[accounts]]`

### `name`

账号名称。它本身不影响资金，只是作为配置里的引用名。

### `enabled`

该账号是否启用。`routing.primary_account` 指向的账号必须是启用状态。

### `chain_id`

链 ID。Polygon 主网是 `137`。

### `signature_type`

签名类型。当前支持：

- `eoa`
- `magic`
- `browser_proxy`
- `gnosis_safe`

建议这样区分：

- `browser_proxy`：Polymarket 普通 proxy wallet / browser wallet
- `gnosis_safe`：Polymarket SAFE / relayer 账户，通常表现为 `signer_address` 和 `funder_address` 不同

### `funder_address`

真正持有资金和仓位的地址。

常见理解方式：

- `signer_address`：负责签名
- `funder_address`：真正持仓的钱包

EOA 场景下通常两者相同，所以这里可以留空。代理钱包场景下则要认真填。

### `private_key_env`

私钥所在环境变量名。程序会从环境里取值，不会直接从 TOML 里读私钥明文。

### `api_key_env`

CLOB API key 所在环境变量名。

### `api_secret_env`

CLOB API secret 所在环境变量名。

### `api_passphrase_env`

CLOB API passphrase 所在环境变量名。

### `relayer_api_key_env`

relayer API key 所在环境变量名。只有 `onchain_execution_mode = "relayer"` 时才需要。

### `relayer_api_key_address_env`

relayer API key owner address 所在环境变量名。这个地址必须和 relayer key 的拥有者一致，否则 relayer 交易会失败。

## 当前模式下的关键时间线

以 `btc-5m`、`quote_start_before_open_secs = 180`、`quote_cancel_before_open_ms = 1000`、`pre_split_before_open_secs = 240` 为例：

- `10:20:35` 启动机器人时，当前 `10:20-10:25` 这一轮已开盘，所以直接跳过。
- 机器人会只接管 `10:25-10:30` 和 `10:30-10:35` 这两轮。
- 如果模式是 `pre_split_dual_sell`，那么 `10:25-10:30` 这轮会在 `10:21:00` 左右开始 split。
- 这轮会在 `10:22:00` 左右开始挂单。
- 如果到了 `10:24:59` 仍有未成交部分，就撤掉所有未成交部分。
- 之后窗口里保留 `10:30-10:35`，并补进 `10:35-10:40`。

这就是当前代码实现的“滚动未来两轮”语义。
