# FuturesAI — 中国商品期货ai自动交易系统

> 将大语言模型（LLM）接入中国商品期货市场，实现从行情订阅、AI 分析到自动下单的全链路自主交易；同时回测系统支持 AI 策略的历史验证。

---

## 项目简介

本项目是一套**可替换策略架构**的量化交易系统，核心创新点在于用 LLM 替代传统规则引擎，让 AI 直接阅读 K 线特征并输出结构化交易信号。默认内置基于价格行为学的策略实现。

系统支持同时分析多个商品期货合约，AI 每根 K 线收盘后自动触发分析，输出开仓/持仓/平仓/反手决策，并通过追踪止损守卫管理风险。配套回测系统支持逐根 K 线重建市场上下文、调用真实 LLM 决策并模拟追踪止损出场，实现 AI 策略的历史验证。

---

## 系统架构

**实时交易路径**

```
tqsdk 线程                         asyncio 事件循环
──────────────────────────────     ──────────────────────────────
行情订阅（250根K线）
K线收盘检测 ─────────────────────→ 多合约 x 多模型并发分析
tick守卫轮询（SL / TP / EOD）                  │
       ↑                              结构化 JSON 信号
       │                                      │
insert_order ←──── 下单队列 ←─────────────────┘
       │
       ↓
FastAPI + SSE ──→ 浏览器前端（实时面板 / 权益曲线）
```

**回测路径（独立）**

```
Parquet历史K线
     │
     ↓
逐根重建 market_data ──→ 策略层特征计算 ──→ 真实LLM分析 ──→ 追踪止损模拟
                                                                   │
/backtest/* API ──→ backtest.html（K线图可视化）        统计汇总（胜率/盈亏R）
```

### 核心模块

| 模块 | 文件 | 职责 |
|------|------|------|
| 后端服务 | `main.py` | FastAPI 应用，状态管理，SSE 实时推送 |
| 配置管理 | `config.py` | 环境变量读取，交易时间判断 |
| 行情订阅 | `data_feed.py` | TqSdk 行情线程，K线订阅，守卫轮询 |
| AI 分析 | `analyzers.py` | 多模型调用，Prompt 构建，JSON 解析 |
| 下单执行 | `trader.py` | 开平仓，线程安全队列，守卫持久化 |
| 策略层 | `strategies/` | 价格行为特征计算，System Prompt 管理 |
| 回测引擎 | `backtest/` | 历史 K 线加载，追踪止损模拟，统计计算 |

---

## 技术栈

**后端**
- Python 3.14
- FastAPI + uvicorn（异步 Web 框架）
- TqSdk（天勤量化，期货行情与交易接口）
- asyncio + 多线程（tqsdk 线程与 asyncio 事件循环隔离）

**AI 层**
- Anthropic SDK（Claude 系列）
- OpenAI SDK（兼容接口，接入 DeepSeek / Gemini / Grok / Qwen3 等）
- 流式输出 + 信号量并发控制（默认 3 并发）
- 动态 System Prompt（有仓/无仓自动切换，token 节省约 62%）

**前端**
- 纯 HTML + Vanilla JS（无构建步骤）
- SSE 实时推送（分析结果、持仓变化、权益曲线）
- Lightweight Charts（回测 K 线图表）

**数据**
- Parquet（历史 K 线存储）
- JSONL（交易记录按日分区）
- JSON（守卫持久化，支持重启恢复）

---

## 主要功能

### 实时交易
- **自定义 K 线周期**：支持任意分钟级周期（默认 5 分钟）
- **多合约并发分析**：同时监控 10 个合约，每根 K 线收盘触发 AI 分析
- **结构化信号输出**：AI 输出 JSON 格式信号（操作建议 / 入场价 / 止损价 / 止盈价）
- **追踪止损守卫**：tick 级别轮询，分段追踪策略（0.6R / 1R / 2R 三档）
- **止盈感知收紧**：接近止盈时自动压缩追踪距离
- **尾盘强制平仓**：收盘前 2 分钟独立检测，tick 级别执行
- **企业微信推送**：开仓/平仓实时通知（可选；无法使用天勤自动下单时，可通过推送人工跟单）

### AI 决策特性
- **双语 Prompt**：中文模型用中文 Prompt，英文模型用英文 Prompt，token 效率提升 ~18%
- **预计算特征块**：EMA 缺口、趋势强度、H2/L2 回调腿、区间边界等提前计算，AI 直接采用
- **历史折叠展示**：连续相同操作折叠显示，减少 token 消耗
- **早退信号流式捕获**：流式输出中检测信号，提前触发下单

### 回测系统

> 传统回测用固定规则跑历史数据；本系统的回测是 **AI 决策的历史重演**——逐根 K 线重建完整市场上下文，调用真实 LLM 分析，追踪止损模拟与实盘代码路径完全共享，确保回测与实盘行为一致。

- **逐根重建上下文**：每根 K 线调用 `build_market_data_at()`，完整还原 EMA、趋势结构、H2/L2 标签等预计算特征
- **真实 AI 调用**：回测直接调用线上 LLM，结果反映模型的真实决策能力，而非规则近似
- **追踪止损模拟**：K 线级分段追踪（0.6R / 1R / 2R 三档）+ 止盈感知收紧，分段逻辑与实盘一致；但实盘为 tick 级执行，回测存在 K 线内滑点误差
- **可视化交互**：Lightweight Charts K 线图点击任意 Bar → 查看预计算面板 → 触发 AI 分析 → 模拟出场
- **统计汇总**：胜率 / 平均盈亏 R / 盈亏比 / 期望值
- **内置历史数据**：仓库已附带部分商品期货 K 线数据（`data/klines/`），克隆后可直接运行回测；回测页面支持在线下载更多品种的历史数据，无需手动执行脚本

---

## 支持的 AI 模型

| 模型 | 接口 | 语言 |
|------|------|------|
| Claude (Anthropic) | 原生 SDK | 英文 |
| GPT-4o (OpenAI) | OpenAI SDK | 英文 |
| DeepSeek | OpenAI 兼容 | 中文 |
| Gemini | OpenAI 兼容 | 英文 |
| Grok | OpenAI 兼容 | 英文 |
| Qwen3 | OpenAI 兼容 | 中文 |
| OpenRouter | OpenAI 兼容 | 英文 |

新增模型只需在 `config.py` 的 `MODEL_DEFINITIONS` 中添加一条配置，无需修改其他代码。

---

## 快速开始

### 环境要求
- Python 3.14（Windows 64位）
- 天勤量化账号（[tqsdk.com](https://www.shinnytech.com/products/tqsdk)，[支持的期货公司列表](https://www.shinnytech.com/articles/reference/tqsdk-brokers)）
- 至少一个 AI 模型的 API Key

### 安装

```bash
git clone https://github.com/tauadjm/FUTURES-AI.git
cd FUTURES-AI

py -3.14 -m venv .venv
.venv\Scripts\activate

pip install -r requirements.txt
```

### 配置

```bash
copy .env.example .env
# 编辑 .env，填入天勤账号和 AI API Key
```

### 启动

```bash
python main.py
# 访问 http://localhost:8888
```

---

## 目录结构

```
├── main.py               # FastAPI 后端
├── config.py             # 配置管理
├── data_feed.py          # 行情订阅 & 守卫轮询
├── analyzers.py          # AI 模型调用
├── trader.py             # 下单执行
├── strategies/
│   ├── __init__.py           # 策略注册表
│   ├── prompts.py            # System Prompt 字符串（中英文双语，开源）
│   └── *.cp314-win_amd64.pyd # 策略模块（编译发布，不开源）
├── backtest/
│   ├── engine.py         # 回测引擎
│   └── router.py         # 回测 API 路由
├── index.html            # 主界面（SSE 实时更新）
├── equity.html           # 权益分析页
├── backtest.html         # 回测界面
├── analyze_trades.py     # 交易统计报告
├── calc_fee.py           # 手续费计算
├── calc_trail.py         # 追踪止损推算器
├── download_klines.py    # 历史 K 线下载
├── data/klines/          # 历史 K 线数据（Parquet，供回测用）
└── requirements.txt
```

---

## 策略插件化

系统采用插件化策略架构，通过抽象基类定义统一接口，新策略只需继承并实现对应方法，在 `.env` 中切换 `STRATEGY_ID` 即可生效，无需改动核心代码。

> 策略接口（`BaseStrategy`）与默认实现均以编译形式发布，源码不公开。

---

## 风险提示

> 本项目仅供技术学习和研究使用。期货交易有较大风险，AI 决策存在不确定性，请勿将本系统直接用于实盘交易。作者不承担任何因使用本系统产生的经济损失。

---

## License

MIT License — 详见 [LICENSE](LICENSE)
