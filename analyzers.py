"""
analyzers.py — AI 层

职责：
1. 对外只暴露一个异步函数 analyze(model_id, market_data, history) → dict
2. 内部根据 model_id 自动选择 Anthropic SDK 或 OpenAI 兼容 SDK
3. 构建 System Prompt 和 User Message，调用 AI，解析 JSON 结果
4. 处理所有异常（网络超时、Key 无效、JSON 解析失败），不向上抛出

支持模型：claude / openai / deepseek / grok / qwen / gemini
"""

import re
import json
import logging
import asyncio
import pathlib
from datetime import datetime
from typing import Any

import config
from strategies import get_strategy as _get_strategy

logger = logging.getLogger(__name__)

# AI 调用超时时间（秒）
AI_TIMEOUT = 180   # 3分钟上限；涵盖连接建立 + 完整流式输出

# ── 早退开仓检测正则（在流式文本中查找关键字段）──────────────────
_EARLY_ACTION_RE = re.compile(r'"操作建议"\s*:\s*"([^"]+)"')
_EARLY_ENTRY_RE  = re.compile(r'"入场价"\s*:\s*([\d.]+)')
_EARLY_SL_RE     = re.compile(r'"止损价"\s*:\s*(null|[\d.]+)(?=[^0-9.])')
_EARLY_TP_RE     = re.compile(r'"止盈价"\s*:\s*(null|[\d.]+)(?=[^0-9.])')

# EN 版（Claude / Grok / Gemini / OpenAI）
_EN_EARLY_ACTION_RE = re.compile(r'"action"\s*:\s*"([^"]+)"')
_EN_EARLY_ENTRY_RE  = re.compile(r'"entry_price"\s*:\s*([\d.]+)')
_EN_EARLY_SL_RE     = re.compile(r'"stop_loss"\s*:\s*(null|[\d.]+)(?=[^0-9.])')
_EN_EARLY_TP_RE     = re.compile(r'"take_profit"\s*:\s*(null|[\d.]+)(?=[^0-9.])')

# ── _build_user_message 模块级常量（避免每次调用重建）──────────────
_SKIP_FIELDS: frozenset[str] = frozenset({
    "pending", "error", "is_trading_time",
    "is_session_first_bar", "is_session_last_bar", "product_name",
    "_struct_calc_time", "_fetch_exec_start", "_fetch_exec_end", "_fetch_submit_time",
})
_SKIP_STRUCT: frozenset[str] = _get_strategy().get_skip_struct()
_SKIP_ALL: frozenset[str] = _SKIP_FIELDS | _SKIP_STRUCT
_KLINE_KEEP: tuple[str, ...] = (
    "time", "open", "high", "low", "close", "body_ratio",
)

# ── EN→ZH 翻译层（仅 EN 模型输出需要）────────────────────────────────

# 1. JSON key 翻译（EN output key → 系统内部 ZH key）
_EN2ZH_KEYS: dict[str, str] = {
    "action":             "操作建议",
    "entry_price":        "入场价",
    "stop_loss":          "止损价",
    "take_profit":        "止盈价",
    "reverse_direction":  "反手方向",
    "stop_basis":         "止损依据",
    "market_state":       "市场状态",
    "setup_type":         "设置类型",
    "logic":              "核心逻辑",
    "technicals":         "技术面",
    "risk_note":          "风险提示",
    "risk_level":         "风险等级",
}

# 2. action 枚举翻译
_EN2ZH_ACTIONS: dict[str, str] = {
    "long":    "做多",
    "short":   "做空",
    "wait":    "观望",
    "hold":    "持有",
    "close":   "平仓",
    "reverse": "反手",
}

# 3. 其余枚举值翻译（market_state / setup_type / stop_basis / risk_level / reverse_direction）
_EN2ZH_VALUES: dict[str, str] = {
    # market_state
    "strong_uptrend":   "强上升趋势",
    "weak_uptrend":     "弱上升趋势",
    "trading_range":    "交易区间",
    "weak_downtrend":   "弱下降趋势",
    "strong_downtrend": "强下降趋势",
    # setup_type（H/L类值中英文统一为"H1"/"H2"等，无需翻译）
    "breakout_pullback":          "突破回撤",
    "failed_false_breakout":      "失败假突破",
    "failed_H2":                  "失败的H2",
    "failed_L2":                  "失败的L2",
    "channel_overshoot_reversal": "通道线过冲反转",
    "range_edge_reversal":        "区间边沿反转",
    "range_double_bottom":        "区间双底",
    "range_double_top":           "区间双顶",
    "range_scalp":                "区间剥头皮",
    "wedge_reversal":             "楔形反转",
    "no_valid_setup":             "无有效设置",
    # stop_basis
    "signal_bar_stop":  "信号棒止损",
    "ema_stop":         "EMA止损",
    "price_level_stop": "价位止损",
    "structure_stop":   "信号棒止损",
    # risk_level
    "low":    "低",
    "medium": "中",
    "high":   "高",
    # reverse_direction（复用 action 的 long/short）
    # （已在 _EN2ZH_ACTIONS 中，合并映射时自动覆盖）
}

# 合并映射：action + 其余枚举值，供统一查表
_EN2ZH_ALL: dict[str, str] = {**_EN2ZH_ACTIONS, **_EN2ZH_VALUES}

# ZH→EN 反向映射（EN 模式历史记录展示用）
_ZH2EN_ALL: dict[str, str] = {v: k for k, v in _EN2ZH_ALL.items()}
_ZH2EN_ALL.update({
    # 守卫/系统触发动作
    "守卫止盈":     "guard_TP",
    "守卫止损":     "guard_SL",
    "跟进不足退出": "early_exit",
    "尾盘强制平仓": "EOD_close",
    # 信号棒评级
    "强多头棒": "strong_bull",
    "中多头棒": "med_bull",
    "弱多头棒": "weak_bull",
    "强空头棒": "strong_bear",
    "中空头棒": "med_bear",
    "弱空头棒": "weak_bear",
})

# ── EN 数据翻译映射（仅在 lang="en" 时对 features/quote 键名生效） ──
_QUOTE_ZH2EN: dict[str, str] = {
    "最新价":          "last_price",
    "今开":            "open",
    "最高":            "high",
    "最低":            "low",
    "昨收":            "prev_close",
    "涨跌额":          "change",
    "涨跌幅%":         "change_pct",
    "当日累计成交量手": "volume",
    "持仓量":          "open_interest",
    "涨停价":          "limit_up",
    "跌停价":          "limit_down",
    "是否涨停":        "is_limit_up",
    "是否跌停":        "is_limit_down",
}

# ── _call_openai_compatible 模块级常量 ──────────────────────────
_OAI_MAX_TOKENS: dict[str, int] = {
    "deepseek":   8192,
    "openai":     16384,
    "grok":       131072,
    "qwen3":      8192,
    "gemini":     65536,
    "openrouter": 16384,
}
_OAI_TEMPERATURE: dict[str, float] = {
    "qwen3":    0.1,
}
_OAI_EXTRA_BODY: dict[str, dict] = {
    "qwen3": {"enable_thinking": False},
}

# ── System Prompt（由活跃策略提供，保留模块级名称供 main.py 兼容）────────────
_s = _get_strategy()
DEFAULT_SYSTEM_PROMPT    = _s.get_default_system_prompt("zh")
HOLDING_SYSTEM_PROMPT    = _s.get_holding_system_prompt("zh")
QWEN3_SYSTEM_PROMPT      = _s.get_merged_system_prompt("zh")
EN_DEFAULT_SYSTEM_PROMPT = _s.get_default_system_prompt("en")
EN_HOLDING_SYSTEM_PROMPT = _s.get_holding_system_prompt("en")

# ── 要求 AI 返回的字段（用于校验完整性）────────────────────────
REQUIRED_FIELDS = [
    "市场状态", "设置类型",
    "操作建议", "入场价", "止损价", "止盈价",
    "核心逻辑", "技术面", "风险提示", "风险等级",
]
# 仅做多/做空/反手时需要止损依据
ENTRY_ACTIONS = {"做多", "做空", "反手"}

VALID_ACTIONS    = {"做多", "做空", "观望", "持有", "平仓", "反手"}
VALID_RISK_LEVELS = {"低", "中", "高"}

_FOLD_ACTIONS: frozenset[str] = frozenset({"持有", "观望"})
_NO_SETUP:    frozenset[str] = frozenset({"", "无有效设置"})


def _is_en_model(model_id: str) -> bool:
    """返回 True 表示该模型使用英文 Prompt（lang="en"）"""
    return config.MODEL_DEFINITIONS.get(model_id, {}).get("lang", "zh") == "en"


# ── User Message 国际化标签 ──────────────────────────────────────
_I18N: dict[str, dict[str, str]] = {
    "zh": {
        "hist_header":   "【你的历史分析记录（原{n}条，折叠后{f}条）】",
        "hist_reflect":  "【历史反思】请对比你之前的分析：",
        "hist_reflect1": "1. 如果之前判断有误（如止损被打掉），请反思原因并调整本次判断",
        "hist_reflect2": "2. 如果之前判断正确，请确认当前形态是否延续",
        "hist_none":     "【历史分析记录】暂无历史记录，这是本次会话的第一次分析。\n",
        "product_hdr":   "【当前分析品种】{name}（合约代码：{sym}）",
        "pos_hdr_has":   "【当前持仓 —— 有持仓，请严格按照有持仓决策规则输出】",
        "pos_hdr_none":  "【当前持仓 —— 无持仓，如看多则做多，看空则做空，无优势则观望】",
        "pos_warn":      "⚠️ 当前已有持仓，操作建议只能是：持有 / 平仓 / 反手，禁止输出做多或做空。",
        "entry_ctx":     "入场结构:{mkt}/{setup}",
        "be_exit_hdr":   "⚠️【保本震出提示】",
        "cool_exit_hdr": "⛔【冷却期警告】",
        "cool_exit_msg": "{n}分钟前刚发生【{t}】，当前无持仓为守卫自动止出结果。",
        "exit_logic":    "   出场逻辑：{logic}",
        "be_advice":     "   ✅ 这是保本止损（Al Brooks 保本陷阱场景）：若当前形态和趋势仍然有效，可以立即在合理支撑/压力位重新入场，无需等待回调。若形态已破坏、趋势不明朗或当前处于窄幅区间，则输出\"观望\"。",
        "cool_advice":   "   ⚠️ 禁止立即在同一价位附近追入！必须等待价格出现明显回调（至少1根强方向K线确认）后，在合理支撑/压力位形成新的高概率设置，才可重新入场。当前若无明确回调确认，操作建议必须输出\"观望\"。",
        "data_hdr":      f"【当前行情数据（{config.ANALYSIS_INTERVAL_MINUTES}分钟K线，最近{config.AI_KLINE_COUNT}根；远端结构已由预计算特征承载）】",
        "data_hdr_nt":   f"【当前行情数据（{config.ANALYSIS_INTERVAL_MINUTES}分钟K线，最近{config.AI_KLINE_COUNT}根；非交易时间收盘状态）】",
        "nontrading_1":  "注意：当前处于非交易时间，行情数据为最近一个交易日的收盘状态。",
        "nontrading_2":  "请基于收盘数据进行技术分析，操作建议供下一交易时段参考。",
        "feat_hdr":      "【预计算价格行为特征】",
        "final_instr":   "请根据以上数据进行分析，严格按照要求的 JSON 格式输出结果。",
        "lbl_action":    "建议",
        "lbl_entry":     "入场",
        "lbl_tp":        "止盈",
        "lbl_sl":        "止损",
        "lbl_logic":     "逻辑",
        "lbl_signal":    "信号",
        "lbl_tech":      "技术",
        "pos_avg":       "均价",
        "pos_cur":       "当前价",
        "pos_pnl":       "浮盈",
        "pos_sl":        "止损",
        "pos_tp":        "止盈",
        "pos_away":      "距{d}%",
        "pos_unset":     "未设",
        "pos_lots":      "手",
    },
    "en": {
        "hist_header":   "[Your Analysis History (original {n} entries, folded to {f})]",
        "hist_reflect":  "[History Review] Compare with your prior analysis:",
        "hist_reflect1": "1. If prior analysis was wrong (e.g., stopped out), reflect on why and adjust",
        "hist_reflect2": "2. If prior analysis was correct, confirm whether the pattern continues",
        "hist_none":     "[Analysis History] No history yet — this is the first analysis of this session.\n",
        "product_hdr":   "[Current Instrument] {name} (symbol: {sym})",
        "pos_hdr_has":   "[Current Position — HOLDING: follow position-management rules strictly]",
        "pos_hdr_none":  "[Current Position — FLAT: go long if bullish, short if bearish, wait if no edge]",
        "pos_warn":      "⚠️ Position is open — output must be one of: hold / close / reverse. long and short are forbidden.",
        "entry_ctx":     "entry setup: {mkt}/{setup}",
        "be_exit_hdr":   "⚠️[Breakeven-stop notice]",
        "cool_exit_hdr": "⛔[Cooldown warning]",
        "cool_exit_msg": "{n}m ago: [{t}] triggered — currently flat due to auto guard exit.",
        "exit_logic":    "   Exit reason: {logic}",
        "be_advice":     "   ✅ Breakeven stop (Al Brooks breakeven trap): if current pattern and trend remain valid, re-enter immediately at a reasonable support/resistance level without waiting for a pullback. If pattern has broken down, trend is unclear, or market is in a narrow range, output wait.",
        "cool_advice":   "   ⚠️ Do NOT re-enter immediately at the same price! Wait for a clear pullback (at least one strong directional bar), then identify a new high-probability setup before re-entering. Without clear pullback confirmation, output must be wait.",
        "data_hdr":      f"[Market Data — {config.ANALYSIS_INTERVAL_MINUTES}-min bars, last {config.AI_KLINE_COUNT} completed; historical structure is in the pre-calculated features block]",
        "data_hdr_nt":   f"[Market Data — {config.ANALYSIS_INTERVAL_MINUTES}-min bars, last {config.AI_KLINE_COUNT}; non-trading hours / close state]",
        "nontrading_1":  "Note: non-trading hours — data reflects the most recent session close.",
        "nontrading_2":  "Perform technical analysis based on close data; recommendation applies to the next session.",
        "feat_hdr":      "[Pre-calculated Price Action Features]",
        "final_instr":   "Analyze the data above and output your result in the required JSON format.",
        "lbl_action":    "action",
        "lbl_entry":     "entry",
        "lbl_tp":        "tp",
        "lbl_sl":        "sl",
        "lbl_logic":     "logic",
        "lbl_signal":    "signal",
        "lbl_tech":      "tech",
        "pos_avg":       "avg",
        "pos_cur":       "cur",
        "pos_pnl":       "pnl",
        "pos_sl":        "sl",
        "pos_tp":        "tp",
        "pos_away":      "{d}% away",
        "pos_unset":     "unset",
        "pos_lots":      "lots",
    },
}



# ─────────────────────────────────────────────────────────────
# Prompt 构建
# ─────────────────────────────────────────────────────────────

def _build_user_message(market_data: dict, history: list[dict], positions: list[dict] | None = None, lang: str = "zh") -> str:
    """
    构建每次分析的 User Message，分两部分拼接：
    1. 该模型自己的历史分析记录（最近 10 条）
    2. 当前实时行情数据（完整 JSON）

    lang: "zh" 使用中文标签（DeepSeek/Qwen3），"en" 使用英文标签（Claude/OpenAI/Grok/Gemini）
    注意：JSON 字段名和枚举值始终为中文（解析代码依赖），仅界面标签随 lang 切换。
    """
    _T = _I18N[lang if lang in _I18N else "zh"]  # 标签字典
    parts = []

    # ── 第一部分：历史分析记录 ──────────────────────────────
    if history:
        # 过滤时间差距过大的历史记录（跨交易时段的旧记录无参考价值）
        _MAX_HISTORY_GAP_SEC = 180 * 60  # 3小时：覆盖午休(≤2h)，过滤跨时段(≥6h)
        _cur_ts_str = market_data.get("timestamp", "")
        if _cur_ts_str:
            try:
                _cur_ts = datetime.strptime(_cur_ts_str[:16], "%Y-%m-%d %H:%M")
                _filtered = []
                for _r in history:
                    _t = _r.get("time", "")
                    if not _t:
                        _filtered.append(_r)
                        continue
                    try:
                        _rt = datetime.strptime(str(_t)[:16], "%Y-%m-%d %H:%M")
                        if (_cur_ts - _rt).total_seconds() <= _MAX_HISTORY_GAP_SEC:
                            _filtered.append(_r)
                    except ValueError:
                        _filtered.append(_r)
                history = _filtered
            except ValueError:
                pass  # 解析失败则保留全部历史

        # 折叠连续相同操作（持有/观望），压缩重复记录
        # 当设置类型从"无有效设置"/空 变为具体形态，或形态本身改变时，不折叠（保留形态变化）
        folded: list[dict] = []
        for record in history:
            action = record.get('操作建议', '')
            curr_setup = record.get('设置类型', '')
            if folded and folded[-1]['_action'] == action and action in _FOLD_ACTIONS:
                prev_setup = folded[-1]['_last'].get('设置类型', '')
                # 设置类型出现新形态或形态切换时，不折叠，单独保留
                setup_changed = (curr_setup not in _NO_SETUP) and (curr_setup != prev_setup)
                if setup_changed:
                    folded.append({'_action': action, '_count': 1, '_first': record, '_last': record})
                else:
                    folded[-1]['_count'] += 1
                    folded[-1]['_last'] = record
            else:
                folded.append({'_action': action, '_count': 1, '_first': record, '_last': record})

        parts.append(_T["hist_header"].format(n=len(history), f=len(folded)))
        for i, entry in enumerate(folded, 1):
            action = entry['_action']
            count  = entry['_count']
            rec    = entry['_first']
            last   = entry['_last']

            # 时间：单条原样，多条显示范围
            time_str   = rec.get('time', '')[:16]
            disp_action = _ZH2EN_ALL.get(action, action) if lang == "en" else action
            action_str = f"{disp_action}×{count}" if count > 1 else disp_action
            if count > 1:
                time_str = f"{time_str}~{last.get('time', '')[:16]}"

            # 价格信息取首条（入场/止盈/止损在建仓时已确定）
            ep = rec.get('入场价'); tp = rec.get('止盈价'); sl = rec.get('止损价')
            price_str = ""
            if ep: price_str += f" {_T['lbl_entry']}:{ep}"
            if tp: price_str += f" {_T['lbl_tp']}:{tp}"
            if sl: price_str += f" {_T['lbl_sl']}:{sl}"

            # 逻辑显示：关键决策保留核心逻辑全文；持有/观望改用结构化字段
            disp = last  # 折叠时取最后一条反映最新状态
            if action in ('做多', '做空', '平仓', '反手'):
                logic_str = f" {_T['lbl_logic']}:{rec.get('核心逻辑', '')}"
            else:
                mkt       = disp.get('市场状态', '')
                setup_raw = disp.get('设置类型', '')
                sig       = disp.get('信号棒评级', '')
                if lang == "en":
                    mkt   = _ZH2EN_ALL.get(mkt, mkt)
                    setup = _ZH2EN_ALL.get(setup_raw, setup_raw)
                    sig   = _ZH2EN_ALL.get(sig, sig)
                else:
                    setup = setup_raw
                logic_str = f" [{mkt}/{setup}/{_T['lbl_signal']}:{sig}]"
                # 有意义的设置类型（非空/非无效）时补充技术面描述（用原始中文值判断）
                if setup_raw and setup_raw not in _NO_SETUP:
                    tech = disp.get('技术面', '')
                    if tech:
                        logic_str += f" {_T['lbl_tech']}:{tech}"

            parts.append(
                f"{i}. [{time_str}] "
                f"{_T['lbl_action']}:{action_str} "
                f"{price_str}"
                f"{logic_str}"
            )
        parts.append("")  # 空行分隔
        parts.append(_T["hist_reflect"])
        parts.append(_T["hist_reflect1"])
        parts.append(_T["hist_reflect2"])
        parts.append("")
    else:
        parts.append(_T["hist_none"])

    # ── 第一点五：行情数据标题补充品种名 ───────────────────────
    product_name = market_data.get("product_name", "")
    if product_name:
        parts.append(_T["product_hdr"].format(name=product_name, sym=market_data.get('symbol', '')))
        parts.append("")

    # ── 第二部分：当前持仓（若有）────────────────────────────
    if positions:
        parts.append(_T["pos_hdr_has"])
        for p in positions:
            fp        = p.get("float_profit", 0)
            open_price = p.get("open_price", 0)
            direction  = p.get("direction", "")
            cur_price  = market_data.get("quote", {}).get("最新价", open_price) or open_price

            # 浮盈R值（相对初始止损的盈利倍数，需要历史止损记录才能算，这里用浮盈%近似）
            fp_pct = round((cur_price - open_price) / open_price * 100, 2) if open_price else 0
            if direction == "空头":
                fp_pct = -fp_pct

            # 从历史记录提取入场结构 + 止损止盈状态
            entry_context = ""
            if history:
                # 找最近一条"做多"或"做空"的记录（即建仓那条）
                for rec in reversed(history):
                    action = rec.get("操作建议", "")
                    if action in ("做多", "做空", "反手"):
                        setup     = rec.get("设置类型", "")
                        mkt_state = rec.get("市场状态", "")
                        if setup or mkt_state:
                            entry_context = _T["entry_ctx"].format(mkt=mkt_state, setup=setup)
                        break
                # 止损止盈状态（从最近一条历史记录读取）
                last = history[-1]
                sl = last.get("止损价"); tp = last.get("止盈价")

            parts.append(
                f"  {p['symbol']} {direction} {p.get('volume',1)}{_T['pos_lots']}  "
                f"{_T['pos_avg']}:{open_price}  {_T['pos_cur']}:{cur_price}  "
                f"{_T['pos_pnl']}:{'+' if fp>=0 else ''}{fp} ({'+' if fp_pct>=0 else ''}{fp_pct}%)"
                + (f"  [{entry_context}]" if entry_context else "")
            )

            if history and (sl or tp):
                sl_dist = round(abs(cur_price - sl) / cur_price * 100, 2) if sl else None
                tp_dist = round(abs(tp - cur_price) / cur_price * 100, 2) if tp else None
                sl_str  = f"{_T['pos_sl']}:{sl}({_T['pos_away'].format(d=sl_dist)})" if sl else f"{_T['pos_sl']}:{_T['pos_unset']}"
                tp_str  = f"{_T['pos_tp']}:{tp}({_T['pos_away'].format(d=tp_dist)})" if tp else f"{_T['pos_tp']}:{_T['pos_unset']}"
                parts.append(f"    {sl_str}  {tp_str}")

        parts.append(_T["pos_warn"])
        parts.append("")
    else:
        parts.append(_T["pos_hdr_none"])

        # ── 检测刚刚发生的守卫平仓，注入冷却期警告 ────────────────
        # 若最近一条历史记录是守卫止盈/止损，且发生在 15 分钟内，
        # 说明当前无持仓是刚被止出，需要等待回调而非立即追入。
        # 例外：保本止损（_is_breakeven=True）→ 形态有效时允许立即重入。
        _recent_guard_exit = None
        if history:
            _last = history[-1]
            _last_action = _last.get("操作建议", "")
            if _last_action in ("守卫止盈", "守卫止损", "守卫平仓"):
                try:
                    _exit_time = datetime.strptime(_last["time"][:16], "%Y-%m-%d %H:%M")
                    _now = datetime.now()
                    _mins_ago = round((_now - _exit_time).total_seconds() / 60)
                    if 0 <= _mins_ago <= 15:
                        _recent_guard_exit = (
                            _last_action, _mins_ago,
                            _last.get("核心逻辑", ""),
                            _last.get("_is_breakeven", False),
                        )
                except Exception:
                    pass

        if _recent_guard_exit:
            _exit_type, _mins, _exit_logic, _is_be = _recent_guard_exit
            _hdr = _T["be_exit_hdr"] if _is_be else _T["cool_exit_hdr"]
            parts.append(_hdr + _T["cool_exit_msg"].format(n=_mins, t=_exit_type))
            parts.append(_T["exit_logic"].format(logic=_exit_logic))
            parts.append(_T["be_advice"] if _is_be else _T["cool_advice"])
        parts.append("")

    # ── 第三部分：当前行情数据 ──────────────────────────────
    is_trading = market_data.get("is_trading_time", True)
    if is_trading:
        parts.append(_T["data_hdr"])
    else:
        parts.append(_T["data_hdr_nt"])
        parts.append(_T["nontrading_1"])
        parts.append(_T["nontrading_2"])

    # ── 预计算价格行为特征 ────────────────────────────────
    ema20 = market_data.get("ema20", {})
    klines = market_data.get("klines", [])
    # 交易时间内末根通常是正在形成的未完成 K 线，但午休/夜盘后首次分析时
    # TqSdk 还未创建新 bar，末根是刚完成的棒，需用时间判断
    if is_trading and len(klines) > 1:
        try:
            last_bar_dt = datetime.strptime(klines[-1].get("time", ""), "%Y-%m-%d %H:%M")
            # 末根超过5分钟前 → 已完成棒（session 边界），全部纳入
            klines_completed = klines if (datetime.now() - last_bar_dt).total_seconds() > 300 else klines[:-1]
        except (ValueError, TypeError):
            klines_completed = klines[:-1]
    else:
        klines_completed = klines
    quote = market_data.get("quote", {})

    # ── 预计算价格行为特征（委托策略插件构建）────────────────────
    features = _get_strategy().build_features(market_data, lang, klines_completed)
    last_bar_rating = features.pop("_last_bar_rating", "")


    parts.append(_T["feat_hdr"])
    parts.append(json.dumps(features, ensure_ascii=False, indent=2))
    parts.append("")

    # 过滤内部字段，只保留 AI 需要的数据（_SKIP_ALL 为模块级常量）
    clean_data = {k: v for k, v in market_data.items() if k not in _SKIP_ALL}

    # klines 只传最近 40 根完整 K 线
    # 远端背景信息已由预计算特征层承载（波段高低点/区间边界/昨高昨低）
    # AI 做形态识别和决策只需要最近 3~4 小时的 K 线
    # 同时去掉每根 K 线的 ema20 字段（EMA 信息已在特征块完整给出）
    #
    # 【部分K线过滤】交易时间内，_five_min_clock 整点触发后经过几秒延迟，
    # tqsdk 已推入下一根刚开始的 K 线（仅几秒数据），必须排除，只传已完成的 K 线。
    # 非交易时间无新 K 线产生，末根为收盘完整 K 线，不排除。
    if "klines" in clean_data:
        # klines_completed 已按时间感知逻辑剥离未完成 K 线（与特征块信号棒评级保持一致）
        clean_data["klines"] = [
            {k: bar[k] for k in _KLINE_KEEP if k in bar}
            for bar in klines_completed[-(config.AI_KLINE_COUNT + 1):]
        ]

    # ── EN 模式：翻译 quote 键名 ──────────────────────────────────
    if lang == "en" and "quote" in clean_data:
        clean_data["quote"] = {_QUOTE_ZH2EN.get(k, k): v for k, v in clean_data["quote"].items()}

    parts.append(json.dumps(clean_data, ensure_ascii=False, indent=2))
    parts.append("")
    parts.append(_T["final_instr"])

    _extra_ctx = _get_strategy().build_user_context(history, market_data, positions, lang)
    if _extra_ctx:
        parts.append(_extra_ctx)

    return "\n".join(parts), last_bar_rating


# ─────────────────────────────────────────────────────────────
# JSON 清理与解析
# ─────────────────────────────────────────────────────────────

def _extract_json(text: str) -> dict | None:
    """
    从 AI 返回文本中提取 JSON 对象。
    处理两种常见情况：
    1. AI 把 JSON 包裹在 ```json ... ``` 代码块中
    2. JSON 前后有多余的说明文字
    """
    if not text:
        return None

    # 先尝试直接解析（理想情况）
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        pass

    # 去掉 markdown 代码块标记后再试
    cleaned = re.sub(r"```(?:json)?\s*", "", text)
    cleaned = cleaned.replace("```", "").strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Bug7修复: 逐字符匹配括号深度，找到完整的最外层 JSON 对象（避免贪心正则把 JSON 后的说明文字包进来）
    # 同时跳过字符串区间，避免值中含 } 触发误判（如 "下跌到支撑位}"）
    def _scan_json_at(src: str, pos: int) -> dict | None:
        """从 pos 位置开始扫描，找到第一个完整的 JSON 对象并解析，失败返回 None。"""
        depth = 0
        in_string = False
        escape_next = False
        for i, ch in enumerate(src[pos:], pos):
            if escape_next:
                escape_next = False
                continue
            if ch == '\\' and in_string:
                escape_next = True
                continue
            if ch == '"':
                in_string = not in_string
                continue
            if in_string:
                continue
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(src[pos:i + 1])
                    except json.JSONDecodeError:
                        return None
        return None

    start = text.find('{')
    if start != -1:
        result = _scan_json_at(text, start)
        if result is not None:
            return result

        # Bug8修复: 外层 JSON 不完整时（如 AI 把完整响应嵌套到某字段值中导致外层 { 未闭合），
        # 遍历每个 { 位置，取第一个包含"操作建议"字段的完整 JSON 对象。
        for inner_start in range(start + 1, len(text)):
            if text[inner_start] != '{':
                continue
            candidate = _scan_json_at(text, inner_start)
            if candidate is not None and ("操作建议" in candidate or "action" in candidate):
                logger.warning("_extract_json: 外层 JSON 不完整，已从内层嵌套对象中提取结果")
                return candidate

        # Bug9修复: 流式截断修复——模型输出被中途截断（如 cursor 代理返回不完整响应）。
        # 策略：从最后一个逗号位置向前逐步截断，补 } 后尝试解析；
        # 找到第一个含 操作建议/action 的完整对象即返回（丢弃末尾不完整字段，保留已生成字段）。
        idx = text.rfind(',')
        _attempts = 0
        while idx > start and _attempts < 20:
            _attempts += 1
            try:
                candidate = json.loads(text[start:idx] + '}')
                if isinstance(candidate, dict) and ("操作建议" in candidate or "action" in candidate):
                    logger.warning("_extract_json: 已修复流式截断的 JSON（丢弃末尾不完整字段）")
                    return candidate
            except json.JSONDecodeError:
                pass
            idx = text.rfind(',', start, idx)

    return None


def _translate_en_to_zh(data: dict) -> dict:
    """将 EN 模型输出的英文 key/value 翻译为系统内部的中文 key/value。"""
    result = {}
    for k, v in data.items():
        zh_key = _EN2ZH_KEYS.get(k, k)
        if isinstance(v, str):
            v = _EN2ZH_ALL.get(v, v)
        result[zh_key] = v
    return result



def _validate_result(data: dict) -> dict:
    """
    校验 AI 返回的 JSON 是否合规，修正可预见的问题：
    - 操作建议不在合法值内 → 改为"观望"
    - 风险等级不合法 → 改为"中"
    - 止损依据不合法 → 改为"信号棒止损"
    - 止损一致性矛盾 → 打 _sl_warning 标记
    """
    # 操作建议 — Bug6修复: 先精确匹配，再做子串 fallback，避免"做多一点"误判为"做多"
    raw_action = str(data.get("操作建议") or "").strip()
    if raw_action in VALID_ACTIONS:
        data["操作建议"] = raw_action
    else:
        matched = next((a for a in VALID_ACTIONS if a in raw_action), None)
        if matched:
            logger.warning(f"操作建议含多余字符: {repr(raw_action)}，已修正为'{matched}'")
            data["操作建议"] = matched
        else:
            logger.warning(f"操作建议值非法: {repr(raw_action)}，已修正为'观望'")
            data["操作建议"] = "观望"

    # 风险等级
    if data.get("风险等级") not in VALID_RISK_LEVELS:
        data["风险等级"] = "中"

    # 止损依据 — 仅做多/做空/反手时校验；其余操作清空避免噪音
    valid_sl_basis = {"信号棒止损", "EMA止损", "价位止损"}
    action_for_basis = data.get("操作建议", "")
    if action_for_basis in ENTRY_ACTIONS:
        raw_basis = str(data.get("止损依据") or "").strip()
        if raw_basis not in valid_sl_basis:
            logger.warning(f"止损依据值非法或缺失: {repr(raw_basis)}，已修正为'信号棒止损'")
            data["止损依据"] = "信号棒止损"
    else:
        data["止损依据"] = None

    # _sl_warning 追加辅助函数（多个校验同时触发时拼接，不覆盖）
    def _append_warning(msg: str) -> None:
        existing = data.get("_sl_warning") or ""
        data["_sl_warning"] = (existing + " | " + msg) if existing else msg

    # ── 止损一致性校验 ────────────────────────────────────────────
    # 规则：若风险提示中出现具体警戒价位，而止损依据为"信号棒止损"，
    #       且止损价与该警戒价位偏差 >5 点，则判定为矛盾。
    # 处理：降置信度10分 + 打 _sl_warning 标记（不阻止下单，只警示）
    action    = data.get("操作建议", "")
    sl_price  = data.get("止损价")
    sl_basis  = data.get("止损依据", "")
    risk_text = str(data.get("风险提示") or "")

    if action in ("做多", "做空") and sl_price is not None and sl_basis == "信号棒止损":
        # 从风险提示中提取所有 3~8 位数字（期货价格范围，覆盖黄金/原油等6位价格）
        mentioned_prices = [
            float(x) for x in re.findall(r'\b\d{3,8}(?:\.\d+)?\b', risk_text)
        ]
        if mentioned_prices:
            closest_dist = min(abs(sl_price - p) for p in mentioned_prices)
            if closest_dist > 5:
                logger.warning(
                    f"[止损一致性矛盾] 风险提示价位{mentioned_prices}，"
                    f"止损依据='{sl_basis}'，止损价={sl_price}，"
                    f"最近偏差={closest_dist:.1f}点"
                )
                _append_warning(
                    f"止损逻辑矛盾：风险提示警戒价位{mentioned_prices} ≠ 止损价{sl_price}"
                    f"（止损依据：{sl_basis}）"
                )

    # ── SL/TP 方向校验与自动修正 ────────────────────────────────────────
    # 做多：SL < entry, TP > entry；做空：SL > entry, TP < entry
    # 规则：以方向正确的一方为基准，将错误的一方修正为 1:1（2×entry − 正确价）
    # 背景：早退流程下单早于 SL/TP 到达，无法阻止下单，只能事后修正守卫
    entry_price_v = data.get("入场价")
    sl_price_v    = data.get("止损价")
    tp_price_v    = data.get("止盈价")

    if action in ("做多", "做空") and entry_price_v is not None:
        try:
            ep = float(entry_price_v)
            sp = float(sl_price_v) if sl_price_v is not None else None
            tp = float(tp_price_v) if tp_price_v is not None else None

            if sp is not None and tp is not None:
                sl_ok = (action == "做多" and sp < ep) or (action == "做空" and sp > ep)
                tp_ok = (action == "做多" and tp > ep) or (action == "做空" and tp < ep)

                if sl_ok and not tp_ok:
                    # TP 方向错误 → 修正 TP 为 1:1（精度保留4位，由 trader._get_price_tick 对齐）
                    new_tp = round(2 * ep - sp, 4)
                    data["止盈价"] = new_tp
                    _append_warning(f"{action}止盈{tp}方向矛盾→已修正为{new_tp}（1:1）")
                    logger.warning(f"[TP方向矛盾] {action} 入场{ep} SL{sp} TP{tp}→修正TP{new_tp}")

                elif not sl_ok and tp_ok:
                    # SL 方向错误 → 修正 SL 为 1:1（精度保留4位，由 trader._get_price_tick 对齐）
                    new_sl = round(2 * ep - tp, 4)
                    data["止损价"] = new_sl
                    _append_warning(f"{action}止损{sp}方向矛盾→已修正为{new_sl}（1:1）")
                    logger.warning(f"[SL方向矛盾] {action} 入场{ep} SL{sp}→修正SL{new_sl} TP{tp}")

                elif not sl_ok and not tp_ok:
                    # 两者均错：清空 SL/TP 防止写入错误守卫值；早退流中的平仓已由 _on_early_guard 处理
                    data["止损价"] = None
                    data["止盈价"] = None
                    _append_warning(f"SL{sp}和TP{tp}均方向错误，已清空")
                    logger.warning(f"[SL/TP双矛盾] {action} 入场{ep} SL{sp} TP{tp}，SL/TP已清空")

            elif sp is not None:
                # 只有 SL 无 TP，仅做方向检查
                sl_ok = (action == "做多" and sp < ep) or (action == "做空" and sp > ep)
                if not sl_ok:
                    _append_warning(f"{action}止损{sp}方向矛盾（无TP无法1:1修正）")
                    logger.warning(f"[SL方向矛盾] {action} 入场{ep} SL{sp}，无TP")

        except (ValueError, TypeError):
            pass

    return data


# ─────────────────────────────────────────────────────────────
# 各模型调用实现
# ─────────────────────────────────────────────────────────────

async def _call_claude(system_prompt: str, user_message: str, on_chunk=None) -> str:
    """使用 Anthropic 官方 SDK 调用 Claude（流式，实时打印到终端）"""
    from anthropic import AsyncAnthropic
    cfg = config.MODEL_DEFINITIONS["claude"]
    chunks = []
    # Bug修复: 使用 async with 确保 httpx 连接池在调用结束后被正确关闭，避免 socket 泄漏
    async with AsyncAnthropic(api_key=cfg["api_key"]) as client:
        async with client.messages.stream(
            model=cfg["model_name"],
            max_tokens=16384,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        ) as stream:
            async for text in stream.text_stream:
                chunks.append(text)
                if on_chunk:
                    if await on_chunk("".join(chunks)):
                        on_chunk = None  # 回调返回 True 表示已触发，停止后续扫描
    full = "".join(chunks)
    logger.info(f"\n[Claude] ■ 完成（{len(full)} 字符）\n{full}")
    return full


async def _call_openai_compatible(model_id: str,
                                   system_prompt: str,
                                   user_message: str,
                                   on_chunk=None) -> str:
    """
    使用 OpenAI SDK 流式调用兼容接口（deepseek / grok / qwen / gemini 等）
    流式输出：第一个 token 到达即开始打印，不再等待完整响应，终端可实时看到内容
    """
    import os, httpx
    from openai import AsyncOpenAI
    cfg = config.MODEL_DEFINITIONS[model_id]

    proxy_url = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
    # 流式模式下 read 超时意义不同（每个chunk间隔），设宽松一些
    hx_timeout = httpx.Timeout(connect=15.0, read=120.0, write=30.0, pool=5.0)

    # Bug1修复: 使用 async with 确保 AsyncClient 连接池被正确释放
    async with (httpx.AsyncClient(proxy=proxy_url, timeout=hx_timeout) if proxy_url
                else httpx.AsyncClient(timeout=hx_timeout)) as http_client:

        client = AsyncOpenAI(
            api_key=cfg["api_key"],
            base_url=cfg.get("base_url"),
            http_client=http_client,
        )

        chunks = []
        max_tok    = _OAI_MAX_TOKENS.get(model_id, 8192)
        temp       = _OAI_TEMPERATURE.get(model_id)
        extra_body = _OAI_EXTRA_BODY.get(model_id)

        # Bug-J 修复：原代码直接 await create()，异常或 CancelledError 时
        # stream 底层 HTTP 连接不会关闭，长时间运行后连接池耗尽导致调用超时。
        # 修复：将 create() 协程与 wait_for 分离，用 async with 管理 stream 生命周期，
        # 与 Claude 路径的 async with client.messages.stream() 保持一致。
        _create_coro = client.chat.completions.create(
            model=cfg["model_name"],
            max_tokens=max_tok,
            stream=True,
            **({"temperature": temp} if temp is not None else {}),
            **({"extra_body": extra_body} if extra_body else {}),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_message},
            ],
        )
        # wait_for 只限制"拿到 stream 对象"的等待时间（首个 token 前的连接建立阶段）
        try:
            stream = await asyncio.wait_for(_create_coro, timeout=30)
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError("连接建立超时（30s 内未收到首个 token）")
        async with stream:                               # ✅ 保证任何退出路径都关闭连接
            async for chunk in stream:
                if not chunk.choices:
                    continue
                delta = chunk.choices[0].delta.content or ""
                if delta:
                    chunks.append(delta)
                    if on_chunk:
                        if await on_chunk("".join(chunks)):
                            on_chunk = None  # 回调返回 True 表示已触发，停止后续扫描
        full = "".join(chunks)
        logger.info(f"\n[{model_id}] ■ 完成（{len(full)} 字符）\n{full}")
        return full


# ─────────────────────────────────────────────────────────────
# 统一对外接口
# ─────────────────────────────────────────────────────────────

async def analyze(
    model_id:        str,
    market_data:     dict[str, Any],
    history:         list[dict],
    system_prompt:   str | None = None,
    positions:       list[dict] | None = None,
    on_early_signal  = None,   # async callable(action: str, entry_price: float) | None
    on_early_guard   = None,   # async callable(sl: float|None, tp: float|None) | None
    data_dir_override: str | None = None,  # 覆盖保存目录（如回测传 "data/backtest"）
) -> dict[str, Any]:
    """
    统一分析入口。main.py 调用此函数，无需关心底层模型差异。

    参数
    ----
    model_id      : 模型 ID，如 'claude' / 'gemini' / 'deepseek' 等
    market_data   : data_feed.get_market_data() 返回的完整行情字典
    history       : 该模型的历史分析摘要列表（最近 10 条）
    system_prompt : 自定义系统提示词；为 None 时使用默认值

    返回
    ----
    成功：包含 10 个分析字段的字典，外加 _meta 元信息
    失败：包含 error 字段的字典
    """
    result: dict[str, Any] = {
        "model_id":  model_id,
        "symbol":    market_data.get("symbol", ""),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "error":     None,
        "raw":       None,   # 调试用：AI 返回的原始文本
    }

    # ── 前置检查 ──────────────────────────────────────────
    if model_id not in config.MODEL_DEFINITIONS:
        result["error"] = f"未知模型: {model_id}"
        return result

    cfg = config.MODEL_DEFINITIONS[model_id]
    if not cfg.get("api_key"):
        result["error"] = f"模型 {model_id} 未配置 API Key"
        return result

    if market_data.get("error"):
        result["error"] = f"行情数据异常，无法分析：{market_data['error']}"
        return result

    # ── 构建 Prompt ────────────────────────────────────────
    _lang = "en" if _is_en_model(model_id) else "zh"
    _has_pos = bool(positions and any(p.get("volume", 0) for p in positions))
    if system_prompt:
        sp = system_prompt
    elif model_id == "qwen3":
        sp = QWEN3_SYSTEM_PROMPT  # 固定单一 Prompt，命中百炼自动前缀缓存
    elif _lang == "en":
        sp = EN_HOLDING_SYSTEM_PROMPT if _has_pos else EN_DEFAULT_SYSTEM_PROMPT
    else:
        sp = HOLDING_SYSTEM_PROMPT if _has_pos else DEFAULT_SYSTEM_PROMPT
    user_message, _last_bar_rating = _build_user_message(market_data, history, positions, lang=_lang)

    # ── 保存完整发送数据到本地 ────────────────────────────
    try:
        _data_dir = pathlib.Path(data_dir_override) if data_dir_override else pathlib.Path(config.get_data_root())
        # 目录结构: data/{live|sim}/market_data/<sym_safe>/<model>/YYYY-MM-DD/HH-MM-SS.json
        sym_raw = market_data.get("symbol", "unknown")
        sym_safe = re.sub(r'[^\w\-.@]', '_', sym_raw).replace('KQ.m@','').replace('KQ.d@','')
        now_str  = market_data.get("timestamp", "")  # "2025-03-07 21:05:00"
        try:
            _dt = datetime.strptime(now_str, "%Y-%m-%d %H:%M:%S")
            date_str = _dt.strftime("%Y-%m-%d")
            time_str = _dt.strftime("%H-%M-%S")
        except Exception:
            _now = datetime.now()
            date_str = _now.strftime("%Y-%m-%d")
            time_str = _now.strftime("%H-%M-%S")

        save_dir = _data_dir / "market_data" / sym_safe / model_id / date_str
        save_path = save_dir / f"{time_str}.json"

        payload = {
            "meta": {
                "symbol":   sym_raw,
                "model":    model_id,
                "datetime": f"{date_str} {time_str.replace('-',':')}",
            },
            "market_data":   market_data,
            "system_prompt": sp,
            "user_message":  user_message,
        }
        _text = json.dumps(payload, ensure_ascii=False, indent=2)

        def _do_save():
            try:
                save_dir.mkdir(parents=True, exist_ok=True)
                save_path.write_text(_text, encoding="utf-8")
            except Exception as _e:
                logger.warning(f"保存市场数据失败: {_e}")

        asyncio.ensure_future(asyncio.to_thread(_do_save))
    except Exception as _e:
        logger.warning(f"保存市场数据失败(准备阶段): {_e}")

    # ── 打印发送给 AI 的行情摘要 ──────────────────────────
    q = market_data.get("quote", {})
    ema = market_data.get("ema20", {})
    klines = market_data.get("klines", [])
    sym_short = (market_data.get("symbol") or "").replace("KQ.m@","").replace("KQ.d@","")

    # 检查数据新鲜度
    data_ts_str = market_data.get("timestamp", "")
    freshness_warning = ""
    if data_ts_str:
        try:
            data_ts = datetime.strptime(data_ts_str, "%Y-%m-%d %H:%M:%S")
            now_ts = datetime.now()
            diff_sec = (now_ts - data_ts).total_seconds()
            if diff_sec > 300:  # 超过5分钟
                freshness_warning = f"  ⚠️数据延迟{int(diff_sec/60)}分钟"
        except Exception:
            pass

    msg = (
        f"\n{'─'*60}\n"
        f"[{model_id.upper()}] 发送行情 → {sym_short}  "
        f"最新价:{q.get('最新价','--')}  "
        f"涨跌幅:{q.get('涨跌幅%','--')}%  "
        f"成交量:{q.get('成交量手','--')}手"
        f"{freshness_warning}\n"
        f"  EMA20:{ema.get('EMA20','--')}  "
        f"位置:{ema.get('价格位置','--')}  "
        f"方向:{ema.get('EMA方向','--')}  "
        f"缺口:{ema.get('EMA缺口K线数','--')}根\n"
        f"  K线:{len(klines)}根  "
        f"历史分析:{len(history)}条\n"
        + (
            "".join(
                f"  ▶ 持仓 {p['symbol']} {p['direction']} {p['volume']}手  "
                f"均价:{p['open_price']}  "
                f"浮盈:{ ('+' if p.get('float_profit',0)>=0 else '') + str(p.get('float_profit',0)) }\n"
                for p in (positions or [])
            ) or "  无持仓\n"
        )
        + f"{'─'*60}"
    )
    logger.info(msg)

    # ── 早退检测：两阶段流式扫描 ─────────────────────────────────
    # Phase-1: 发现 操作建议+入场价 → 立即下单（on_early_signal）
    # Phase-2: 继续扫描 止损价+止盈价 → 立即补充守卫（on_early_guard），然后停止扫描
    # 若无 on_early_guard，Phase-1 触发后直接停止扫描（旧行为）
    if _lang == "en":
        _re_action, _re_entry = _EN_EARLY_ACTION_RE, _EN_EARLY_ENTRY_RE
        _re_sl,     _re_tp    = _EN_EARLY_SL_RE,     _EN_EARLY_TP_RE
    else:
        _re_action, _re_entry = _EARLY_ACTION_RE, _EARLY_ENTRY_RE
        _re_sl,     _re_tp    = _EARLY_SL_RE,     _EARLY_TP_RE

    _early_fired = False
    _guard_fired = False

    async def _on_chunk(accumulated: str) -> bool:
        """两阶段扫描；返回 True 通知调用方置空回调停止后续扫描"""
        nonlocal _early_fired, _guard_fired
        # Phase-1: 下单
        if not _early_fired:
            action_m = _re_action.search(accumulated)
            entry_m  = _re_entry.search(accumulated)
            if action_m and entry_m:
                action_raw = action_m.group(1)
                # EN 模型翻译: "long"→"做多", "short"→"做空"
                action_str = _EN2ZH_ACTIONS.get(action_raw, action_raw)
                if action_str in ("做多", "做空"):
                    _early_fired = True
                    try:
                        await on_early_signal(action_str, float(entry_m.group(1)))
                    except Exception as _cb_e:
                        logger.warning(f"[{model_id}] 早退回调异常: {_cb_e}")
                    if not on_early_guard:
                        return True  # 无守卫回调时旧行为：立即停止扫描
        # Phase-2: 补充守卫
        if _early_fired and not _guard_fired and on_early_guard:
            sl_m = _re_sl.search(accumulated)
            tp_m = _re_tp.search(accumulated)
            if sl_m and tp_m:
                _guard_fired = True
                sl_v = None if sl_m.group(1) == "null" else float(sl_m.group(1))
                tp_v = None if tp_m.group(1) == "null" else float(tp_m.group(1))
                try:
                    await on_early_guard(sl_v, tp_v)
                except Exception as _cb_e:
                    logger.warning(f"[{model_id}] 早退守卫回调异常: {_cb_e}")
                return True  # 两阶段均完成，停止扫描
        return False

    # ── 调用 AI ────────────────────────────────────────────
    # on_chunk 返回 True 时流式函数将其置 None，停止后续无效 join
    _chunk_cb = _on_chunk if on_early_signal else None
    raw_text = ""
    _t0 = datetime.now()
    try:
        if model_id == "claude":
            raw_text = await asyncio.wait_for(_call_claude(sp, user_message, on_chunk=_chunk_cb), timeout=AI_TIMEOUT)
        else:
            raw_text = await asyncio.wait_for(_call_openai_compatible(model_id, sp, user_message, on_chunk=_chunk_cb), timeout=AI_TIMEOUT)

        _elapsed = (datetime.now() - _t0).total_seconds()
        result["raw"] = raw_text
        logger.info(f"[{model_id.upper()}] {sym_short} 分析完成  耗时:{_elapsed:.1f}s")

    except asyncio.TimeoutError as _te:
        _elapsed = (datetime.now() - _t0).total_seconds()
        _te_msg = str(_te) if str(_te) else f"超过 {AI_TIMEOUT} 秒"
        result["error"] = f"AI 调用超时（{_te_msg}，实际耗时 {_elapsed:.1f}s）"
        logger.error(f"analyze({model_id}): 超时  {_te_msg}  实际耗时={_elapsed:.1f}s")
        return result

    except Exception as e:
        result["error"] = f"AI 调用失败：{e}"
        logger.exception(f"analyze({model_id}): 调用异常")
        return result

    # ── 解析 JSON ──────────────────────────────────────────
    parsed = _extract_json(raw_text)

    if parsed is None:
        result["error"] = "JSON 解析失败，AI 返回了非 JSON 格式的内容"
        logger.warning(f"analyze({model_id}): JSON 解析失败\n原始内容(前2000字):\n{raw_text[:2000]}")
        # 把完整原始内容写到文件，方便离线检查
        try:
            log_dir = pathlib.Path("ai_raw_logs")
            log_dir.mkdir(exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fpath = log_dir / f"{model_id}_{ts}.txt"
            fpath.write_text(raw_text, encoding="utf-8")
            logger.warning(f"完整原始内容已写入: {fpath}")
        except Exception:
            pass
        return result

    # ── EN 模型输出翻译 ───────────────────────────────────
    if _is_en_model(model_id):
        parsed = _translate_en_to_zh(parsed)

    _translated = _get_strategy().translate_output(parsed, market_data)
    if _translated is not None:
        parsed = _translated

    # ── 校验并修正字段 ────────────────────────────────────
    parsed = _validate_result(parsed)

    # 检查必要字段是否完整
    missing = [f for f in REQUIRED_FIELDS if f not in parsed]
    if missing:
        result["error"] = f"AI 返回 JSON 缺少字段: {missing}"
        result.update(parsed)   # 保留已有字段，方便调试
        result["信号棒评级"] = _last_bar_rating
        return result

    # ── 成功：合并结果 ────────────────────────────────────
    result.update(parsed)
    result["error"] = None
    result["信号棒评级"] = _last_bar_rating  # 预计算值，不依赖 AI 输出
    return result


def build_history_summary(analysis_result: dict) -> dict:
    """
    从完整分析结果中提取摘要，用于追加到历史记录列表。
    main.py 在每次分析完成后调用此函数。
    """
    return {
        "time":      analysis_result.get("timestamp", ""),
        "symbol":    analysis_result.get("symbol", ""),
        "model_id":  analysis_result.get("model_id", ""),
        "操作建议":  analysis_result.get("操作建议", ""),
        "入场价":    analysis_result.get("入场价"),
        "止盈价":    analysis_result.get("止盈价"),
        "止损价":    analysis_result.get("止损价"),
        "止损依据":  analysis_result.get("止损依据", ""),
        "反手方向":  analysis_result.get("反手方向"),
        "核心逻辑":  analysis_result.get("核心逻辑", ""),
        "市场状态":  analysis_result.get("市场状态", ""),
        "设置类型":  analysis_result.get("设置类型", ""),
        "信号棒评级": analysis_result.get("信号棒评级", ""),
        # 完整内容，用于历史详情展开
        "技术面":    analysis_result.get("技术面", ""),
        "风险提示":  analysis_result.get("风险提示", ""),
        "风险等级":  analysis_result.get("风险等级", ""),
        "product_name": analysis_result.get("product_name", ""),
        # 止损一致性校验结果（有矛盾时才存在）
        "_sl_warning": analysis_result.get("_sl_warning", ""),
    }


# ─────────────────────────────────────────────────────────────
# 自检（不需要真实 API Key，验证 Prompt 构建和 JSON 解析逻辑）
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("analyzers.py 自检（验证 Prompt 构建和 JSON 解析）")
    print("=" * 55)

    # ── 测试 1：Prompt 构建 ────────────────────────────────
    mock_market = {
        "symbol": "SHFE.rb2501",
        "timestamp": "2025-01-15 10:30:00",
        "quote": {"最新价": 3820.0, "涨跌幅%": 0.52},
        "indicators": {"MA5": 3815.0, "RSI14": 58.3},
        "patterns": {"均线形态": "多头排列", "MACD信号": "金叉看多"},
        "klines": [],
        "error": None,
    }
    mock_history = [
        {"time": "10:00", "symbol": "SHFE.rb2501",
         "操作建议": "观望", "置信度": 55, "核心逻辑": "震荡等待方向"},
    ]
    msg, _ = _build_user_message(mock_market, mock_history)
    print("\n[OK] Prompt 构建成功")
    print(f"   User Message 长度: {len(msg)} 字符")
    print(f"   包含历史记录: {'是' if '历史分析记录' in msg else '否'}")
    print(f"   包含行情数据: {'是' if 'SHFE.rb2501' in msg else '否'}")

    # ── 测试 2：JSON 提取（各种格式）──────────────────────
    print("\n── JSON 提取测试 ──")
    test_cases = [
        ('纯 JSON',
         '{"操作建议":"做多"}'),
        ('带代码块',
         '```json\n{"操作建议":"做空"}\n```'),
        ('前后有文字',
         '根据分析，我认为：\n{"操作建议":"观望"}\n以上是我的判断。'),
        ('无效 JSON',
         '抱歉，我无法提供分析。'),
    ]
    for name, text in test_cases:
        result = _extract_json(text)
        ok = result is not None if name != '无效 JSON' else result is None
        print(f"  {'[OK]' if ok else '[ERR]'} {name}: {result}")

    # ── 测试 3：字段校验 ──────────────────────────────────
    print("\n── 字段校验测试 ──")
    cases = [
        ({"操作建议": "持有", "风险等级": "很高"},
         {"操作建议": "持有", "风险等级": "中"}),
        ({"操作建议": "做多", "风险等级": "低"},
         {"操作建议": "做多", "风险等级": "低"}),
    ]
    all_ok = True
    for inp, expected in cases:
        out = _validate_result(inp.copy())
        for k, v in expected.items():
            if out[k] != v:
                print(f"  [ERR] {k}: 期望 {v}，实际 {out[k]}")
                all_ok = False
    print(f"  {'[OK] 全部通过' if all_ok else '[ERR] 存在失败'}")

    # ── 测试 3b：止损依据校验 ──────────────────────────────
    print("\n── 止损依据校验测试 ──")
    # 非法值 → 兜底为信号棒止损
    r1 = _validate_result({"操作建议": "做空", "风险等级": "中",
                            "止损依据": "随便写", "止损价": 9778.0, "风险提示": "无风险"})
    assert r1["止损依据"] == "信号棒止损", f"[ERR] 期望信号棒止损，实际{r1['止损依据']}"
    print("  [OK] 非法止损依据 → 兜底为'信号棒止损'")

    # 矛盾场景：风险提示提到9760，止损依据=信号棒止损，止损价=9778（偏差18点）→ 应触发警告
    r2 = _validate_result({"操作建议": "做空", "风险等级": "中",
                            "止损依据": "信号棒止损", "止损价": 9778.0,
                            "风险提示": "若价格突破EMA（9760）则趋势可能转为震荡，需严格执行止损"})
    assert "_sl_warning" in r2, "[ERR] 矛盾场景应生成 _sl_warning"
    print(f"  [OK] 矛盾场景 → 警告: {r2['_sl_warning']}")

    # 自洽场景：止损依据=价位止损，止损价与风险提示价位一致 → 不应触发警告
    r3 = _validate_result({"操作建议": "做空", "风险等级": "中",
                            "止损依据": "价位止损", "止损价": 9762.0,
                            "风险提示": "若价格突破EMA（9760）则趋势可能转为震荡"})
    assert "_sl_warning" not in r3 or not r3["_sl_warning"], "[ERR] 自洽场景不应生成 _sl_warning"
    print(f"  [OK] 自洽场景 → 无警告")

    # ── 测试 4：历史摘要提取 ──────────────────────────────
    print("\n── 历史摘要提取测试 ──")
    full_result = {
        "timestamp": "2025-01-15 10:30:00",
        "symbol": "SHFE.rb2501",
        "操作建议": "做多",
        "核心逻辑": "均线多头排列，MACD金叉",
        "技术面": "...",
    }
    summary = build_history_summary(full_result)
    assert len(summary) >= 15, f"摘要字段数不足，实际{len(summary)}"
    assert summary["操作建议"] == "做多"
    print(f"  [OK] 摘要字段数: {len(summary)}，内容: {summary}")
