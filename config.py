"""
config.py — 配置层

职责：
1. 从 .env 读取所有参数
2. 扫描哪些 AI 模型已配置 API Key，生成可用模型列表
3. 提供 is_trading_time(symbol, dt) 函数，判断当前是否处于交易时间
"""

import os
from datetime import datetime, time, timedelta
from dotenv import load_dotenv

load_dotenv()

# ── 天勤账号 ─────────────────────────────────────────────────
TQ_USERNAME: str = os.getenv("TQ_USERNAME", "")
TQ_PASSWORD: str = os.getenv("TQ_PASSWORD", "")

# ── AI 模型配置 ──────────────────────────────────────────────
ANTHROPIC_API_KEY: str    = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL_NAME: str = os.getenv("ANTHROPIC_MODEL_NAME", "claude-opus-4-5")

OPENAI_API_KEY: str    = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL: str   = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_MODEL_NAME: str = os.getenv("OPENAI_MODEL_NAME", "gpt-4o")

DEEPSEEK_API_KEY: str    = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL: str   = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")
DEEPSEEK_MODEL_NAME: str = os.getenv("DEEPSEEK_MODEL_NAME", "deepseek-chat")

GROK_API_KEY: str    = os.getenv("GROK_API_KEY", "")
GROK_BASE_URL: str   = os.getenv("GROK_BASE_URL", "https://api.x.ai/v1")
GROK_MODEL_NAME: str = os.getenv("GROK_MODEL_NAME", "grok-3")

QWEN_API_KEY: str    = os.getenv("QWEN_API_KEY", "")
QWEN_BASE_URL: str   = os.getenv("QWEN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
QWEN3_MODEL_NAME: str = os.getenv("QWEN3_MODEL_NAME", "qwen3.5-plus")

GEMINI_API_KEY: str    = os.getenv("GEMINI_API_KEY", "")
GEMINI_BASE_URL: str   = os.getenv("GEMINI_BASE_URL", "https://generativelanguage.googleapis.com/v1beta/openai/")
GEMINI_MODEL_NAME: str = os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash")

OPENROUTER_API_KEY: str    = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL: str   = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
OPENROUTER_MODEL_NAME: str = os.getenv("OPENROUTER_MODEL_NAME", "google/gemini-2.5-pro")

NVIDIA_API_KEY: str    = os.getenv("NVIDIA_API_KEY", "")
NVIDIA_BASE_URL: str   = os.getenv("NVIDIA_BASE_URL", "https://integrate.api.nvidia.com/v1")
NVIDIA_MODEL_NAME: str = os.getenv("NVIDIA_MODEL_NAME", "deepseek-ai/deepseek-v3.2")

CLAUDE2API_API_KEY: str    = os.getenv("CLAUDE_API_KEY", "")
CLAUDE2API_BASE_URL: str   = os.getenv("CLAUDE_BASE_URL", "http://localhost:8080/v1")
CLAUDE2API_MODEL_NAME: str = os.getenv("CLAUDE_MODEL_NAME", "claude-sonnet-4-6")

# ── 交易账户配置 ─────────────────────────────────────────────
# TQ_ACCOUNT_MODE: "sim" = TqKq快期模拟账户；"live" = 实盘账户
TQ_ACCOUNT_MODE: str  = os.getenv("TQ_ACCOUNT_MODE", "sim")
TQ_ENABLE_TRADING: bool = os.getenv("TQ_ENABLE_TRADING", "false").lower() == "true"

# 实盘账户信息（TQ_ACCOUNT_MODE=live 时生效）
TQ_BROKER:           str = os.getenv("TQ_BROKER",   "")   # 如 "H宏源期货"
TQ_ACCOUNT_ID:       str = os.getenv("TQ_ACCOUNT_ID", "")  # 资金账号
TQ_TRADE_PASSWORD:   str = os.getenv("TQ_TRADE_PASSWORD", "")  # 交易密码

# AI 下单时的默认手数（每次下单固定手数，不按仓位百分比计算）
DEFAULT_TRADE_VOLUME: int = int(os.getenv("DEFAULT_TRADE_VOLUME", "1"))

def is_live_mode() -> bool:
    return TQ_ACCOUNT_MODE.strip().lower() == "live" and bool(TQ_BROKER and TQ_ACCOUNT_ID and TQ_TRADE_PASSWORD)

def get_data_root() -> str:
    """数据根目录：live/sim 隔离，避免实盘和模拟数据互相污染"""
    return "data/live" if is_live_mode() else "data/sim"

# ── 运行参数 ─────────────────────────────────────────────────
DEFAULT_MODEL:              str = os.getenv("DEFAULT_MODEL", "gemini")
DEFAULT_SYMBOL:             str = os.getenv("DEFAULT_SYMBOL", "KQ.m@SHFE.rb")
ANALYSIS_INTERVAL_MINUTES:  int = int(os.getenv("ANALYSIS_INTERVAL_MINUTES", "5"))
KLINE_INTERVAL_SECONDS:     int = ANALYSIS_INTERVAL_MINUTES * 60
AI_KLINE_COUNT:             int = int(os.getenv("AI_KLINE_COUNT", "40"))   # 传给 AI 的 K 线根数
TREND_LOOKBACK_BARS:        int = int(os.getenv("TREND_LOOKBACK_BARS", "10"))  # 预计算趋势斜率窗口
EOD_FORCE_CLOSE_MINUTES:    int = 2   # 尾盘强制平仓窗口（分钟），tick级别检测，与K线周期无关
MAX_CONCURRENT:             int = int(os.getenv("MAX_CONCURRENT", "3"))
PORT:                       int = int(os.getenv("PORT", "8888"))
WECHAT_WEBHOOK_KEY:         str = os.getenv("WECHAT_WEBHOOK_KEY", "")

# ── 功能开关（运行时热更新，无需重启）───────────────────────────
ENABLE_TRAILING_STOP:    bool = os.getenv("ENABLE_TRAILING_STOP",    "true").lower() == "true"
ENABLE_TP_AWARENESS:     bool = os.getenv("ENABLE_TP_AWARENESS",     "true").lower() == "true"
ENABLE_BAR_EXIT:         bool = os.getenv("ENABLE_BAR_EXIT",         "true").lower() == "true"
ENABLE_EOD_CLOSE:        bool = os.getenv("ENABLE_EOD_CLOSE",        "true").lower() == "true"
ENABLE_SKIP_FIRST_BAR:   bool = os.getenv("ENABLE_SKIP_FIRST_BAR",   "true").lower() == "true"
ENABLE_SKIP_LAST_BAR:    bool = os.getenv("ENABLE_SKIP_LAST_BAR",    "true").lower() == "true"
ENABLE_REVERSAL:         bool = os.getenv("ENABLE_REVERSAL",         "true").lower() == "true"
ENABLE_SKIP_NARROW:      bool = os.getenv("ENABLE_SKIP_NARROW",      "true").lower() == "true"
ENABLE_SKIP_OSCILLATION: bool = os.getenv("ENABLE_SKIP_OSCILLATION", "true").lower() == "true"
ENABLE_WECHAT_PUSH:      bool = os.getenv("ENABLE_WECHAT_PUSH",      "false").lower() == "true"

# ── AI 模型注册表 ────────────────────────────────────────────
# 新增模型只需在这里加一条，其他文件无需改动
# Claude 单独使用 anthropic SDK，其余均使用 openai SDK + base_url
MODEL_DEFINITIONS: dict = {
    "claude": {
        "display_name": "Claude (Anthropic)",
        "model_name":   ANTHROPIC_MODEL_NAME,
        "api_key":      ANTHROPIC_API_KEY,
        "lang":         "en",   # English prompt（token efficiency for Western models）
        # 无 base_url：analyzers.py 根据是否有 base_url 决定用哪个 SDK
    },
    "deepseek": {
        "display_name": "DeepSeek",
        "model_name":   DEEPSEEK_MODEL_NAME,
        "api_key":      DEEPSEEK_API_KEY,
        "base_url":     DEEPSEEK_BASE_URL,
        "lang":         "zh",   # 中文 Prompt（中文模型语义更准确）
    },
    "claude2api": {
        "display_name": "Claude2API (本地代理)",
        "model_name":   CLAUDE2API_MODEL_NAME,
        "api_key":      CLAUDE2API_API_KEY,
        "base_url":     CLAUDE2API_BASE_URL,
        "lang":         "en",
    },
    "qwen3": {
        "display_name": "通义千问3 (Qwen3·Thinking)",
        "model_name":   QWEN3_MODEL_NAME,
        "api_key":      QWEN_API_KEY,
        "base_url":     QWEN_BASE_URL,
        "lang":         "zh",
    },
    "gemini": {
        "display_name": "Gemini (Google)",
        "model_name":   GEMINI_MODEL_NAME,
        "api_key":      GEMINI_API_KEY,
        "base_url":     GEMINI_BASE_URL,
        "lang":         "en",
    },
    "openrouter": {
        "display_name": "OpenRouter",
        "model_name":   OPENROUTER_MODEL_NAME,
        "api_key":      OPENROUTER_API_KEY,
        "base_url":     OPENROUTER_BASE_URL,
        "lang":         "en",
    },
    "nvidia": {
        "display_name": "NVIDIA NIM (DeepSeek)",
        "model_name":   NVIDIA_MODEL_NAME,
        "api_key":      NVIDIA_API_KEY,
        "base_url":     NVIDIA_BASE_URL,
        "lang":         "zh",
    },
    "grok": {
        "display_name": "Grok (xAI)",
        "model_name":   GROK_MODEL_NAME,
        "api_key":      GROK_API_KEY,
        "base_url":     GROK_BASE_URL,
        "lang":         "en",
    },
    "openai": {
        "display_name": "OpenAI (GPT)",
        "model_name":   OPENAI_MODEL_NAME,
        "api_key":      OPENAI_API_KEY,
        "base_url":     OPENAI_BASE_URL,
        "lang":         "en",
    },
}


def get_available_models() -> list[dict]:
    """
    返回所有模型的列表，每项包含：
      id           — 模型内部 ID
      display_name — 前端显示名
      model_name   — 实际调用的模型字符串
      enabled      — 是否已配置 API Key
    """
    return [
        {
            "id":           model_id,
            "display_name": cfg["display_name"],
            "model_name":   cfg["model_name"],
            "enabled":      bool(cfg.get("api_key", "")),
        }
        for model_id, cfg in MODEL_DEFINITIONS.items()
    ]


def get_default_model() -> str:
    """
    返回启动时应使用的默认模型 ID。
    若 DEFAULT_MODEL 未配置 Key，自动回退到第一个可用模型。
    若一个模型都没有配置，返回 'none'。
    """
    available = [m for m in get_available_models() if m["enabled"]]
    if not available:
        return "none"
    ids = [m["id"] for m in available]
    return DEFAULT_MODEL if DEFAULT_MODEL in ids else ids[0]


# ── 交易时间判断 ──────────────────────────────────────────────

def _product_of(symbol: str) -> str:
    """
    提取品种字母（小写）
    'SHFE.rb2501'   → 'rb'
    'KQ.m@SHFE.rb'  → 'rb'   主连格式
    'KQ.i@CFFEX.IF' → 'if'   指数格式
    """
    # 主连/指数格式：KQ.m@SHFE.rb → 取 @ 后半部分再处理
    if "@" in symbol:
        symbol = symbol.split("@")[-1]   # → SHFE.rb
    code = symbol.split(".")[-1] if "." in symbol else symbol
    return "".join(c for c in code if c.isalpha()).lower()


# 各品种交易时段（北京时间）
# 跨午夜夜盘拆成两段：21:00–23:59:59 + 00:00–结束时间
_TRADING_SESSIONS: dict[str, list[tuple[time, time]]] = {
    # 股指期货（无夜盘）
    "if": [(time(9,30),  time(11,30)), (time(13,0),  time(15,0))],
    "ih": [(time(9,30),  time(11,30)), (time(13,0),  time(15,0))],
    "ic": [(time(9,30),  time(11,30)), (time(13,0),  time(15,0))],
    "im": [(time(9,30),  time(11,30)), (time(13,0),  time(15,0))],
    # 国债期货（无夜盘）
    "t":  [(time(9,30),  time(11,30)), (time(13,0),  time(15,15))],
    "tf": [(time(9,30),  time(11,30)), (time(13,0),  time(15,15))],
    "ts": [(time(9,30),  time(11,30)), (time(13,0),  time(15,15))],
    # 贵金属（夜盘到次日 02:30）
    "au": [(time(9,0),  time(11,30)), (time(13,30), time(15,0)),
           (time(21,0),  time(23,59,59)), (time(0,0), time(2,30))],
    "ag": [(time(9,0),  time(11,30)), (time(13,30), time(15,0)),
           (time(21,0),  time(23,59,59)), (time(0,0), time(2,30))],
    # 有色金属（夜盘到次日 01:00）
    "cu": [(time(9,0),  time(11,30)), (time(13,30), time(15,0)),
           (time(21,0),  time(23,59,59)), (time(0,0), time(1,0))],
    "al": [(time(9,0),  time(11,30)), (time(13,30), time(15,0)),
           (time(21,0),  time(23,59,59)), (time(0,0), time(1,0))],
    "zn": [(time(9,0),  time(11,30)), (time(13,30), time(15,0)),
           (time(21,0),  time(23,59,59)), (time(0,0), time(1,0))],
    "pb": [(time(9,0),  time(11,30)), (time(13,30), time(15,0)),
           (time(21,0),  time(23,59,59)), (time(0,0), time(1,0))],
    "ni": [(time(9,0),  time(11,30)), (time(13,30), time(15,0)),
           (time(21,0),  time(23,59,59)), (time(0,0), time(1,0))],
    "sn": [(time(9,0),  time(11,30)), (time(13,30), time(15,0)),
           (time(21,0),  time(23,59,59)), (time(0,0), time(1,0))],
    "ss": [(time(9,0),  time(11,30)), (time(13,30), time(15,0)),
           (time(21,0),  time(23,59,59)), (time(0,0), time(1,0))],
    "ao": [(time(9,0),  time(11,30)), (time(13,30), time(15,0)),
           (time(21,0),  time(23,59,59)), (time(0,0), time(1,0))],
    # 原油（夜盘到次日 02:30）
    "sc": [(time(9,0),  time(11,30)), (time(13,30), time(15,0)),
           (time(21,0),  time(23,59,59)), (time(0,0), time(2,30))],
    # 国际铜（INE，夜盘到次日 01:00）
    "bc": [(time(9,0),  time(11,30)), (time(13,30), time(15,0)),
           (time(21,0),  time(23,59,59)), (time(0,0), time(1,0))],
    # 无夜盘品种（农产品、郑商所部分化工、线材、集运指数）
    "jd": [(time(9,0),  time(11,30)), (time(13,30), time(15,0))],
    "lh": [(time(9,0),  time(11,30)), (time(13,30), time(15,0))],
    "ap": [(time(9,0),  time(11,30)), (time(13,30), time(15,0))],
    "cj": [(time(9,0),  time(11,30)), (time(13,30), time(15,0))],
    "pk": [(time(9,0),  time(11,30)), (time(13,30), time(15,0))],
    "wh": [(time(9,0),  time(11,30)), (time(13,30), time(15,0))],
    "pm": [(time(9,0),  time(11,30)), (time(13,30), time(15,0))],
    "ur": [(time(9,0),  time(11,30)), (time(13,30), time(15,0))],
    "sm": [(time(9,0),  time(11,30)), (time(13,30), time(15,0))],
    "sf": [(time(9,0),  time(11,30)), (time(13,30), time(15,0))],
    "wr": [(time(9,0),  time(11,30)), (time(13,30), time(15,0))],
    "ec": [(time(9,0),  time(11,30)), (time(13,30), time(15,0))],
}

# 兜底：普通商品期货（夜盘到 23:00）
_DEFAULT_SESSIONS: list[tuple[time, time]] = [
    (time(9,0),  time(11,30)),
    (time(13,30), time(15,0)),
    (time(21,0),  time(23,0)),
]

def is_trading_time(symbol: str, dt: datetime | None = None) -> bool:
    """
    判断给定时刻是否处于该合约的交易时间段内。
    正确处理跨午夜时段（如 21:00-01:00）。
    收盘时刻本身不算交易时间（严格小于收盘时间）。
    """
    if dt is None:
        dt = datetime.now()

    wd = dt.weekday()  # 0=周一 … 6=周日
    if wd == 6:
        return False  # 周日全天不交易

    product  = _product_of(symbol)
    sessions = _TRADING_SESSIONS.get(product, _DEFAULT_SESSIONS)
    now_min  = dt.hour * 60 + dt.minute

    # 周六凌晨超过 02:30 不交易
    if wd == 5 and now_min > 150:
        return False

    for start, end in sessions:
        s_min = start.hour * 60 + start.minute
        e_min = end.hour   * 60 + end.minute
        # time(23,59,59) 是跨午夜拆分的占位结束时间，整个23:59分钟都属于交易时段
        # 加1使 now_min=1439 能通过 now_min < e_min 检验
        if end == time(23, 59, 59):
            e_min += 1
        if e_min < s_min:  # 跨午夜（理论上不出现，已拆成两段，保留兜底）
            if wd == 5:
                if now_min < e_min:
                    return True
            else:
                if now_min >= s_min:
                    return True
                if now_min < e_min and (wd - 1) % 7 != 6:
                    # 凌晨延续段：前一天不是周日才算交易中
                    return True
        else:
            if s_min <= now_min < e_min:
                # s_min==0 表示这是跨午夜夜盘的凌晨延续段（如 00:00-02:30）
                # 周一凌晨(wd=0)前一天是周日，周日无夜盘，不算交易时间
                if s_min == 0 and wd == 0:
                    continue
                return True
    return False


def is_session_first_bar(symbol: str, dt: datetime | None = None) -> bool:
    """
    判断当前是否处于各小节开盘后的第一根K线（开盘0到 ANALYSIS_INTERVAL_MINUTES 分钟内）。
    用于提示 AI 开盘首根K线不开仓。
    跨午夜拆分的 00:00 续接段不算新小节开盘。
    """
    if dt is None:
        dt = datetime.now()
    product  = _product_of(symbol)
    sessions = _TRADING_SESSIONS.get(product, _DEFAULT_SESSIONS)
    now_min  = dt.hour * 60 + dt.minute
    for start, end in sessions:
        # 跨午夜续接段（00:00 开始）不是真正的开盘，跳过
        if start == time(0, 0):
            continue
        s_min = start.hour * 60 + start.minute
        if s_min <= now_min <= s_min + ANALYSIS_INTERVAL_MINUTES:
            return True
    return False


def is_near_session_end(symbol: str, minutes: int = EOD_FORCE_CLOSE_MINUTES, dt: datetime | None = None) -> bool:
    """
    判断当前时间是否处于各节次收盘前 minutes 分钟内。
    跳过上午节次（第0段），跳过跨午夜分割的 23:59:59 结束段。
    即：下午段（15:00/15:15）、夜盘段（23:00）、隔夜续接段（01:00/02:30）均触发，
    上午段（11:30）不触发。
    """
    if dt is None:
        dt = datetime.now()
    product  = _product_of(symbol)
    sessions = _TRADING_SESSIONS.get(product, _DEFAULT_SESSIONS)
    now_min  = dt.hour * 60 + dt.minute
    for i, (_, end) in enumerate(sessions):
        if i == 0:              # 跳过上午节次
            continue
        if end == time(23, 59, 59):  # 跳过跨午夜分割段
            continue
        e_min = end.hour * 60 + end.minute
        if e_min - minutes <= now_min <= e_min:
            return True
    return False


def is_bar_near_session_end(symbol: str, bar_closing_dt: datetime) -> bool:
    """
    判断给定K线收盘时间是否落在下午/夜盘节次收盘前 ANALYSIS_INTERVAL_MINUTES 分钟内（含两端边界）。
    与 is_session_last_bar 的区别：
    - 基于K线自身的收盘时间，而非 datetime.now()
    - 使用 <= 而非 <，确保恰好等于 e_min - interval 的棒（如14:55收盘）也被捕获
    - 跳过上午节次（11:30 收盘为午休，非真正收盘；由 is_noon_boundary 豁免处理）
    """
    product  = _product_of(symbol)
    sessions = _TRADING_SESSIONS.get(product, _DEFAULT_SESSIONS)
    bar_min  = bar_closing_dt.hour * 60 + bar_closing_dt.minute
    for i, (_, end) in enumerate(sessions):
        if i == 0:                        # 跳过上午节次（午休非真正收盘）
            continue
        if end == time(23, 59, 59):       # 跳过跨午夜分割占位段
            continue
        e_min = end.hour * 60 + end.minute
        if e_min - ANALYSIS_INTERVAL_MINUTES <= bar_min <= e_min:
            return True
    return False


def is_session_last_bar(symbol: str, dt: datetime | None = None) -> bool:
    """
    判断当前是否处于各小节真正的最后一根K线（收盘前 ANALYSIS_INTERVAL_MINUTES 分钟内，
    但不含「恰好等于收盘前 ANALYSIS_INTERVAL_MINUTES 分钟」的那一刻）。
    用于拦截尾盘开仓。

    下界用严格大于（<），避免 K 线回调在「末根刚开始」时（now == e_min - interval）
    误触发拦截——该回调实际是上一根 bar 刚收盘，分析对象并非末根本身。
    跨午夜拆分的 23:59:59 结束段不是真正的收盘，跳过（由 00:00 续接段统一处理）。
    """
    if dt is None:
        dt = datetime.now()
    product  = _product_of(symbol)
    sessions = _TRADING_SESSIONS.get(product, _DEFAULT_SESSIONS)
    now_min  = dt.hour * 60 + dt.minute
    for start, end in sessions:
        # 跨午夜拆分的第一段（结束于 23:59:59）不是真正的收盘，跳过
        if end == time(23, 59, 59):
            continue
        e_min = end.hour * 60 + end.minute
        if e_min - ANALYSIS_INTERVAL_MINUTES < now_min <= e_min:
            return True
    return False


def is_noon_boundary(symbol: str, dt: datetime | None = None) -> bool:
    """
    判断当前是否处于午休过渡窗口，豁免早盘/尾盘开仓限制：
    - 早盘末根 K 线（11:25–11:30，所有品种）
    - 午后开盘首根 K 线（13:00–13:05 或 13:30–13:35，视品种）
    此窗口内禁止开仓逻辑不适用，但分析跳过逻辑（is_session_last_bar）照常生效。
    """
    if dt is None:
        dt = datetime.now()
    now_min = dt.hour * 60 + dt.minute
    # 早盘结束前最后一根 K 线（11:25–11:30）
    if 11 * 60 + 30 - ANALYSIS_INTERVAL_MINUTES <= now_min <= 11 * 60 + 30:
        return True
    # 午后开盘首根 K 线（开盘时间在 13:00–14:00 之间）
    product  = _product_of(symbol)
    sessions = _TRADING_SESSIONS.get(product, _DEFAULT_SESSIONS)
    for start, _ in sessions:
        if start == time(0, 0):
            continue
        s_min = start.hour * 60 + start.minute
        if 780 <= s_min <= 840 and s_min <= now_min <= s_min + ANALYSIS_INTERVAL_MINUTES:
            return True
    return False


# ── 自检（直接运行此文件时输出当前配置状态）──────────────────
if __name__ == "__main__":
    print("=" * 52)
    print("可用模型列表：")
    for m in get_available_models():
        status = "✅ 已启用" if m["enabled"] else "❌ 未配置 Key"
        print(f"  [{m['id']:<8}] {m['display_name']:<22} {m['model_name']:<25} {status}")

    print(f"\n默认模型 : {get_default_model()}")
    print(f"默认合约 : {DEFAULT_SYMBOL}")
    print(f"K线周期  : {ANALYSIS_INTERVAL_MINUTES} 分钟（{KLINE_INTERVAL_SECONDS}秒）")
    print(f"端口     : {PORT}")

    print("\n交易时间自检（当前时刻）：")
    test_symbols = [
        "SHFE.rb2501", "SHFE.au2501", "SHFE.cu2501",
        "INE.sc2501",  "CFFEX.IF2501", "DCE.m2501",
    ]
    now = datetime.now()
    print(f"  当前北京时间: {now.strftime('%H:%M:%S')}")
    for sym in test_symbols:
        flag = is_trading_time(sym, now)
        print(f"  {sym:<22} → {'✅ 交易中' if flag else '🔴 非交易时间'}")
