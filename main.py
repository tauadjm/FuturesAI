"""
main.py — 主程序（多合约版）
"""

import sys, pathlib, subprocess
_venv_python = pathlib.Path(__file__).resolve().parent / ".venv" / "Scripts" / "python.exe"
if _venv_python.exists() and pathlib.Path(sys.executable).resolve() != _venv_python.resolve():
    sys.exit(subprocess.call([str(_venv_python)] + sys.argv))

import asyncio
import pathlib
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel

import config
import data_feed
import analyzers
import trader
from strategies import get_strategy as _get_strategy_main

# ── Logging 配置：按日期自动切割，保留 30 天 ──────────────────────────
import logging.handlers

os.makedirs("logs", exist_ok=True)

_log_formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",   # 加上日期，方便跨天排查
)

# 每天午夜切割，保留最近 30 天，文件名格式：2025-01-13.run.log
_file_handler = logging.handlers.TimedRotatingFileHandler(
    filename="logs/run.log",
    when="midnight",
    interval=1,
    backupCount=30,
    encoding="utf-8",
)
_file_handler.suffix = "%Y-%m-%d"      # 内部后缀，实际文件名由 namer 重写

def _log_namer(default_name: str) -> str:
    """将 logs/run.log.2025-01-13 重命名为 logs/2025-01-13.run.log"""
    import os as _os
    dir_part = _os.path.dirname(default_name)
    base = _os.path.basename(default_name)   # run.log.2025-01-13
    stem, date = base.rsplit(".", 1)          # stem=run.log, date=2025-01-13
    return _os.path.join(dir_part, f"{date}.{stem}")

_file_handler.namer = _log_namer
_file_handler.setFormatter(_log_formatter)

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_log_formatter)

logging.basicConfig(
    level=logging.INFO,
    handlers=[_console_handler, _file_handler],
    force=True,  # 强制清除已有 handler，防止 uvicorn 提前注册的 handler 残留导致日志重复输出
)
# uvicorn 内部 logger 不再向 root 传播，避免访问日志二次输出
logging.getLogger("uvicorn").propagate = False
logging.getLogger("uvicorn.access").propagate = False
logging.getLogger("uvicorn.error").propagate = False
logger = logging.getLogger(__name__)
# ─────────────────────────────────────────────────────────────────────


def _make_contract(model_id: str, symbol: str = "") -> dict:
    # 从按日期分文件中倒查最近30天，恢复最新20条分析记录
    saved_history = []
    if symbol:
        try:
            from datetime import date as _date, timedelta as _td
            today = _date.today()
            collected: list[dict] = []
            for i in range(30):  # 最多往前找30天
                d = (today - _td(days=i)).isoformat()
                f = _history_file(d, symbol)
                if not f.exists():
                    continue
                day_recs = []
                for line in f.read_text(encoding="utf-8").splitlines():
                    if not line.strip():
                        continue
                    try:
                        r = json.loads(line)
                        if r.get("model_id") == model_id:
                            day_recs.append(r)
                    except Exception:
                        pass
                # 当天记录时间正序，插到已收集记录的前面（倒查：今天最新）
                collected = day_recs + collected
                if len(collected) >= 20:
                    break
            saved_history = collected[-20:]  # 取最新20条，时间正序
            if saved_history:
                logger.info(f"[{symbol}] 从文件恢复 {len(saved_history)} 条历史分析记录")
        except Exception as e:
            logger.warning(f"恢复历史记录失败 [{symbol}]: {e}")
    return {
        "model":        model_id,
        "loop_running": True,
        "analysis":     None,
        "prices":       None,
        "history":      saved_history,
    }


_file_lock: asyncio.Lock | None = None  # Bug3: 在 lifespan 中初始化（asyncio.Lock 需在事件循环内创建）

# ── 持仓查询共享 Task（同一 K 线周期内多品种只查一次，避免 tqsdk 锁串行累积）──
_positions_task: asyncio.Task | None = None
_positions_cache: list = []
_positions_cache_time: float = 0.0
_POSITIONS_TTL: float = 15.0  # 秒，覆盖一个分析周期
_POSITIONS_WAIT_TIMEOUT: float = 1.2   # 秒，单次等待上限（超时优先回退缓存）
_POSITIONS_MAX_STALE: float = 300.0    # 秒，回退缓存允许的最大陈旧度

# 行情抓取独立线程池：避免默认 executor 被其他任务占满或线程数过小导致串行化。
_market_data_executor: ThreadPoolExecutor | None = None

def _get_market_data_workers() -> int:
    cpu = os.cpu_count() or 4
    return max(4, min(16, cpu + 2))

async def _fetch_market_data(symbol: str) -> dict[str, Any]:
    """包装 data_feed.get_market_data，并记录执行时刻用于计时分离"""
    loop = asyncio.get_event_loop()
    _t_submit = time.perf_counter()
    
    def _fetch_timed():
        _t_exec_start = time.perf_counter()
        result = data_feed.get_market_data(symbol)
        _t_exec_end = time.perf_counter()
        # 把执行时刻附加到结果中，供调用方日志使用
        result['_fetch_exec_start'] = _t_exec_start
        result['_fetch_exec_end'] = _t_exec_end
        result['_fetch_submit_time'] = _t_submit
        return result
    
    return await loop.run_in_executor(_market_data_executor, _fetch_timed)

async def _get_positions_shared() -> list:
    """多个并发分析协程共享同一次 get_positions 调用，避免 tqsdk 锁串行。"""
    global _positions_task, _positions_cache, _positions_cache_time
    _now = time.perf_counter()
    if _now - _positions_cache_time < _POSITIONS_TTL:
        return _positions_cache
    # 第一个到达的协程创建 Task，后续协程直接 await 同一个 Task
    if _positions_task is None or _positions_task.done():
        _positions_task = asyncio.ensure_future(
            asyncio.get_event_loop().run_in_executor(None, trader.get_positions)
        )

    # 不让慢查询拖住整轮分析：超时后优先回退最近缓存（若缓存仍可接受）
    try:
        result = await asyncio.wait_for(asyncio.shield(_positions_task), timeout=_POSITIONS_WAIT_TIMEOUT)
    except asyncio.TimeoutError:
        _cache_age = _now - _positions_cache_time
        if _positions_cache and _cache_age < _POSITIONS_MAX_STALE:
            logger.warning("get_positions 超时(>%.1fs)，回退缓存(%.1fs)", _POSITIONS_WAIT_TIMEOUT, _cache_age)
            return _positions_cache
        # 首次启动且无缓存时，仍需等待真实结果，避免误判为无持仓
        result = await _positions_task

    _positions_cache = result
    _positions_cache_time = time.perf_counter()
    return result


# 行情 fetch 直接按任务并发执行：fetch() 只读内存，不调用 tqsdk 函数。


async def _wechat_push(text: str) -> None:
    """推送文字消息到企业微信群机器人，KEY 未配置或推送关闭时静默跳过"""
    if not config.ENABLE_WECHAT_PUSH:
        return
    key = config.WECHAT_WEBHOOK_KEY
    if not key:
        return
    url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={key}"
    try:
        import httpx
        async with httpx.AsyncClient(timeout=5) as client:
            await client.post(url, json={"msgtype": "text", "text": {"content": text}})
    except Exception as e:
        logger.warning(f"[微信推送] 失败: {e}")


def _sym_to_product(symbol: str) -> str:
    """从 symbol 提取品种代码：KQ.m@SHFE.rb → rb，SHFE.rb2605 → rb"""
    import re as _re
    part = symbol.split("@")[-1].split(".")[-1] if symbol else ""
    m = _re.match(r"([a-zA-Z]+)", part)
    return m.group(1).lower() if m else (part.lower() or "_all")


def _history_file(date_str: str = "", symbol: str = "", ensure_dir: bool = False) -> pathlib.Path:
    """返回历史记录文件路径：data/history/YYYY-MM-DD/品种.jsonl"""
    d = date_str or datetime.now().strftime("%Y-%m-%d")
    date_dir = pathlib.Path(config.get_data_root()) / "history" / d
    if ensure_dir:
        date_dir.mkdir(parents=True, exist_ok=True)
    product = _sym_to_product(symbol) if symbol else "_all"
    return date_dir / f"{product}.jsonl"


def _history_date_range(start_date: str, end_date: str, default_days: int = 7) -> list[str]:
    """返回 [start_date, end_date] 闭区间内所有日期字符串，未传时默认最近 default_days 天。"""
    from datetime import date as _date, timedelta as _td
    today = _date.today()
    try:
        s = _date.fromisoformat(start_date) if start_date else today - _td(days=default_days - 1)
    except ValueError:
        s = today - _td(days=default_days - 1)
    try:
        e = _date.fromisoformat(end_date) if end_date else today
    except ValueError:
        e = today
    result, cur = [], s
    while cur <= e:
        result.append(cur.isoformat())
        cur += _td(days=1)
    return result



async def _save_history_record(symbol: str, record: dict):
    """追加一条历史记录到今天的日期文件（加锁保证并发安全）"""
    global _file_lock
    if _file_lock is None:
        _file_lock = asyncio.Lock()
    try:
        async with _file_lock:
            f = _history_file(symbol=symbol, ensure_dir=True)  # 写入时才创建目录，避免空日期文件夹
            with open(f, "a", encoding="utf-8") as fp:
                fp.write(json.dumps({**record, "symbol": symbol}, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning(f"写入历史记录失败: {e}")

def _load_history(symbol: str = "", limit: int = 100, start_date: str = "", end_date: str = "", model_id: str = "") -> list[dict]:
    """从按日期/品种分目录读取历史记录，默认最近7天。"""
    data_root = pathlib.Path(config.get_data_root())
    records = []
    for d in reversed(_history_date_range(start_date, end_date, default_days=7)):
        date_dir = data_root / "history" / d
        # 确定本日期要读的文件列表
        if date_dir.is_dir():
            if symbol:
                files = [date_dir / f"{_sym_to_product(symbol)}.jsonl"]
            else:
                files = sorted(date_dir.glob("*.jsonl"))
        else:
            # 兼容旧平铺结构 history/YYYY-MM-DD.jsonl
            old_f = data_root / "history" / f"{d}.jsonl"
            files = [old_f] if old_f.exists() else []

        for f in files:
            if not f.exists():
                continue
            try:
                for line in f.read_text(encoding="utf-8").splitlines():
                    if not line.strip():
                        continue
                    try:
                        r = json.loads(line)
                        if symbol and r.get("symbol") != symbol:
                            continue  # 兼容旧平铺文件中可能混入其他品种的记录
                        if model_id and r.get("model_id") != model_id:
                            continue
                        records.append(r)
                    except Exception:
                        continue
            except Exception as e:
                logger.warning(f"读取历史文件失败 {f}: {e}")
    # 已按日期倒序遍历，文件内部是时间正序，整体 reverse 后最新在前
    records.reverse()
    return records[:limit]

def _passes_date_filter(record: dict, start_date: str, end_date: str) -> bool:
    """检查记录是否通过日期筛选"""
    time_str = record.get("time", "")
    if not time_str:
        return False
    if start_date and time_str[:10] < start_date:
        return False
    if end_date:
        if time_str[:10] > end_date:
            return False
    return True


state: dict[str, Any] = {
    "contracts":      {},
    "active_symbol":  config.DEFAULT_SYMBOL,
    "custom_prompt":  None,  # 用户自定义 Prompt；None=自动按持仓状态选择
    "max_concurrent": config.MAX_CONCURRENT,
    "_semaphore":     None,
    "sl_tp_monitor":  {},
    "logs":           [],
    "sse_clients":    set(),
}


def _add_log(message: str, level: str = "INFO"):
    entry = {"time": datetime.now().strftime("%H:%M:%S"), "level": level, "message": message}
    state["logs"].append(entry)
    if len(state["logs"]) > 80:
        state["logs"].pop(0)
    if level == "ERROR":
        logger.error(message)
    elif level == "WARNING":
        logger.warning(message)
    else:
        logger.info(message)


def _push_sse(data: dict):
    msg  = json.dumps(data, ensure_ascii=False)
    dead = set()
    # Bug2修复: 先复制快照再迭代，避免其他协程同时 add/discard 触发 RuntimeError
    for q in list(state["sse_clients"]):
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            dead.add(q)
    state["sse_clients"] -= dead


def _market_sse_payload(market_data: dict[str, Any], symbol: str, key: str) -> dict[str, Any]:
    """SSE 行情事件轻量化：移除大体积 klines，降低事件循环序列化开销。"""
    payload = dict(market_data)
    payload.pop("klines", None)
    payload["symbol"] = symbol
    payload["key"] = key
    return payload


def _parse_key(key: str) -> tuple[str, str]:
    """拆分合约键 'symbol::model' → (symbol, model_id)"""
    parts = key.split("::", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""

def _make_key(symbol: str, model_id: str) -> str:
    return f"{symbol}::{model_id}"

def _get_contract(key: str) -> dict | None:
    return state["contracts"].get(key)

def _require_contract(key: str) -> dict:
    c = _get_contract(key)
    if c is None:
        raise HTTPException(status_code=404, detail=f"合约 {key} 未添加")
    return c


async def _fetch_prices_only(key: str):
    # Bug-E 修复：原签名接收 symbol，但 _get_contract 需要 "symbol::model_id" 格式的 key，
    # 导致 c 恒为 None，prices 永远不更新，启动后行情面板静默空白。
    # 修复：改为接收 key，用 _parse_key 拆出 symbol 供数据获取，key 供合约查找。
    symbol, _ = _parse_key(key)
    try:
        market_data = await _fetch_market_data(symbol)
        if market_data.get("pending"):
            _add_log(f"等待 {symbol} 数据到达...")
            for _ in range(10):
                await asyncio.sleep(3)
                market_data = await _fetch_market_data(symbol)
                if not market_data.get("pending"):
                    break
        c = _get_contract(key)                          # ✅ 用完整 key 查找合约
        if c is not None and not market_data.get("pending"):
            c["prices"] = market_data
            _push_sse(_market_sse_payload(market_data, symbol, key))
    except Exception as e:
        _add_log(f"获取行情异常 [{symbol}]: {e}", "ERROR")


async def _run_one_analysis(key: str):
    """key = 'symbol::model_id'"""
    symbol, model_id = _parse_key(key)
    c = _get_contract(key)
    if c is None:
        return
    if c.get("_analyzing"):
        _add_log(f"[{key}] 上轮分析未完成，跳过本次触发", "WARNING")
        return
    c["_analyzing"] = True
    semaphore  = state["_semaphore"]
    is_trading = config.is_trading_time(symbol)

    if data_feed._feed is None or not data_feed._feed.is_ready():
        _add_log(f"天勤未连接，跳过分析 [{model_id}] {symbol}", "ERROR")
        c["_analyzing"] = False
        return

    _add_log(f"开始分析 [{model_id}] {symbol} ...")
    _t_start = time.perf_counter()
    _fetch_calls = 0
    _fetch_queue_total = 0.0
    _fetch_exec_total = 0.0
    _fetch_struct_total = 0.0
    _fetch_pending_wait_total = 0.0
    _fetch_resume_lag_total = 0.0
    _fetch_submit_gap_total = 0.0

    def _accumulate_fetch_detail(md: dict[str, Any], t_before_await: float, t_after_await: float) -> None:
        nonlocal _fetch_calls, _fetch_queue_total, _fetch_exec_total, _fetch_struct_total
        nonlocal _fetch_resume_lag_total, _fetch_submit_gap_total
        if not isinstance(md, dict):
            return
        _fetch_calls += 1
        _t_submit = md.get("_fetch_submit_time", 0.0)
        _t_exec_start = md.get("_fetch_exec_start", 0.0)
        _t_exec_end = md.get("_fetch_exec_end", 0.0)
        if _t_submit:
            _fetch_submit_gap_total += max(0.0, _t_submit - t_before_await)
        if _t_submit and _t_exec_start:
            _fetch_queue_total += max(0.0, _t_exec_start - _t_submit)
        if _t_exec_start and _t_exec_end:
            _fetch_exec_total += max(0.0, _t_exec_end - _t_exec_start)
            _fetch_resume_lag_total += max(0.0, t_after_await - _t_exec_end)
        _fetch_struct_total += float(md.get("_struct_calc_time", 0.0) or 0.0)

    try:
        _t_before_fetch_await = time.perf_counter()
        market_data = await _fetch_market_data(symbol)
        _t_after_fetch_await = time.perf_counter()
        _accumulate_fetch_detail(market_data, _t_before_fetch_await, _t_after_fetch_await)
    except Exception as e:
        _add_log(f"获取行情异常: {e}", "ERROR")
        c["_analyzing"] = False
        return
    _t_fetch = time.perf_counter()
    
    # 调试：检查是否有等待时间来自于 await 的调度延迟
    _await_time = _t_fetch - _t_start
    if isinstance(market_data, dict):
        _t_submit = market_data.get('_fetch_submit_time', 0)
        _t_exec_start = market_data.get('_fetch_exec_start', 0)
        if _t_submit and _t_exec_start:
            # 记录"await 提交到协程获得 loop 的时间"
            logger.debug(f"[{symbol}] await total={_await_time:.3f}s, queue={_t_exec_start - _t_submit:.3f}s")

    if market_data.get("pending"):
        for _ in range(10):
            _t_wait_start = time.perf_counter()
            await asyncio.sleep(3)
            _fetch_pending_wait_total += time.perf_counter() - _t_wait_start
            _t_before_fetch_await = time.perf_counter()
            market_data = await _fetch_market_data(symbol)
            _t_after_fetch_await = time.perf_counter()
            _accumulate_fetch_detail(market_data, _t_before_fetch_await, _t_after_fetch_await)
            if not market_data.get("pending"):
                break
        else:
            _add_log(f"等待行情超时，跳过分析: {symbol}", "ERROR")
            c["_analyzing"] = False
            return
        if market_data.get("pending"):
            c["_analyzing"] = False
            return
        _t_fetch = time.perf_counter()

    if market_data.get("error"):
        _add_log(f"行情数据错误: {market_data['error']}", "ERROR")
        c["_analyzing"] = False
        return

    market_data["is_trading_time"] = is_trading
    c["prices"] = market_data
    _push_sse(_market_sse_payload(market_data, symbol, key))

    positions = []
    if config.TQ_ENABLE_TRADING:
        try:
            all_positions = await _get_positions_shared()
            # 只传入与当前合约相关的持仓，避免 AI 看到其他合约持仓而产生混淆
            def _sym_match(pos_sym: str, target: str) -> bool:
                if pos_sym == target:
                    return True
                import re
                m = re.search(r'@([A-Z]+)\.([a-zA-Z]+)', target)
                if not m:
                    return target.lower() in pos_sym.lower()
                exch, code = m.group(1).lower(), m.group(2).lower()
                return pos_sym.lower().startswith(exch + '.' + code)
            positions = [p for p in all_positions if _sym_match(p.get('symbol', ''), symbol)]
        except Exception as e:
            logger.warning("获取持仓失败，以无持仓继续: %s", e)
    _t_pos = time.perf_counter()

    # ── 涨跌停板检测：锁板期间无交易意义，跳过本轮AI分析 ───────────────
    _limit_up   = market_data.get("is_limit_up", False)
    _limit_down = market_data.get("is_limit_down", False)
    if _limit_up or _limit_down:
        _board = "涨停板" if _limit_up else "跌停板"
        logger.info("[%s][%s] %s 期间跳过AI分析", symbol, model_id, _board)
        c["_analyzing"] = False
        return  # 市场数据已在上方 _push_sse 推送（含 is_limit_up），前端通过价格事件更新标识

    # ── 第零步预过滤（代码层，无论有无持仓均检查）──────────────────────────
    # 尾盘末根：无论有无持仓均跳过。有持仓由 tick 级守卫 EOD 强制平仓（收盘前2分钟），无需AI介入
    # 用刚收盘K线的收盘时间（klines[-2]）判断，而非 datetime.now()：
    #   - 14:55 回调（分析14:50-14:55棒）：klines[-2]["time"]="14:55"，命中 [14:55,15:00] ✓
    #   - 21:00 回调（夜盘开盘，分析14:55-15:00棒）：klines[-2]["time"]="15:00"，命中 ✓
    _klines_raw = market_data.get("klines", [])
    _closed_bar_time_str = _klines_raw[-2].get("time", "") if len(_klines_raw) >= 2 else ""
    if _closed_bar_time_str:
        try:
            _closed_bar_dt = datetime.strptime(_closed_bar_time_str, "%Y-%m-%d %H:%M")
            if config.is_bar_near_session_end(symbol, _closed_bar_dt) and not config.is_noon_boundary(symbol):
                _add_log(f"[{key}] ⛔ 尾盘K线({_closed_bar_time_str[-5:]})，跳过AI分析")
                c["_analyzing"] = False
                return
        except Exception:
            pass

    if not positions:
        _prefilter_reason = None
        if config.ENABLE_SKIP_OSCILLATION and market_data.get("趋势方向") == "震荡":
            _prefilter_reason = "震荡区间跳过分析"
        elif config.ENABLE_SKIP_NARROW and market_data.get("是否窄幅区间"):
            _prefilter_reason = "窄幅区间跳过分析"
        elif config.ENABLE_SKIP_FIRST_BAR and market_data.get("is_session_first_bar") and not config.is_noon_boundary(symbol):
            _prefilter_reason = "开盘首根K线禁止开仓"
        else:
            # 守卫冷静期：守卫触发后 15 分钟内无持仓，无需开仓分析，直接跳过
            _cool_guard_rec = None
            for _r in reversed(c["history"]):
                if _r.get("操作建议") in ("守卫止盈", "守卫止损", "守卫平仓", "跟进不足退出", "尾盘强制平仓"):
                    _cool_guard_rec = _r
                    break
            if _cool_guard_rec and not _cool_guard_rec.get("_is_breakeven", False):
                try:
                    _cool_exit_t = datetime.strptime(_cool_guard_rec["time"][:16], "%Y-%m-%d %H:%M")
                    _cool_mins = (datetime.now() - _cool_exit_t).total_seconds() / 60
                    if 0 <= _cool_mins <= 15:
                        _prefilter_reason = f"守卫冷静期跳过分析({_cool_mins:.0f}min内)"
                except Exception:
                    pass
        if _prefilter_reason:
            _add_log(f"[{key}] ⛔ {_prefilter_reason}，跳过AI分析")
            c["_analyzing"] = False
            return

    # ── 早退开仓：AI 流中检测到操作建议+入场价时立即下单，守卫在流中发现止损/止盈时立即补充 ──
    _early_order_result: dict = {}
    _early_guard_supplemented: bool = False  # 流式阶段是否已成功补充守卫
    _early_guard_partial: bool = False       # 流式补充时有一方被过滤，兜底阶段须补全缺失项
    _early_action: str = ""                  # 早退阶段记录的操作方向（做多/做空）
    _early_intercepted: bool = False         # 早退被主动拦截（追价/首根/尾盘），后分析不得再下单

    async def _on_early_signal(action: str, entry_price: float) -> None:
        """AI 流中检测到做多/做空信号时立即触发，不等完整回复"""
        nonlocal _early_action, _early_intercepted
        if not config.TQ_ENABLE_TRADING:
            return
        _early_action = action

        # 开盘首根拦截（午后开盘 13:30/13:00 除外）
        if config.ENABLE_SKIP_FIRST_BAR and (market_data or {}).get("is_session_first_bar") and not config.is_noon_boundary(symbol):
            _add_log(f"⚡ 早退信号被首根拦截: {key}", "WARNING")
            _early_intercepted = True
            return

        # 尾盘拦截（午休前最后一根 11:25-11:30 除外）
        if config.ENABLE_SKIP_LAST_BAR and (market_data or {}).get("is_session_last_bar") and not config.is_noon_boundary(symbol):
            _add_log(f"⚡ 早退信号被尾盘拦截: {key}", "WARNING")
            _early_intercepted = True
            return

        # 守卫冷却期 + 价格追入检查（与 post-analysis 逻辑一致）
        # 先 yield 一次，让 call_soon_threadsafe 排队的守卫平仓回调先写入 history，
        # 再做冷却期检查，避免竞态导致守卫记录还不在 history 里。
        await asyncio.sleep(0)
        _guard_rec = None
        for _r in reversed(c["history"]):
            if _r.get("操作建议") in ("守卫止盈", "守卫止损", "守卫平仓", "跟进不足退出", "尾盘强制平仓"):
                _guard_rec = _r
                break
        if _guard_rec:
            try:
                _guard_time   = datetime.fromisoformat(_guard_rec["time"])
                _elapsed_bars = int((datetime.now() - _guard_time).total_seconds() / 300)
            except Exception:
                _elapsed_bars = 99
            if _elapsed_bars < 2:
                # 与 post-analysis 一致：保本止损 或 反方向信号不受冷却限制
                _gdir_e = _guard_rec.get("_guard_direction")  # "LONG" / "SHORT"
                _is_be_e = _guard_rec.get("_is_breakeven", False)
                _same_dir_e = (
                    (_gdir_e == "LONG"  and action == "做多") or
                    (_gdir_e == "SHORT" and action == "做空") or
                    not _gdir_e  # 方向未知，保守拦截
                )
                if not _is_be_e and _same_dir_e:
                    _add_log(f"⚡ 早退被守卫冷却期拦截({_elapsed_bars}根K线): {key}", "WARNING")
                    _early_intercepted = True
                    return
            if _elapsed_bars < 3:
                _ep   = _guard_rec.get("_exit_price")
                _gdir = _guard_rec.get("_guard_direction")
                _cur  = (market_data or {}).get("quote", {}).get("最新价")
                if _ep and _gdir and _cur:
                    if _gdir == "LONG" and action == "做多" and _cur > _ep:
                        _add_log(f"⚡ 早退追价拦截: 当前{_cur} > 出场{_ep}，禁止追多", "WARNING")
                        _early_intercepted = True
                        return
                    if _gdir == "SHORT" and action == "做空" and _cur < _ep:
                        _add_log(f"⚡ 早退追价拦截: 当前{_cur} < 出场{_ep}，禁止追空", "WARNING")
                        _early_intercepted = True
                        return

        _add_log(f"⚡ 早退开仓触发: [{model_id}] {symbol} → {action} 入场≈{entry_price}")
        try:
            _early_analysis = {
                "操作建议": action,
                "symbol":   symbol,
                "入场价":   entry_price,
                "止损价":   None,
                "止盈价":   None,
            }
            _use_trailing = _get_strategy_main().use_trailing_stop
            r = await asyncio.get_event_loop().run_in_executor(
                None, lambda: trader.place_order_from_analysis(_early_analysis, model_id, trailing_on=_use_trailing)
            )
            _early_order_result.update(r)
            if r.get("ok"):
                _add_log(f"⚡ 早退下单成功: {r.get('message')}")
                try:
                    trader.save_order_record({
                        "time":      datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                        "model_id":  model_id,
                        "symbol":    symbol,
                        "direction": "买" if action == "做多" else "卖",
                        "offset":    "开仓",
                        "price":     r.get("entry_price"),
                        "volume":    config.DEFAULT_TRADE_VOLUME,
                        "filled":    config.DEFAULT_TRADE_VOLUME,
                        "status":    "已成交",
                        "order_id":  r.get("order_id", ""),
                    })
                except Exception as _oe:
                    logger.warning(f"早退保存订单记录失败: {_oe}")
                try:
                    _record_open_trade(model_id, symbol, {
                        "direction": "买" if action == "做多" else "卖",
                        "offset":    "开仓",
                        "price":     r.get("entry_price"),
                        "volume":    config.DEFAULT_TRADE_VOLUME,
                    })
                except Exception as _oe:
                    logger.warning(f"早退保存开仓成交记录失败: {_oe}")
                asyncio.create_task(_wechat_push(
                    f"{'做多' if action == '做多' else '做空'} {_sym_to_product(symbol)} @{r.get('entry_price')}\n"
                    f"模型: {model_id}"
                ))
            else:
                _add_log(f"⚡ 早退下单失败: {r.get('message')}", "WARNING")
        except Exception as _e:
            _add_log(f"⚡ 早退下单异常: {_e}", "ERROR")

    async def _on_early_guard(sl: float | None, tp: float | None) -> None:
        """AI 流中检测到止损/止盈价时立即补充守卫，无需等待完整回复"""
        nonlocal _early_guard_supplemented, _early_guard_partial
        if not _early_order_result.get("ok"):
            return
        if not (sl or tp):
            return
        # ── SL/TP 方向验证：写入守卫前过滤错误方向，避免立即误触发 ──
        _ep = _early_order_result.get("entry_price")
        if _ep and _early_action:
            _sl_ok = (not sl) or ((_early_action == "做多" and sl < _ep) or (_early_action == "做空" and sl > _ep))
            _tp_ok = (not tp) or ((_early_action == "做多" and tp > _ep) or (_early_action == "做空" and tp < _ep))
            if not _sl_ok and not _tp_ok:
                # 两者均错：立刻平仓，守卫保持 SL/TP=None 占位（不写入错误值）
                _early_guard_supplemented = True  # 阻止兜底写入错误 SL/TP
                logger.warning(
                    f"[早退双错平仓] {_early_action} 入场{_ep} SL{sl} TP{tp} 均方向错误，立刻平仓"
                )
                _add_log(f"⚡ 早退守卫双错，立刻平仓: SL={sl} TP={tp}", "WARNING")
                try:
                    await asyncio.get_event_loop().run_in_executor(
                        None, trader.close_position, symbol, model_id
                    )
                except Exception as _ce:
                    logger.warning(f"早退双错平仓异常: {_ce}")
                return
            # 单项错误：过滤掉错误方向的价格，避免误触发守卫
            if sl and not _sl_ok:
                logger.warning(f"[早退SL方向错误] {_early_action} 入场{_ep} SL{sl}→过滤，不写入守卫")
                sl = None
                _early_guard_partial = True  # 兜底阶段需补全缺失的 SL
            if tp and not _tp_ok:
                logger.warning(f"[早退TP方向错误] {_early_action} 入场{_ep} TP{tp}→过滤，不写入守卫")
                tp = None
                _early_guard_partial = True  # 兜底阶段需补全缺失的 TP
            if not sl and not tp:
                return  # 单项传入且方向错误（另一项为None），过滤后无有效值，无需更新守卫
        try:
            _updated = trader.update_guard(
                _make_key(symbol, model_id),
                stop_loss=sl,
                take_profit=tp,
                recalc_risk=True,
            )
            if _updated:
                _early_guard_supplemented = True
                _add_log(f"⚡ 早退守卫流式补充: SL={sl} TP={tp}")
            else:
                logger.warning(f"早退守卫流式补充失败: 未找到匹配守卫 key={_make_key(symbol, model_id)}")
        except Exception as _ge:
            logger.warning(f"早退守卫流式补充异常: {_ge}")

    _t_pre_sem = time.perf_counter()
    try:
        async with semaphore:
            _t_sem = time.perf_counter()
            logger.info(
                "[TIMING][%s][%s] fetch=%.2fs(calls=%d wait=%.2fs submit_gap=%.3fs queue=%.3fs exec=%.3fs resume_lag=%.3fs struct=%.1fms) pos=%.2fs pre_sem=%.2fs sem_wait=%.2fs",
                model_id, symbol,
                _t_fetch - _t_start,
                _fetch_calls,
                _fetch_pending_wait_total,
                _fetch_submit_gap_total,
                _fetch_queue_total,
                _fetch_exec_total,
                _fetch_resume_lag_total,
                _fetch_struct_total * 1000.0,
                _t_pos   - _t_fetch,
                _t_pre_sem - _t_pos,
                _t_sem   - _t_pre_sem,
            )
            analysis = await analyzers.analyze(
                model_id=model_id,
                market_data=market_data,
                history=c["history"],
                system_prompt=state["custom_prompt"] or None,
                positions=positions,
                on_early_signal=_on_early_signal if config.TQ_ENABLE_TRADING else None,
                on_early_guard=_on_early_guard if config.TQ_ENABLE_TRADING else None,
            )
    except Exception as _ana_e:
        _add_log(f"[{key}] 分析异常: {_ana_e}", "ERROR")
        c["_analyzing"] = False
        return

    c["analysis"] = analysis

    if not analysis.get("error"):
        action = analysis.get("操作建议", "?")

        # ── 开盘首根禁止开仓（代码层强制，9:00/21:00 开盘首根；午后开盘 13:30/13:00 除外）──
        if config.ENABLE_SKIP_FIRST_BAR and action in ("做多", "做空", "反手") and (market_data or {}).get("is_session_first_bar") and not config.is_noon_boundary(symbol):
            _add_log(f"⛔ 开盘首根拦截: {key} 强制观望（不允许首根开仓）", "WARNING")
            action = "观望"
            analysis["操作建议"] = "观望"

        # ── 尾盘禁止开仓（代码层强制，收盘前5分钟；午休前 11:25-11:30 除外）──
        if config.ENABLE_SKIP_LAST_BAR and action in ("做多", "做空", "反手") and (market_data or {}).get("is_session_last_bar") and not config.is_noon_boundary(symbol):
            _add_log(f"⛔ 尾盘拦截: {key} 强制观望（收盘前5分钟不允许开仓）", "WARNING")
            action = "观望"
            analysis["操作建议"] = "观望"

        # ── 反手操作禁用时降级为持有 ──
        if action == "反手" and not config.ENABLE_REVERSAL:
            _add_log(f"⛔ 反手已禁用: {key} 降级为持有", "WARNING")
            action = "持有"
            analysis["操作建议"] = "持有"

        # ── 尾盘有持仓强制平仓（代码层强制，收盘前5分钟）──
        if config.ENABLE_EOD_CLOSE and (market_data or {}).get("is_session_last_bar") and positions and action != "平仓":
            _add_log(f"⛔ 尾盘强制平仓: {key}（尾盘有持仓，强制平仓不留隔夜仓）", "WARNING")
            action = "平仓"
            analysis["操作建议"] = "平仓"
            _eod_suffix = " 【系统】尾盘有持仓，强制平仓不留隔夜仓。"
            analysis["核心逻辑"] = (analysis.get("核心逻辑") or "") + _eod_suffix

        # ── 守卫冷却期 + 价格校验（写入历史前拦截，确保 history 记录为实际决策）──
        # 冷却期：守卫触发后 10 分钟（2根K线）内，禁止任何开仓/反手
        # 价格校验：触发后 15 分钟（3根K线）内，禁止在比出场价更差的价位追入
        if action in ("做多", "做空", "反手"):
            _guard_rec = None
            for _r in reversed(c["history"]):
                if _r.get("操作建议") in ("守卫止盈", "守卫止损", "守卫平仓", "跟进不足退出", "尾盘强制平仓"):
                    _guard_rec = _r
                    break
            if _guard_rec:
                _blocked_reason = None
                _guard_is_breakeven = _guard_rec.get("_is_breakeven", False)
                try:
                    from datetime import datetime as _dt2
                    _exit_t = _dt2.strptime(_guard_rec["time"][:16], "%Y-%m-%d %H:%M")
                    _mins_since = (_dt2.now() - _exit_t).total_seconds() / 60
                    # 保本止损（_is_breakeven=True）跳过硬冷却期：
                    # 形态/趋势仍有效时应立即重入（Al Brooks 保本陷阱原则），无需等待
                    if 0 <= _mins_since <= 10 and not _guard_is_breakeven:
                        _gdir_cool = _guard_rec.get("_guard_direction")  # "LONG" / "SHORT"
                        _act_dir_cool = analysis.get("反手方向") if action == "反手" else action
                        if not _act_dir_cool:
                            _act_dir_cool = action  # 反手方向缺失，保守回退
                        # 只拦截与原持仓同方向的再入场，反方向信号不受冷却期限制
                        _same_dir_cool = (
                            (_gdir_cool == "LONG"  and _act_dir_cool == "做多") or
                            (_gdir_cool == "SHORT" and _act_dir_cool == "做空")
                        )
                        if _same_dir_cool or not _gdir_cool:
                            _blocked_reason = f"守卫冷却期({_mins_since:.0f}min内)"
                except Exception:
                    pass
                if not _blocked_reason:
                    try:
                        from datetime import datetime as _dt2
                        _exit_t2 = _dt2.strptime(_guard_rec["time"][:16], "%Y-%m-%d %H:%M")
                        _mins_since2 = (_dt2.now() - _exit_t2).total_seconds() / 60
                        _price_check_active = 0 <= _mins_since2 <= 15
                    except Exception:
                        _price_check_active = False
                    # 保本止损同样跳过追价拦截：入场价附近重入属于正常操作
                    if _price_check_active and not _guard_is_breakeven:
                        _ep   = _guard_rec.get("_exit_price")
                        _gdir = _guard_rec.get("_guard_direction")   # "LONG" / "SHORT"
                        _cur  = (market_data or {}).get("quote", {}).get("最新价")
                        _act  = analysis.get("反手方向") if action == "反手" else action
                        if not _act:
                            _act = action  # 反手方向缺失，保守回退
                        if _ep and _gdir and _cur:
                            if _gdir == "LONG" and _act == "做多" and _cur > _ep:
                                _blocked_reason = f"追价拦截(15min): 当前{_cur} > 出场价{_ep}，禁止追多"
                            elif _gdir == "SHORT" and _act == "做空" and _cur < _ep:
                                _blocked_reason = f"追价拦截(15min): 当前{_cur} < 出场价{_ep}，禁止追空"
                if _blocked_reason:
                    logger.warning(f"[{key}] {_blocked_reason}，action={action} → 强制观望")
                    _add_log(f"⛔ 守卫拦截: {_blocked_reason}", "WARNING")
                    action = "观望"
                    analysis["操作建议"] = "观望"

        # ── AI 主动平仓冷却期 + 价格校验（与守卫出场逻辑对齐）────────────────
        # 覆盖守卫检测的盲区：AI 主动说"平仓"后，同方向追入也须受到限制
        if action in ("做多", "做空", "反手"):
            _ai_close_rec = None
            for _r in reversed(c["history"]):
                if _r.get("操作建议") == "平仓" and _r.get("_exit_direction"):
                    _ai_close_rec = _r
                    break
            if _ai_close_rec:
                _ai_blocked = None
                try:
                    from datetime import datetime as _dt3
                    _ai_exit_t = _dt3.strptime(_ai_close_rec["time"][:16], "%Y-%m-%d %H:%M")
                    _ai_mins   = (_dt3.now() - _ai_exit_t).total_seconds() / 60
                    _ai_gdir   = _ai_close_rec.get("_exit_direction")   # "LONG" / "SHORT"
                    _ai_act    = analysis.get("反手方向") if action == "反手" else action
                    if not _ai_act:
                        _ai_act = action
                    _ai_same = (
                        (_ai_gdir == "LONG"  and _ai_act == "做多") or
                        (_ai_gdir == "SHORT" and _ai_act == "做空")
                    )
                    if _ai_same:
                        if 0 <= _ai_mins <= 10:
                            _ai_blocked = f"AI平仓冷却期({_ai_mins:.0f}min内)"
                        elif _ai_mins <= 15:
                            _ai_ep  = _ai_close_rec.get("_exit_price")
                            _ai_cur = (market_data or {}).get("quote", {}).get("最新价")
                            if _ai_ep and _ai_cur:
                                if _ai_gdir == "LONG"  and _ai_cur > _ai_ep:
                                    _ai_blocked = f"AI平仓追价拦截(15min): 当前{_ai_cur} > 出场价{_ai_ep}，禁止追多"
                                elif _ai_gdir == "SHORT" and _ai_cur < _ai_ep:
                                    _ai_blocked = f"AI平仓追价拦截(15min): 当前{_ai_cur} < 出场价{_ai_ep}，禁止追空"
                except Exception:
                    pass
                if _ai_blocked:
                    logger.warning(f"[{key}] {_ai_blocked}，action={action} → 强制观望")
                    _add_log(f"⛔ AI平仓拦截: {_ai_blocked}", "WARNING")
                    action = "观望"
                    analysis["操作建议"] = "观望"

        # 早退追价拦截：流式阶段已拦截，强制覆盖 action 为观望，保持历史记录一致
        if _early_intercepted and action in ("做多", "做空"):
            action = "观望"
            analysis["操作建议"] = "观望"

        # 持有时入场价替换为当前最新价（反映当前市场价，而非历史开仓均价）
        if action == "持有":
            _cur_price = (market_data or {}).get("quote", {}).get("最新价")
            if _cur_price is not None:
                analysis["入场价"] = _cur_price

        # summary 在守卫门之后构建，history 记录即为实际执行的操作（观望/做多/…）
        summary = analyzers.build_history_summary(analysis)
        summary["symbol"]       = symbol
        summary["model_id"]     = model_id
        summary["product_name"] = analysis.get("product_name", "")
        # 平仓时记录出场方向和价格，供下次分析的冷却期检测使用
        if action == "平仓" and positions:
            _pd = positions[0].get("direction", "")
            if _pd in ("多头", "空头"):
                summary["_exit_direction"] = "LONG" if _pd == "多头" else "SHORT"
            _ep_now = (market_data or {}).get("quote", {}).get("最新价")
            if _ep_now:
                summary["_exit_price"] = _ep_now
        c["history"].append(summary)
        if len(c["history"]) > 20:
            c["history"].pop(0)
        await _save_history_record(symbol, summary)
        _add_log(f"分析完成 [{model_id}] {symbol} → {action}")

        if config.TQ_ENABLE_TRADING:
            if action in ("做多", "做空"):
                if _early_order_result.get("ok"):
                    # 早退已成功下单；守卫通常已在流式阶段补充，此处仅作兜底
                    # _early_guard_partial=True 时流式有一方被过滤，需用完整 JSON 值补全缺失项
                    if not _early_guard_supplemented or _early_guard_partial:
                        _sl = trader.extract_price(analysis.get("止损价"))
                        _tp = trader.extract_price(analysis.get("止盈价"))
                        _ep2 = _early_order_result.get("entry_price")
                        # 方向验证：过滤完整 JSON 中仍然方向错误的值
                        if _ep2 and _early_action:
                            _sl_ok2 = (not _sl) or ((_early_action == "做多" and _sl < _ep2) or (_early_action == "做空" and _sl > _ep2))
                            _tp_ok2 = (not _tp) or ((_early_action == "做多" and _tp > _ep2) or (_early_action == "做空" and _tp < _ep2))
                            if _sl and not _sl_ok2:
                                logger.warning(f"[早退兜底SL方向错误] {_early_action} 入场{_ep2} SL{_sl}→过滤")
                                _sl = None
                            if _tp and not _tp_ok2:
                                logger.warning(f"[早退兜底TP方向错误] {_early_action} 入场{_ep2} TP{_tp}→过滤")
                                _tp = None
                            # 一方失效后用对方补充 1:1R
                            if _tp and not _sl:
                                _r = abs(_ep2 - _tp)
                                _sl = (_ep2 + _r) if _early_action == "做空" else (_ep2 - _r)
                                logger.warning(f"[早退兜底SL自动1:1R] {_early_action} 入场{_ep2} TP{_tp} R={_r:.4g} → SL={_sl:.4g}")
                            elif _sl and not _tp:
                                _r = abs(_ep2 - _sl)
                                _tp = (_ep2 - _r) if _early_action == "做空" else (_ep2 + _r)
                                logger.warning(f"[早退兜底TP自动1:1R] {_early_action} 入场{_ep2} SL{_sl} R={_r:.4g} → TP={_tp:.4g}")
                        if _sl or _tp:
                            try:
                                _updated = trader.update_guard(
                                    _make_key(symbol, model_id),
                                    stop_loss=_sl,
                                    take_profit=_tp,
                                    recalc_risk=True,
                                )
                                if _updated:
                                    _add_log(f"⚡ 早退守卫兜底补充: SL={_sl} TP={_tp}")
                                else:
                                    _add_log(f"⚡ 早退守卫补充失败: 未找到匹配守卫 key={_make_key(symbol, model_id)}", "WARNING")
                            except Exception as _ge:
                                logger.warning(f"早退守卫补充失败: {_ge}")
                else:
                    # 早退未触发或失败，走正常下单路径
                    try:
                        t0_ns = time.time_ns()
                        _use_trailing = _get_strategy_main().use_trailing_stop
                        r = await asyncio.get_event_loop().run_in_executor(
                            None, lambda: trader.place_order_from_analysis(analysis, model_id, trailing_on=_use_trailing)
                        )
                        _add_log(f"自动下单{'成功' if r.get('ok') else '失败'}: {r.get('message')}")
                        if r.get("ok"):
                            # 持久化订单记录（字段与前端渲染对齐）
                            try:
                                trader.save_order_record({
                                    "time":      datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                                    "model_id":  model_id,
                                    "symbol":    symbol,
                                    "direction": "买" if action == "做多" else "卖",
                                    "offset":    "开仓",
                                    "price":     r.get("entry_price"),
                                    "volume":    config.DEFAULT_TRADE_VOLUME,
                                    "filled":    config.DEFAULT_TRADE_VOLUME,
                                    "status":    "已成交",
                                    "order_id":  r.get("order_id", ""),
                                })
                            except Exception as oe:
                                logger.warning(f"保存订单记录失败: {oe}")
                            # 直接从下单结果构造开仓记录，不依赖 get_trades() 回报
                            try:
                                _record_open_trade(model_id, symbol, {
                                    "direction": "买" if action == "做多" else "卖",
                                    "offset":    "开仓",
                                    "price":     r.get("entry_price"),
                                    "volume":    config.DEFAULT_TRADE_VOLUME,
                                })
                            except Exception as oe:
                                logger.warning(f"保存开仓成交记录失败: {oe}")
                            asyncio.create_task(_wechat_push(
                                f"{'做多' if action == '做多' else '做空'} {_sym_to_product(symbol)} @{r.get('entry_price')}\n"
                                f"模型: {model_id}"
                            ))
                    except Exception as e:
                        _add_log(f"自动下单异常: {e}", "ERROR")
            elif action == "平仓":
                try:
                    # ── Bug-D 修复：平仓前预读开仓价缓存 ────────────────────
                    # close_position() 内部也会写缓存，但执行在 executor 线程，
                    # 平仓完成后持仓已消失，此处提前读一份交给补录协程使用。
                    _pre_cache: dict = {}
                    try:
                        _pre_actual = trader._resolve_symbol(symbol) or symbol
                        _pre_cache  = dict(trader._position_open_cache.get(_pre_actual, {}))
                    except Exception:
                        pass

                    r = await asyncio.get_event_loop().run_in_executor(
                        None, trader.close_position, symbol, model_id
                    )
                    if r.get("ok"):
                        state["sl_tp_monitor"].pop(symbol, None)
                    _add_log(f"自动平仓{'成功' if r.get('ok') else '失败'}: {r.get('message')}")

                    if r.get("ok"):
                        # ── Bug-D 修复：去掉立即兜底，改为单一补录路径 ──────
                        # 原逻辑：①立即 _record_trade_pnl（兜底）+ ②3秒后再次 _record_trade_pnl（补录）
                        # 问题：tqsdk 有回报时两条路径都执行，同一笔平仓被写入两次，
                        #       累计盈亏 _model_pnl_accum 翻倍。
                        # 修复：只走补录路径，内部先查 tqsdk 真实回报（路径1），
                        #       无回报时用缓存兜底计算（路径2），两条路径严格互斥。
                        #       与守卫平仓 _on_guard_close 的逻辑完全对齐。
                        t0_ns = time.time_ns() - 30_000_000_000  # 向前30秒覆盖时间戳偏差

                        async def _补录平仓收益(mid: str, sym: str, pre_cache: dict, t0: int):
                            await asyncio.sleep(2)
                            try:
                                actual   = trader._resolve_symbol(sym) or sym
                                sym_code = actual.split(".")[-1].rstrip("0123456789")
                                recorded = False
                                _check_count = 0
                                _max_checks  = 10  # 无撤单重下时约20s；有撤单重下时每轮约3s
                                while True:
                                    _check_count += 1

                                    # ── 路径1 & 2：并行读取成交回报和持仓状态 ──
                                    _trades_res, _pos_res = await asyncio.gather(
                                        asyncio.get_event_loop().run_in_executor(
                                            None, trader.get_trades, t0
                                        ),
                                        _get_positions_shared(),
                                        return_exceptions=True,
                                    )
                                    trades  = _trades_res if not isinstance(_trades_res, Exception) else []
                                    all_pos = _pos_res    if not isinstance(_pos_res,    Exception) else []

                                    # ── 路径1：有真实 tqsdk 成交回报，直接记录 ──
                                    recent = [
                                        t for t in trades
                                        if t.get("offset") != "开仓"
                                        and sym_code.lower() in t.get("symbol", "").lower()
                                        and t.get("close_profit") is not None
                                    ]
                                    if recent:
                                        for t in recent:
                                            _record_trade_pnl(mid, sym, t)
                                        logger.info(
                                            f"[平仓补录] 真实回报 {mid}/{sym}: "
                                            f"close_profit={recent[0].get('close_profit')}"
                                        )
                                        recorded = True
                                        break

                                    # ── 路径2：无回报，检查持仓是否已清零 ────────
                                    position_cleared = not any(
                                        sym_code.lower() in p.get("symbol", "").lower()
                                        for p in all_pos
                                    )

                                    # 超时兜底：10次检测后强制视为已平仓
                                    if not position_cleared and _check_count >= _max_checks:
                                        logger.warning(
                                            f"[平仓补录] 检测{_check_count}次未确认，"
                                            f"强制兜底: {mid}/{sym}"
                                        )
                                        position_cleared = True

                                    if position_cleared:
                                        # 持仓已清零，用缓存价格兜底
                                        close_price  = trader._get_last_price(sym) or 0.0
                                        open_price   = (pre_cache.get("open_price_long")
                                                        or pre_cache.get("open_price_short"))
                                        multiplier   = pre_cache.get("multiplier")
                                        close_profit = None
                                        if open_price and multiplier and close_price:
                                            if pre_cache.get("open_price_long"):
                                                close_profit = round(
                                                    (close_price - open_price)
                                                    * config.DEFAULT_TRADE_VOLUME * multiplier, 2
                                                )
                                            else:
                                                close_profit = round(
                                                    (open_price - close_price)
                                                    * config.DEFAULT_TRADE_VOLUME * multiplier, 2
                                                )
                                        if close_profit is not None:
                                            _record_trade_pnl(mid, sym, {
                                                "direction":    "卖" if pre_cache.get("open_price_long") else "买",
                                                "offset":       "平仓",
                                                "price":        close_price,
                                                "open_price":   open_price,
                                                "multiplier":   multiplier,
                                                "volume":       config.DEFAULT_TRADE_VOLUME,
                                                "close_profit": close_profit,
                                            })
                                            logger.info(
                                                f"[平仓补录] 兜底计算 {mid}/{sym}: "
                                                f"close_profit={close_profit}"
                                            )
                                            recorded = True
                                        else:
                                            logger.warning(
                                                f"[平仓补录] 兜底缺少开仓价或乘数，"
                                                f"PnL 无法计算: {mid}/{sym} cache={pre_cache}"
                                            )
                                        break

                                    # ── 路径3：持仓未清零，检查是否有未成交挂单 ──
                                    try:
                                        _cancel_r = await asyncio.get_event_loop().run_in_executor(
                                            None, trader.cancel_pending_orders, sym
                                        )
                                        if not _cancel_r.get("ok"):
                                            logger.warning(
                                                f"[平仓补录] 撤单异常: {_cancel_r.get('message')}: {mid}/{sym}"
                                            )
                                        _cancelled_n = len(_cancel_r.get("cancelled", []))
                                        if _cancelled_n > 0:
                                            # 检测到未成交挂单 → 撤掉后等1秒（给交易所时间确认撤单）再重下
                                            logger.info(
                                                f"[平仓补录] 检测到{_cancelled_n}笔未成交挂单，"
                                                f"撤单重下({_check_count}/{_max_checks}): {mid}/{sym}"
                                            )
                                            await asyncio.sleep(1)
                                            r_retry = await asyncio.get_event_loop().run_in_executor(
                                                None, trader.close_position, sym, mid
                                            )
                                            if not r_retry.get("ok"):
                                                _retry_msg = r_retry.get("message", "")
                                                if "暂无持仓" in _retry_msg:
                                                    logger.info(f"[平仓补录] 持仓已消失，停止重试: {mid}/{sym}")
                                                    break
                                                logger.warning(f"[平仓补录] 重下失败（{_retry_msg}）: {mid}/{sym}")
                                        else:
                                            logger.info(
                                                f"[平仓补录] 无挂单（已成交？），等待数据刷新"
                                                f"({_check_count}/{_max_checks}): {mid}/{sym}"
                                            )
                                    except Exception as _re:
                                        logger.warning(f"[平仓补录] 撤单重下异常: {mid}/{sym}: {_re}")
                                    await asyncio.sleep(2)

                                if not recorded:
                                    logger.warning(f"[平仓补录] 最终未能录入 PnL: {mid}/{sym}")

                                # ── 订单记录（平仓动作本身，不影响 PnL 统计）──
                                try:
                                    _close_price_rec = trader._get_last_price(sym) or 0.0
                                    _direction_rec   = "卖" if pre_cache.get("open_price_long") else "买"
                                    trader.save_order_record({
                                        "time":      datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                                        "model_id":  mid,
                                        "symbol":    sym,
                                        "direction": _direction_rec,
                                        "offset":    "平仓",
                                        "price":     _close_price_rec,
                                        "volume":    config.DEFAULT_TRADE_VOLUME,
                                        "filled":    config.DEFAULT_TRADE_VOLUME,
                                        "status":    "已成交",
                                    })
                                except Exception as oe:
                                    logger.warning(f"保存平仓订单记录失败: {oe}")

                            except Exception as e:
                                logger.warning(f"[平仓补录] 异常 {mid}/{sym}: {e}")

                        asyncio.ensure_future(
                            _补录平仓收益(model_id, symbol, _pre_cache, t0_ns)
                        )
                except Exception as e:
                    _add_log(f"自动平仓异常: {e}", "ERROR")
            elif action == "持有":
                ai_sl = trader.extract_price(analysis.get("止损价"))
                ai_tp = trader.extract_price(analysis.get("止盈价"))
                if ai_sl or ai_tp:
                    trader.update_guard(
                        key,
                        stop_loss   = ai_sl,
                        take_profit = ai_tp,
                        recalc_risk = False,  # 只移动止损位，保持原始追踪距离不变
                    )
            elif action == "反手":
                # 先平仓，再用对价反向开仓
                try:
                    t0_ns = time.time_ns()
                    r_close = await asyncio.get_event_loop().run_in_executor(
                        None, trader.close_position, symbol, model_id
                    )
                    if r_close.get("ok"):
                        state["sl_tp_monitor"].pop(symbol, None)
                        _add_log(f"反手：平仓成功，准备对价反向开仓")
                        # ── Bug-3 修复：删除立即写入路径，统一走补录（与常规平仓路径对齐）──
                        # 原代码在此立即调用 _record_trade_pnl，3秒后补录协程又再次调用，
                        # 导致同一笔平仓的 close_profit 被累计两次。
                        # 修复：只保存委托记录（供查询），PnL 全部由补录协程负责，
                        # 补录协程先查真实 tqsdk 回报（路径1），无回报时用缓存兜底（路径2）。
                        _pre_cache: dict = {}
                        try:
                            actual_sym  = trader._resolve_symbol(symbol) or symbol
                            close_price = trader._get_last_price(symbol) or 0.0
                            _pre_cache  = dict(trader._position_open_cache.get(actual_sym, {}))
                            trader.save_order_record({
                                "time":      datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                                "model_id":  model_id,
                                "symbol":    symbol,
                                "direction": "卖" if _pre_cache.get("open_price_long") else "买",
                                "offset":    "平仓",
                                "price":     close_price,
                                "volume":    config.DEFAULT_TRADE_VOLUME,
                                "filled":    config.DEFAULT_TRADE_VOLUME,
                                "status":    "已成交",
                            })
                        except Exception as e:
                            logger.warning(f"反手保存平仓记录失败: {e}")
                        # ── 补录真实 close_profit（路径1：tqsdk回报；路径2：缓存兜底）──
                        async def _补录反手平仓收益(mid, sym, pre_cache):
                            try:
                                await asyncio.sleep(3)
                                actual = trader._resolve_symbol(sym) or sym
                                sym_code = actual.split(".")[-1].rstrip("0123456789")
                                t0 = time.time_ns() - 30_000_000_000
                                trades = await asyncio.get_event_loop().run_in_executor(None, trader.get_trades, t0)
                                recent = [t for t in trades
                                          if t.get("offset") != "开仓"
                                          and sym_code.lower() in t.get("symbol", "").lower()
                                          and t.get("close_profit") is not None]
                                if recent:
                                    # 路径1：有真实成交回报
                                    _record_trade_pnl(mid, sym, recent[0])
                                elif pre_cache:
                                    # 路径2：无回报，用平仓前捕获的开仓价缓存兜底
                                    # 反手后立即有新仓，不能用"持仓是否清零"判断，直接兜底
                                    cp = trader._get_last_price(sym) or 0.0
                                    op = pre_cache.get("open_price_long") or pre_cache.get("open_price_short")
                                    ml = pre_cache.get("multiplier")
                                    if op and ml and cp:
                                        pnl = round(
                                            ((cp - op) if pre_cache.get("open_price_long") else (op - cp))
                                            * config.DEFAULT_TRADE_VOLUME * ml, 2
                                        )
                                        _record_trade_pnl(mid, sym, {
                                            "direction":    "卖" if pre_cache.get("open_price_long") else "买",
                                            "offset":       "平仓",
                                            "price":        cp,
                                            "open_price":   op,
                                            "multiplier":   ml,
                                            "volume":       config.DEFAULT_TRADE_VOLUME,
                                            "close_profit": pnl,
                                        })
                            except Exception:
                                pass
                        asyncio.ensure_future(_补录反手平仓收益(model_id, symbol, _pre_cache))
                        rev_dir_cn = analysis.get("反手方向", "做多")
                        new_dir    = "BUY" if rev_dir_cn == "做多" else "SELL"
                        sl_price = trader.extract_price(analysis.get("止损价"))
                        tp_price = trader.extract_price(analysis.get("止盈价"))
                        _use_trailing = _get_strategy_main().use_trailing_stop
                        r_open = await asyncio.get_event_loop().run_in_executor(
                            None, lambda: trader.reverse_order(symbol, new_dir, sl_price, tp_price, model_id, trailing_on=_use_trailing)
                        )
                        _add_log(f"反手：对价开仓{'成功' if r_open.get('ok') else '失败'}: {r_open.get('message')}")
                        if r_open.get("ok"):
                            # 持久化反手订单记录（字段与前端渲染对齐）
                            try:
                                trader.save_order_record({
                                    "time":      datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                                    "model_id":  model_id,
                                    "symbol":    symbol,
                                    "direction": "买" if new_dir == "BUY" else "卖",
                                    "offset":    "开仓",
                                    "price":     r_open.get("entry_price"),
                                    "volume":    config.DEFAULT_TRADE_VOLUME,
                                    "filled":    config.DEFAULT_TRADE_VOLUME,
                                    "status":    "已成交",
                                    "order_id":  r_open.get("order_id", ""),
                                })
                            except Exception as oe:
                                logger.warning(f"保存反手订单记录失败: {oe}")
                            # 直接从下单结果构造反手开仓记录，不依赖 get_trades() 回报
                            try:
                                _record_open_trade(model_id, symbol, {
                                    "direction": "买" if new_dir == "BUY" else "卖",
                                    "offset":    "开仓",
                                    "price":     r_open.get("entry_price"),
                                    "volume":    config.DEFAULT_TRADE_VOLUME,
                                })
                            except Exception as oe:
                                logger.warning(f"保存反手开仓记录失败: {oe}")
                    else:
                        _add_log(f"反手：平仓失败，取消反向开仓: {r_close.get('message')}", "WARNING")
                except Exception as e:
                    _add_log(f"反手操作异常: {e}", "ERROR")
    else:
        _add_log(f"AI 分析失败 [{model_id}] {symbol}: {analysis['error']}", "ERROR")
        # ── AI失败时的尾盘兜底平仓（无补录，边缘情况可接受）──
        if config.ENABLE_EOD_CLOSE and (market_data or {}).get("is_session_last_bar") and positions:
            _add_log(f"⏰ AI失败但尾盘有持仓，强制兜底平仓: {key}", "WARNING")
            try:
                _r = await asyncio.get_event_loop().run_in_executor(
                    None, trader.close_position, symbol, model_id
                )
                _add_log(f"尾盘兜底平仓{'成功' if _r.get('ok') else '失败'}: {_r.get('message')}")
                if _r.get("ok"):
                    state["sl_tp_monitor"].pop(symbol, None)
            except Exception as _eod_e:
                _add_log(f"尾盘兜底平仓异常: {_eod_e}", "ERROR")

    c["_analyzing"] = False
    _push_sse({**analysis, "__type": "analysis", "symbol": symbol, "model_id": model_id, "key": key})






DATA_DIR        = pathlib.Path(config.get_data_root())
DATA_DIR.mkdir(parents=True, exist_ok=True)
SESSION_FILE    = DATA_DIR / "session.json"   # 持久化合约列表
# 一次性迁移 session.json
_old_session = pathlib.Path("data/session.json")
if not SESSION_FILE.exists() and _old_session.exists():
    import shutil as _shutil
    _shutil.copy2(_old_session, SESSION_FILE)

def _save_session():
    """将当前合约列表和激活合约保存到 data/session.json"""
    try:
        data = {
            "active": state["active_symbol"],
            "contracts": [
                {"key": key, "symbol": _parse_key(key)[0], "model": _parse_key(key)[1]}
                for key in state["contracts"]
            ]
        }
        SESSION_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning(f"保存 session 失败: {e}")

def _load_session() -> tuple[list[dict], str]:
    """从 data/session.json 读取上次的合约列表，返回 ([{key, symbol, model}], active_key)"""
    if not SESSION_FILE.exists():
        return [], ""
    try:
        data = json.loads(SESSION_FILE.read_text(encoding="utf-8"))
        return data.get("contracts", []), data.get("active", "")
    except Exception as e:
        logger.warning(f"读取 session 失败: {e}")
        return [], ""

def _pnl_file(model_id: str) -> pathlib.Path:
    safe = model_id.replace("/", "_").replace("\\", "_").replace(":", "_")
    return DATA_DIR / f"equity_{safe}.jsonl"

def _save_equity_snapshot(snap: dict, model_id: str):
    """持久化写入 data/equity_<model>.jsonl"""
    f = _pnl_file(model_id)
    try:
        with open(f, "a", encoding="utf-8") as fp:
            fp.write(json.dumps(snap, ensure_ascii=False) + "\n")

    except Exception as e:
        logger.warning(f"写入权益文件失败 {f}: {e}")

MAX_EQUITY_RECORDS = 1000000   # 每个模型最多返回条数（基本无上限）

def _load_equity_curve(model_id: str = "") -> list[dict]:
    """读取指定模型的权益曲线；model_id为空则读全部模型合并"""
    if model_id:
        f = _pnl_file(model_id)
        if not f.exists():
            return []
        try:
            lines = f.read_text(encoding="utf-8").splitlines()
            result = []
            for line in lines[-MAX_EQUITY_RECORDS:]:
                if line.strip():
                    try: result.append(json.loads(line))
                    except: pass
            return result
        except Exception:
            return []
    else:
        # 合并所有模型
        all_rec: list[dict] = []
        for fp in DATA_DIR.glob("equity_*.jsonl"):
            mid = fp.stem.replace("equity_", "")
            try:
                for line in fp.read_text(encoding="utf-8").splitlines():
                    if line.strip():
                        try:
                            r = json.loads(line)
                            r.setdefault("model", mid)
                            all_rec.append(r)
                        except: pass
            except: pass
        all_rec.sort(key=lambda x: x.get("time", ""))
        return all_rec

def _list_model_ids_with_data() -> list[str]:
    return [fp.stem.replace("equity_", "") for fp in sorted(DATA_DIR.glob("equity_*.jsonl"))]

def _record_open_trade(model_id: str, symbol: str, trade: dict):
    """开仓成交后记录（type=open，不累计盈亏）"""
    snap = {
        "time":      datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "model":     model_id,
        "symbol":    symbol,
        "direction": trade.get("direction", ""),
        "offset":    trade.get("offset", ""),
        "price":     trade.get("price"),
        "volume":    trade.get("volume"),
        "type":      "open",
    }
    _save_equity_snapshot(snap, model_id)
    # 修复：开仓成交也写入 trades/YYYY-MM-DD.jsonl，与平仓记录统一
    try:
        trader.save_trade_record({k: v for k, v in snap.items()})
    except Exception as e:
        logger.warning(f"持久化开仓成交记录失败: {e}")

def _record_trade_pnl(model_id: str, symbol: str, trade: dict):
    """平仓成交后记录该模型收益（type=trade）"""
    close_profit = trade.get("close_profit") or 0.0
    snap = {
        "time":         datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "model":        model_id,
        "symbol":       symbol,
        "direction":    trade.get("direction", ""),
        "offset":       trade.get("offset", ""),
        "price":        trade.get("price"),
        "open_price":   trade.get("open_price"),
        "multiplier":   trade.get("multiplier"),
        "volume":       trade.get("volume"),
        "close_profit":  round(close_profit, 2),
        "trigger_type":  trade.get("trigger_type", ""),
        "type":          "trade",
    }
    _save_equity_snapshot(snap, model_id)
    try:
        trader.save_trade_record(snap)
    except Exception as e:
        logger.warning(f"持久化成交记录失败: {e}")


# ── K线收盘驱动的分析触发（回调方式）────────────────────────────────
# 由 data_feed._run_loop 在 K 线 datetime 变化时通过 call_soon_threadsafe 调用
# 相比时钟方式，触发时刻更精确：K 线真正收盘后才触发，不存在提前读到未完成 K 线的问题
def _on_kline_close(symbol: str):
    """K线收盘回调（在 asyncio 线程中执行）。
    data_feed 检测到指定品种新 K 线出现时调用此函数，触发该品种所有活跃模型的分析。
    """
    if data_feed._feed is None or not data_feed._feed.is_ready():
        return
    if not config.is_trading_time(symbol):
        return
    now_str = datetime.now().strftime("%H:%M:%S")
    triggered = []
    for key, c in list(state["contracts"].items()):
        if _parse_key(key)[0] != symbol:
            continue
        if not c.get("loop_running"):
            continue
        if c.get("_analyzing"):
            _add_log(f"[{key}] K线收盘跳过：上轮分析未完成", "WARNING")
            continue
        triggered.append(key)
    if triggered:
        for key in triggered:
            asyncio.create_task(_run_one_analysis(key))
        _add_log(f"[{now_str}] K线收盘触发 {symbol} → {len(triggered)} 个模型")



@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 50)
    logger.info("期货 AI 实时分析系统（多合约版）启动中...")
    for m in config.get_available_models():
        status = "✅" if m["enabled"] else "❌"
        logger.info(f"  {status} {m['id']:<8} {m['model_name']}")

    state["_semaphore"] = asyncio.Semaphore(state["max_concurrent"])
    global _market_data_executor
    if _market_data_executor is None:
        _market_data_executor = ThreadPoolExecutor(
            max_workers=_get_market_data_workers(),
            thread_name_prefix="mdata-fetch",
        )
        logger.info("行情抓取线程池已创建: workers=%s", _get_market_data_workers())
    # 在事件循环内创建文件写入锁（若已由 _save_history_record 提前创建则复用，不替换）
    global _file_lock
    if _file_lock is None:
        _file_lock = asyncio.Lock()


    feed = data_feed.init_feed()
    feed._loop = asyncio.get_event_loop()

    async def _on_guard_close(model_id: str, symbol: str, close_info: dict = None):
        """守卫触发平仓后，等待成交回报并将 PnL 记入对应 AI 收益曲线"""
        # 立即让持仓缓存失效，避免下次分析仍读到已平仓的旧持仓
        global _positions_cache_time
        _positions_cache_time = 0.0

        # t0_ns 向前30秒，避免 tqsdk 成交时间戳与服务器有微小偏差导致漏单
        t0_ns = time.time_ns() - 30_000_000_000

        # ── 守卫平仓事件注入历史记录（实时，不等待成交确认）──────────────────────────────
        # 目的：让 AI 在下次分析时感知到"刚刚在当前价格附近发生了止盈/止损"，
        # 避免在高位/低位被保本震出或止盈后立即追高/追低重新开仓。
        try:
            ep = close_info.get("entry_price") if close_info else None
            cp = close_info.get("close_price")  if close_info else None
            cdir = close_info.get("close_direction", "SELL") if close_info else "SELL"
            trigger_type = close_info.get("trigger_type", "") if close_info else ""

            # 判断平仓类型：优先使用 trigger_type（EOD/BAR_EXIT），其次按价格判断
            if trigger_type == "EOD":
                close_type = "尾盘强制平仓"
                is_breakeven = False
                cp = cp or 0
                logic = f"尾盘强制平仓，收盘前自动平仓，平仓价{cp}。下个交易时段若仍有高概率设置可重新入场。"
            elif trigger_type == "BAR_EXIT":
                close_type = "跟进不足退出"
                is_breakeven = False
                cp = cp or 0
                logic = f"跟进不足退出，开仓后{cp}附近价格未能有效推进，守卫提前平仓。等待更高质量的设置再入场。"
            elif ep and cp:
                if cdir == "SELL":
                    is_profit = cp > ep
                else:
                    is_profit = cp < ep
                close_type = "守卫止盈" if is_profit else "守卫止损"
                pnl_pts = round(abs(cp - ep), 1)
                # 保本止损：追踪止损已移至入场价，价格小幅穿越被震出；亏损极小（≤0.2%）
                is_breakeven = (not is_profit) and (abs(cp - ep) / ep <= 0.002)
                if is_breakeven:
                    logic = (
                        f"保本止损触发，开仓价{ep}，平仓价{cp}，亏损{pnl_pts}点（接近保本）。"
                        f"形态和趋势若仍有效，可立即在合理位置重新入场（Al Brooks 保本陷阱场景）。"
                    )
                else:
                    logic = (
                        f"{'追踪止损触发止盈' if is_profit else '止损触发平仓'}，"
                        f"开仓价{ep}，平仓价{cp}，{'盈利' if is_profit else '亏损'}{pnl_pts}点。"
                        f"当前价格{'处于本轮高位，追高需谨慎' if is_profit else '处于本轮低位，追空需谨慎'}。"
                        f"如需重新入场，须等待价格回调至合理位置后出现新的高概率设置。"
                    )
            else:
                close_type = "守卫平仓"
                is_breakeven = False
                cp = cp or 0
                logic = f"守卫自动平仓，平仓价{cp}。如需重新入场，须等待新的高概率设置出现。"

            guard_close_record = {
                "time":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "symbol":   symbol,
                "model_id": model_id,
                "操作建议": close_type,
                "入场价":   ep,
                "止盈价":   None,
                "止损价":   None,
                "核心逻辑": logic,
                "市场状态": "",
                "设置类型": "",
                "信号棒评级": "",
                "技术面":   "",
                "风险提示": "",
                "风险等级": "",
                "product_name": "",
                "_exit_price":      close_info.get("close_price") if close_info else None,
                "_guard_direction": close_info.get("guard_direction") if close_info else None,
                "_is_breakeven":    is_breakeven,
            }

            # 注入到内存 history（下次分析立即可见）
            key = _make_key(symbol, model_id)
            c = _get_contract(key)
            if c is not None:
                c["history"].append(guard_close_record)
                if len(c["history"]) > 20:
                    c["history"].pop(0)

            # 同时持久化到文件（重启后可恢复）
            await _save_history_record(symbol, guard_close_record)
            trigger_label = {"SL": "止损触发", "TP": "止盈触发", "EOD": "尾盘触发", "BAR_EXIT": "跟进不足"}.get(trigger_type, trigger_type)
            logger.info(f"[守卫] 平仓事件已注入历史记录 {model_id}/{symbol}: {close_type}（{trigger_label}）@{cp}")
            asyncio.create_task(_wechat_push(
                f"{close_type} {_sym_to_product(symbol)} @{cp}\n"
                f"开仓价: {ep}  模型: {model_id}"
            ))

        except Exception as e:
            logger.warning(f"[守卫] 历史记录注入失败 {model_id}/{symbol}: {e}")

        # 此函数由 _pending_close_orders 追踪确认持仓清零后才调用，无需再检查 position_cleared
        # 等待 tqsdk 成交回报（通常 1-2 秒；5秒覆盖高延迟场景）
        await asyncio.sleep(5)
        try:
            trades = await asyncio.get_event_loop().run_in_executor(None, trader.get_trades, t0_ns)
            _sym_raw = symbol or ""
            if "@" in _sym_raw:
                _sym_code = _sym_raw.split("@")[-1].split(".")[-1].lower()
            else:
                _base = _sym_raw.split(".")[-1] if "." in _sym_raw else _sym_raw
                _sym_code = "".join(c for c in _base if c.isalpha()).lower()
            recent = [t for t in trades
                      if t.get("offset") and t["offset"] != "开仓"
                      and t.get("close_profit") is not None
                      and (not _sym_code or _sym_code in t.get("symbol", "").lower())]
            if recent:
                for t in recent:
                    _record_trade_pnl(model_id, symbol, {**t, "trigger_type": trigger_type})
            elif close_info:
                # 兜底：tqsdk 未返回成交回报，用 close_info 估算（持仓已确认清零，安全写入）
                ep   = close_info.get("entry_price")
                cp   = close_info.get("close_price")
                vol  = close_info.get("volume", 1)
                mult = close_info.get("multiplier") or 1
                cdir = close_info.get("close_direction", "SELL")
                if ep and cp:
                    pnl = (cp - ep) * vol * mult if cdir == "SELL" else (ep - cp) * vol * mult
                    _record_trade_pnl(model_id, symbol, {
                        "direction":    "卖" if cdir == "SELL" else "买",
                        "offset":       "平仓",
                        "price":        cp,
                        "open_price":   ep,
                        "multiplier":   mult if mult > 0 else None,
                        "volume":       vol,
                        "close_profit": round(pnl, 2),
                        "trigger_type": trigger_type,
                    })
                    logger.info(f"[守卫] 兜底计算 PnL {model_id}/{symbol}: {round(pnl, 2)}")
                else:
                    logger.warning(f"[守卫] 兜底路径缺少 entry_price 或 close_price: {close_info}")
        except Exception as e:
            logger.warning(f"[守卫] PnL 归因失败 {model_id}/{symbol}: {e}")

    feed.on_guard_close = _on_guard_close

    async def _on_guard_update(model_id: str, symbol: str, guard: dict):
        """追踪止损移动时通过 SSE 推送最新 SL/TP 到前端"""
        key = _make_key(symbol, model_id)
        _push_sse({
            "__type":      "guard_update",
            "key":         key,
            "stop_loss":   guard.get("stop_loss"),
            "take_profit": guard.get("take_profit"),
            "entry_price": guard.get("entry_price"),
            "trailing_on": guard.get("trailing_on"),
            "peak_price":  guard.get("peak_price"),
        })

    feed.on_guard_update = _on_guard_update
    _add_log("天勤数据层初始化中...")

    async def _wait_tq():
        connected = await asyncio.get_event_loop().run_in_executor(
            None, feed.wait_ready, 30.0
        )
        if connected and feed.is_ready():
            _add_log("天勤连接成功 ✅")

        else:
            _add_log("天勤连接失败或超时", "ERROR")

    # 注册 K 线收盘回调：K 线真正收盘时由 data_feed 通过 call_soon_threadsafe 触发
    feed._kline_close_callbacks.append(_on_kline_close)

    # Bug5修复: 保存全局任务引用，关闭时可以正确 cancel
    _bg_tasks = [
        asyncio.create_task(_wait_tq()),
        asyncio.create_task(feed._flush_guards_loop()),
    ]

    def _on_market_update():
        for key, c in list(state["contracts"].items()):
            symbol, _ = _parse_key(key)
            try:
                fresh = data_feed.get_market_data(symbol)
            except Exception as e:
                # Bug11修复: 记录异常日志，方便排查行情停止更新的问题
                logger.debug(f"[{symbol}] 行情更新异常: {e}")
                continue
            if not fresh or fresh.get("pending") or fresh.get("error"):
                continue
            c["prices"] = fresh
            _push_sse(_market_sse_payload(fresh, symbol, key))

    if data_feed._feed:
        data_feed._feed._on_update_callbacks.append(_on_market_update)

    # 尝试从上次 session 恢复合约列表
    saved_contracts, saved_active = _load_session()
    if not saved_contracts:
        # 首次启动，使用默认合约
        default_sym = config.DEFAULT_SYMBOL
        default_mdl = config.get_default_model()
        saved_contracts = [{"key": _make_key(default_sym, default_mdl),
                            "symbol": default_sym, "model": default_mdl}]
        saved_active = saved_contracts[0]["key"]

    available_models = {m["id"] for m in config.get_available_models() if m["enabled"]}
    for entry in saved_contracts:
        sym, mdl, key = entry["symbol"], entry["model"], entry["key"]
        if mdl not in available_models:
            logger.warning(f"模型 {mdl} 不可用，跳过恢复 {key}")
            continue
        c = _make_contract(mdl, sym)
        if c["history"]:
            c["analysis"] = {**c["history"][-1], "symbol": sym, "_restored": True}
        state["contracts"][key] = c
        asyncio.create_task(_fetch_prices_only(key))    # ✅ 传 key
        logger.info(f"已恢复合约: {sym} [{mdl}]")

    if not state["contracts"]:
        # 所有模型均不可用时，仍加载默认合约（无模型）
        default_sym = config.DEFAULT_SYMBOL
        default_mdl = config.get_default_model()
        default_key = _make_key(default_sym, default_mdl)
        state["contracts"][default_key] = _make_contract(default_mdl, default_sym)
        saved_active = default_key
        asyncio.create_task(_fetch_prices_only(default_key))  # ✅ 传 key

    # 恢复激活合约
    if saved_active in state["contracts"]:
        state["active_symbol"] = saved_active
    else:
        state["active_symbol"] = list(state["contracts"].keys())[0]

    # 恢复止盈止损守卫（重启后不丢失）
    if data_feed._feed:
        trader._load_guards()

    logger.info(f"服务已启动，访问 http://localhost:{config.PORT}")
    logger.info("=" * 50)

    yield

    logger.info("服务关闭中...")
    _save_session()
    # Bug5修复: 取消全局后台任务
    for t in _bg_tasks:
        if t and not t.done():
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
    if _market_data_executor is not None:
        _market_data_executor.shutdown(wait=False, cancel_futures=True)
        _market_data_executor = None
    data_feed._feed and data_feed._feed.stop()
    logger.info("服务已停止")


app = FastAPI(title="期货 AI 实时分析系统（多合约版）", lifespan=lifespan)

from backtest.router import backtest_router
app.include_router(backtest_router)


class AddContractReq(BaseModel):
    symbol: str
    model:  str

class RemoveContractReq(BaseModel):
    symbol: str

class SetModelReq(BaseModel):
    symbol: str
    model:  str

class TriggerReq(BaseModel):
    symbol: str

class SetActiveReq(BaseModel):
    symbol: str

class LoopControlReq(BaseModel):
    symbol: str

class UpdatePromptRequest(BaseModel):
    prompt: str

class ClosePositionRequest(BaseModel):
    symbol: str

class CancelOrderRequest(BaseModel):
    order_id: str


@app.get("/equity", response_class=HTMLResponse)
async def equity_page():
    html_path = Path(__file__).parent / "equity.html"
    if not html_path.exists():
        return HTMLResponse("<h2>equity.html 未找到</h2>", status_code=404)
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    html_path = Path(__file__).parent / "index.html"
    if not html_path.exists():
        return HTMLResponse("<h2>index.html 未找到</h2>", status_code=404)
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.get("/api/stream/prices")
async def sse_prices():
    queue: asyncio.Queue = asyncio.Queue(maxsize=30)
    state["sse_clients"].add(queue)

    async def generator():
        try:
            # Bug8修复: 先复制快照，避免其他协程并发修改 contracts 字典触发异常
            for key, c in list(state["contracts"].items()):
                sym, _ = _parse_key(key)
                if c["prices"]:
                    yield f"data: {json.dumps({**c['prices'], 'symbol': sym, 'key': key}, ensure_ascii=False)}\n\n"
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=30)
                    yield f"data: {msg}\n\n"
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            state["sse_clients"].discard(queue)

    return StreamingResponse(
        generator(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                 "Access-Control-Allow-Origin": "*"},
    )


@app.get("/api/status")
async def get_status():
    contracts_info = {
        key: {"model": c["model"], "loop_running": c["loop_running"],
              "has_analysis": c["analysis"] is not None,
              "symbol": _parse_key(key)[0],
              "in_session": config.is_trading_time(_parse_key(key)[0])}
        for key, c in state["contracts"].items()
    }
    return JSONResponse({
        "contracts":        contracts_info,
        "active_symbol":    state["active_symbol"],
        "available_models": config.get_available_models(),
        "system_prompt":    state["custom_prompt"] or analyzers.DEFAULT_SYSTEM_PROMPT,
        "max_concurrent":   state["max_concurrent"],
        "logs":             state["logs"][-30:],
    })


@app.get("/api/summary")
async def get_summary():
    """所有合约汇总行情+分析，供总览表格使用，含持仓盈亏"""
    # 获取持仓数据
    pos_map: dict[str, dict] = {}
    if config.TQ_ENABLE_TRADING:
        try:
            positions = await asyncio.get_event_loop().run_in_executor(None, trader.get_positions)
            for p in positions:
                pos_map[p.get("symbol", "")] = p
        except Exception:
            pass

    def _match_pos(sym: str, pos_map: dict) -> dict:
        """主力连续 KQ.m@SHFE.rb → 匹配持仓 key SHFE.rb2505 等"""
        if sym in pos_map:
            return pos_map[sym]
        # 提取交易所和品种代码，模糊匹配
        import re
        m = re.search(r'@([A-Z]+)\.([a-zA-Z]+)', sym)
        if not m:
            return {}
        exch, code = m.group(1), m.group(2).lower()
        for key, val in pos_map.items():
            key_lower = key.lower()
            if exch.lower() in key_lower and key_lower.startswith(exch.lower()+'.'+code):
                return val
        return {}

    rows = []
    for key, c in state["contracts"].items():
        sym, mdl = _parse_key(key)
        p = c["prices"] or {}
        q = p.get("quote", {})
        a = c["analysis"] or {}
        pos = _match_pos(sym, pos_map)
        rows.append({
            "key":        key,
            "symbol":     sym,
            "model":      c["model"],
            "loop_on":    c["loop_running"],
            "price":      q.get("最新价"),
            "change_pct": q.get("涨跌幅%"),
            "action":     a.get("操作建议"),
            "entry":      a.get("入场价"),
            "tp":         a.get("止盈价"),
            "sl":         a.get("止损价"),
            "updated":    (a.get("timestamp") or "").split(" ")[-1] or None,
            "volume":     pos.get("volume"),
            "pnl":        pos.get("float_profit"),
            "open_price": pos.get("open_price"),
        })
    return JSONResponse({"rows": rows})


@app.get("/api/prices")
async def get_prices(symbol: str = ""):
    # symbol param can be raw symbol or key; extract symbol part
    raw = symbol or state["active_symbol"]
    sym = _parse_key(raw)[0] if "::" in raw else raw
    # find a contract with matching symbol
    c = None
    for key, cv in state["contracts"].items():
        if _parse_key(key)[0] == sym:
            c = cv
            break
    data = c["prices"] if c else None
    if data is None:
        return JSONResponse({"symbol": sym, "quote": {}, "ema20": {}, "klines": [], "error": "暂无行情"})
    return JSONResponse(data)


@app.get("/api/analysis")
async def get_analysis(symbol: str = ""):
    key = symbol or state["active_symbol"]
    c = _get_contract(key)
    if not c or not c.get("analysis"):
        return JSONResponse({"error": "暂无分析结果"})
    sym, mdl = _parse_key(key)
    return JSONResponse({**c["analysis"], "key": key, "model_id": mdl})


@app.get("/api/history")
async def get_history(symbol: str = "", limit: int = 100, all_symbols: bool = False, start_date: str = "", end_date: str = ""):
    if all_symbols:
        recs = _load_history(symbol="", limit=limit, start_date=start_date, end_date=end_date)
    else:
        key = symbol or state["active_symbol"]
        # 从 key 中提取合约符号和模型ID
        sym_part, model_part = _parse_key(key)
        # merge file history + in-memory (deduplicate by time+symbol)
        file_recs = _load_history(symbol=sym_part, model_id=model_part, limit=limit, start_date=start_date, end_date=end_date)
        c = _get_contract(key)
        mem_recs  = c["history"] if c else []
        # 对内存记录也应用时间筛选
        if start_date or end_date:
            mem_recs = [r for r in mem_recs if _passes_date_filter(r, start_date, end_date)]
        # file already has most recent first; merge unique by time
        seen = {r.get("time") for r in file_recs}
        extra = [r for r in mem_recs if r.get("time") not in seen]
        recs = (extra + file_recs)[:limit]
    return JSONResponse({"records": recs})


@app.get("/api/orders")
async def get_orders(symbol: str = "", limit: int = 100, start_date: str = "", end_date: str = "", model_id: str = ""):
    """获取订单历史记录"""
    # symbol 参数可以是完整 key (sym::model) 或仅合约符号
    if "::" in symbol:
        sym_part, model_part = _parse_key(symbol)
        # 如果未单独提供 model_id，则使用 key 中的模型部分
        if not model_id:
            model_id = model_part
        symbol = sym_part
    records = trader.load_orders(symbol=symbol, limit=limit, start_date=start_date, end_date=end_date, model_id=model_id)
    return JSONResponse({"records": records})


@app.get("/api/trades")
async def get_trades(symbol: str = "", limit: int = 100, start_date: str = "", end_date: str = "", model_id: str = ""):
    """获取成交历史记录"""
    if "::" in symbol:
        sym_part, model_part = _parse_key(symbol)
        if not model_id:
            model_id = model_part
        symbol = sym_part
    records = trader.load_trades(symbol=symbol, limit=limit, start_date=start_date, end_date=end_date, model_id=model_id)
    return JSONResponse({"records": records})


@app.post("/api/contracts/add")
async def add_contract(req: AddContractReq):
    symbol = req.symbol.strip()
    model  = req.model.strip()
    if not symbol:
        raise HTTPException(status_code=400, detail="合约代码不能为空")
    available = [m["id"] for m in config.get_available_models() if m["enabled"]]
    if model not in available:
        raise HTTPException(status_code=400, detail=f"模型 {model} 不可用")
    key = _make_key(symbol, model)
    if key in state["contracts"]:
        return JSONResponse({"ok": True, "message": f"{symbol}[{model}] 已存在"})
    state["contracts"][key] = _make_contract(model, symbol)
    # Restore last analysis so UI shows previous result immediately
    c_new = state["contracts"][key]
    if c_new["history"]:
        c_new["analysis"] = {**c_new["history"][-1], "symbol": symbol, "_restored": True}
    state["active_symbol"] = key
    asyncio.create_task(_fetch_prices_only(key))
    _add_log(f"已添加合约: {symbol} [{model}]")
    _save_session()
    return JSONResponse({"ok": True, "message": f"已添加 {symbol} [{model}]"})


@app.post("/api/contracts/remove")
async def remove_contract(req: RemoveContractReq):
    key = req.symbol   # frontend sends full key sym::model
    c = _get_contract(key)
    if c is None:
        raise HTTPException(status_code=404, detail="合约不存在")
    del state["contracts"][key]
    if state["active_symbol"] == key:
        remaining = list(state["contracts"].keys())
        state["active_symbol"] = remaining[0] if remaining else ""
    sym, mdl = _parse_key(key)
    _add_log(f"已移除合约: {sym} [{mdl}]")
    _save_session()
    return JSONResponse({"ok": True})


@app.post("/api/contracts/set_model")
async def set_contract_model(req: SetModelReq):
    old_key = req.symbol  # frontend passes full key: sym::old_model
    c = _require_contract(old_key)
    available = [m["id"] for m in config.get_available_models() if m["enabled"]]
    if req.model not in available:
        raise HTTPException(status_code=400, detail=f"模型 {req.model} 不可用")
    sym, old_model = _parse_key(old_key)
    if old_model == req.model:
        return JSONResponse({"ok": True, "new_key": old_key})
    new_key = _make_key(sym, req.model)
    if new_key in state["contracts"]:
        raise HTTPException(status_code=400, detail=f"合约 {new_key} 已存在")
    c["model"] = req.model
    # 迁移到新 key
    state["contracts"][new_key] = c
    del state["contracts"][old_key]
    if state.get("active_symbol") == old_key:
        state["active_symbol"] = new_key
    _add_log(f"[{sym}] 模型: {old_model} → {req.model}")
    _save_session()
    return JSONResponse({"ok": True, "new_key": new_key})


@app.post("/api/set_active")
async def set_active(req: SetActiveReq):
    key = req.symbol  # frontend passes key
    if key not in state["contracts"]:
        raise HTTPException(status_code=404, detail="合约未添加")
    state["active_symbol"] = key
    return JSONResponse({"ok": True})


@app.post("/api/trigger")
async def trigger_analysis(req: TriggerReq):
    key = req.symbol.strip()  # frontend passes key
    _require_contract(key)
    asyncio.create_task(_run_one_analysis(key))
    sym, mdl = _parse_key(key)
    _add_log(f"[{sym}][{mdl}] 已触发分析")
    return JSONResponse({"ok": True, "message": f"[{key}] 已触发"})


@app.post("/api/start")
async def start_loop(req: LoopControlReq):
    key = req.symbol  # frontend passes key
    c = _require_contract(key)
    sym, _ = _parse_key(key)
    c["loop_running"] = True
    _add_log(f"[{key}] 分析已启用")
    _push_sse({"__type": "loop_change", "key": key, "symbol": sym, "loop_on": True})
    return JSONResponse({"ok": True})


@app.post("/api/stop")
async def stop_loop(req: LoopControlReq):
    key = req.symbol  # frontend passes key
    c = _require_contract(key)
    sym, _ = _parse_key(key)
    c["loop_running"] = False
    _add_log(f"[{key}] 分析已暂停")
    _push_sse({"__type": "loop_change", "key": key, "symbol": sym, "loop_on": False})
    return JSONResponse({"ok": True})


@app.get("/api/trading")
async def get_trading():
    if not config.TQ_ENABLE_TRADING:
        return JSONResponse({"enabled": False, "account": {}, "positions": [], "orders": [], "trades": []})
    try:
        account   = await asyncio.get_event_loop().run_in_executor(None, trader.get_account_info)
        positions = await asyncio.get_event_loop().run_in_executor(None, trader.get_positions)
        orders    = await asyncio.get_event_loop().run_in_executor(None, trader.get_orders)
        trades    = await asyncio.get_event_loop().run_in_executor(None, trader.get_trades)
        return JSONResponse({"enabled": True, "account": account, "positions": positions, "orders": orders, "trades": trades})
    except Exception as e:
        return JSONResponse({"enabled": True, "account": {"error": str(e)}, "positions": [], "orders": []})


@app.post("/api/close_position")
async def do_close_position(req: ClosePositionRequest):
    if not config.TQ_ENABLE_TRADING:
        return JSONResponse({"ok": False, "message": "模拟交易未启用"})
    try:
        t0_ns = time.time_ns()
        # 从当前 contracts 找对应 model_id（key 格式为 sym::model，contract dict 里用 "model" 键）
        model_id = ""
        for key, c in state.get("contracts", {}).items():
            sym_k, mdl_k = _parse_key(key)
            if sym_k == req.symbol:
                model_id = mdl_k
                break
        r = await asyncio.get_event_loop().run_in_executor(None, trader.close_position, req.symbol, model_id)
        if r.get("ok"):
            _add_log(f"平仓成功: {req.symbol}")
            try:
                await asyncio.sleep(2)
                trades = await asyncio.get_event_loop().run_in_executor(None, trader.get_trades, t0_ns)
                for t in trades:
                    if t.get("offset") and t["offset"] != "开仓" and t.get("close_profit") is not None:
                        _record_trade_pnl(model_id or "manual", req.symbol, t)
            except Exception:
                pass
        return JSONResponse(r)
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)})


@app.post("/api/cancel_order")
async def do_cancel_order(req: CancelOrderRequest):
    if not config.TQ_ENABLE_TRADING:
        return JSONResponse({"ok": False, "message": "模拟交易未启用"})
    try:
        r = await asyncio.get_event_loop().run_in_executor(None, trader.cancel_order, req.order_id)
        return JSONResponse(r)
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)})


@app.post("/api/update_prompt")
async def update_prompt(req: UpdatePromptRequest):
    state["custom_prompt"] = req.prompt or None
    _add_log("System Prompt 已更新")
    return JSONResponse({"ok": True})


@app.get("/api/settings")
async def get_settings():
    import config as _c
    def mask(v):
        if not v: return ""
        return v[:6] + "…" + v[-4:] if len(v) > 12 else "****"
    return JSONResponse({
        "tq_username":       _c.TQ_USERNAME,
        "tq_password":       mask(_c.TQ_PASSWORD),
        "tq_enable_trading": _c.TQ_ENABLE_TRADING,
        "tq_account_mode":   _c.TQ_ACCOUNT_MODE,
        "tq_broker":         _c.TQ_BROKER,
        "tq_account_id":     _c.TQ_ACCOUNT_ID,
        "tq_trade_password": mask(_c.TQ_TRADE_PASSWORD),
        "default_volume":    _c.DEFAULT_TRADE_VOLUME,
        "max_concurrent":    state["max_concurrent"],
        "anthropic_key":          mask(_c.ANTHROPIC_API_KEY),
        "anthropic_model_name":   _c.ANTHROPIC_MODEL_NAME,
        "openai_key":             mask(_c.OPENAI_API_KEY),
        "openai_base_url":        _c.OPENAI_BASE_URL,
        "openai_model_name":      _c.OPENAI_MODEL_NAME,
        "deepseek_key":           mask(_c.DEEPSEEK_API_KEY),
        "deepseek_base_url":      _c.DEEPSEEK_BASE_URL,
        "deepseek_model_name":    _c.DEEPSEEK_MODEL_NAME,
        "grok_key":               mask(_c.GROK_API_KEY),
        "grok_base_url":          _c.GROK_BASE_URL,
        "grok_model_name":        _c.GROK_MODEL_NAME,
        "qwen_key":               mask(_c.QWEN_API_KEY),
        "qwen_base_url":          _c.QWEN_BASE_URL,
        "qwen_model_name":        _c.QWEN3_MODEL_NAME,
        "gemini_key":             mask(_c.GEMINI_API_KEY),
        "gemini_base_url":        _c.GEMINI_BASE_URL,
        "gemini_model_name":      _c.GEMINI_MODEL_NAME,
        "openrouter_key":         mask(_c.OPENROUTER_API_KEY),
        "openrouter_base_url":    _c.OPENROUTER_BASE_URL,
        "openrouter_model_name":  _c.OPENROUTER_MODEL_NAME,
        "nvidia_key":             mask(_c.NVIDIA_API_KEY),
        "nvidia_base_url":        _c.NVIDIA_BASE_URL,
        "nvidia_model_name":      _c.NVIDIA_MODEL_NAME,
        "enable_trailing_stop":    _c.ENABLE_TRAILING_STOP,
        "enable_tp_awareness":     _c.ENABLE_TP_AWARENESS,
        "enable_bar_exit":         _c.ENABLE_BAR_EXIT,
        "enable_eod_close":        _c.ENABLE_EOD_CLOSE,
        "enable_skip_first_bar":   _c.ENABLE_SKIP_FIRST_BAR,
        "enable_skip_last_bar":    _c.ENABLE_SKIP_LAST_BAR,
        "enable_reversal":         _c.ENABLE_REVERSAL,
        "enable_skip_narrow":      _c.ENABLE_SKIP_NARROW,
        "enable_skip_oscillation": _c.ENABLE_SKIP_OSCILLATION,
        "enable_wechat_push":      _c.ENABLE_WECHAT_PUSH,
    })


@app.post("/api/settings")
async def save_settings(request: Request):
    body = await request.json()

    field_map = {
        "tq_username": "TQ_USERNAME", "tq_password": "TQ_PASSWORD",
        "tq_enable_trading": "TQ_ENABLE_TRADING",
        "tq_account_mode": "TQ_ACCOUNT_MODE",
        "tq_broker": "TQ_BROKER", "tq_account_id": "TQ_ACCOUNT_ID",
        "tq_trade_password": "TQ_TRADE_PASSWORD",
        "default_volume": "DEFAULT_TRADE_VOLUME",
        "max_concurrent": "MAX_CONCURRENT",
        "anthropic_key": "ANTHROPIC_API_KEY", "anthropic_model_name": "ANTHROPIC_MODEL_NAME",
        "openai_key": "OPENAI_API_KEY", "openai_base_url": "OPENAI_BASE_URL", "openai_model_name": "OPENAI_MODEL_NAME",
        "deepseek_key": "DEEPSEEK_API_KEY", "deepseek_base_url": "DEEPSEEK_BASE_URL", "deepseek_model_name": "DEEPSEEK_MODEL_NAME",
        "grok_key": "GROK_API_KEY", "grok_base_url": "GROK_BASE_URL", "grok_model_name": "GROK_MODEL_NAME",
        "qwen_key": "QWEN_API_KEY", "qwen_base_url": "QWEN_BASE_URL", "qwen_model_name": "QWEN_MODEL_NAME",
        "gemini_key": "GEMINI_API_KEY", "gemini_base_url": "GEMINI_BASE_URL", "gemini_model_name": "GEMINI_MODEL_NAME",
        "openrouter_key": "OPENROUTER_API_KEY", "openrouter_base_url": "OPENROUTER_BASE_URL", "openrouter_model_name": "OPENROUTER_MODEL_NAME",
        "nvidia_key": "NVIDIA_API_KEY", "nvidia_base_url": "NVIDIA_BASE_URL", "nvidia_model_name": "NVIDIA_MODEL_NAME",
        "enable_trailing_stop":    "ENABLE_TRAILING_STOP",
        "enable_tp_awareness":     "ENABLE_TP_AWARENESS",
        "enable_bar_exit":         "ENABLE_BAR_EXIT",
        "enable_eod_close":        "ENABLE_EOD_CLOSE",
        "enable_skip_first_bar":   "ENABLE_SKIP_FIRST_BAR",
        "enable_skip_last_bar":    "ENABLE_SKIP_LAST_BAR",
        "enable_reversal":         "ENABLE_REVERSAL",
        "enable_skip_narrow":      "ENABLE_SKIP_NARROW",
        "enable_skip_oscillation": "ENABLE_SKIP_OSCILLATION",
        "enable_wechat_push":      "ENABLE_WECHAT_PUSH",
    }

    if "max_concurrent" in body:
        try:
            mc = max(1, min(10, int(body["max_concurrent"])))
            state["max_concurrent"] = mc
            state["_semaphore"] = asyncio.Semaphore(mc)
            _add_log(f"AI 并发限制已更新为 {mc}")
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="max_concurrent 必须为 1-10 之间的整数")

    env_path = pathlib.Path(".env")
    env: dict[str, str] = {}
    if env_path.exists():
        for ln in env_path.read_text(encoding="utf-8").splitlines():
            ln = ln.strip()
            if ln and not ln.startswith("#") and "=" in ln:
                k, _, v = ln.partition("=")
                env[k.strip()] = v.strip()

    changed = []
    for fk, ek in field_map.items():
        val = body.get(fk)
        if val is None:
            continue
        sv = str(val).strip()
        if "…" in sv or sv == "****":
            continue
        if isinstance(val, bool):
            sv = "true" if val else "false"
        if sv == "" and fk.endswith("_key"):
            continue
        env[ek] = sv
        changed.append(ek)

    raw_lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    written: set[str] = set()
    out: list[str] = []
    for ln in raw_lines:
        s = ln.strip()
        if s and not s.startswith("#") and "=" in s:
            k = s.split("=", 1)[0].strip()
            if k in env:
                out.append(f"{k}={env[k]}")
                written.add(k)
                continue
        out.append(ln)
    for k, v in env.items():
        if k not in written:
            out.append(f"{k}={v}")

    env_path.write_text("\n".join(out) + "\n", encoding="utf-8")

    import importlib, config as _cfg
    for k, v in env.items():
        os.environ[k] = v
    importlib.reload(_cfg)

    msg = f"已保存 {len(changed)} 项" if changed else "无变更"
    _add_log(f"设置已保存 — {msg}")
    # 检查是否修改了需要重启才能生效的关键字段
    restart_keys = {"TQ_ACCOUNT_MODE", "TQ_ENABLE_TRADING", "TQ_BROKER",
                    "TQ_ACCOUNT_ID", "TQ_TRADE_PASSWORD", "TQ_USERNAME", "TQ_PASSWORD"}
    needs_restart = bool(restart_keys & set(changed))
    return JSONResponse({"ok": True, "message": msg, "needs_restart": needs_restart})






@app.get("/api/equity_curve")
async def get_equity_curve(model: str = ""):
    """历史权益曲线，按 model 筛选或返回全部"""
    file_data = _load_equity_curve(model_id=model)
    models_available = _list_model_ids_with_data()
    return JSONResponse({"curve": file_data, "models": models_available})

@app.patch("/api/guard")
async def patch_guard(request: Request):
    """手动调整守卫的止损/止盈/追踪开关"""
    body = await request.json()
    key        = body.get("key", "")
    results = trader.update_guard(
        key,
        stop_loss   = body.get("stop_loss"),
        take_profit = body.get("take_profit"),
        trailing_on = body.get("trailing_on"),
    )
    if not results:
        raise HTTPException(status_code=404, detail=f"守卫未找到: {key}")
    for item in results:
        g = item["guard"]
        sse_key = _make_key(g["symbol"], g["model_id"])
        _push_sse({
            "__type":      "guard_update",
            "key":         sse_key,
            "stop_loss":   g.get("stop_loss"),
            "take_profit": g.get("take_profit"),
            "entry_price": g.get("entry_price"),
            "trailing_on": g.get("trailing_on"),
            "peak_price":  g.get("peak_price"),
        })
    return JSONResponse({"ok": True, "updated": results})


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=config.PORT, reload=False, log_level="info")
