"""
trader.py — 交易层

支持模拟账户（TqKq）与实盘账户，账户模式由 TQ_ACCOUNT_MODE 配置。
TqApi 实例由 data_feed.TqFeed 统一管理，此模块通过 _feed 访问。

关键约束：
  - insert_order / cancel_order 必须在 tqsdk 线程中调用
  - 通过 _pending_orders 队列传递指令，由 _run_loop 处理
  - 读取账户/持仓数据可以从任意线程直接读引用对象
"""

import re
import json
import pathlib
import threading
import logging
from datetime import date, datetime, timedelta
from typing import Any

import numpy as np

import data_feed
import config

logger = logging.getLogger(__name__)

# 订单/成交历史持久化
_orders_file_lock = threading.Lock()
_trades_file_lock = threading.Lock()

# 平仓前捕获的持仓开仓价缓存，用于补全 tqsdk 未提供的 close_profit
# key: actual_instrument_id → {open_price_long, open_price_short, multiplier}
# 持久化到 data/open_cache.json，重启后可恢复（修复：重启期间持仓的平仓收益计算）
_open_cache_lock = threading.Lock()

def _open_cache_file() -> pathlib.Path:
    p = pathlib.Path(config.get_data_root()) / "open_cache.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def _load_open_cache() -> dict:
    f = _open_cache_file()
    if not f.exists():
        return {}
    try:
        return json.loads(f.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_open_cache(cache: dict):
    try:
        with _open_cache_lock:
            _open_cache_file().write_text(
                json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
            )
    except Exception as e:
        logger.warning(f"保存开仓价缓存失败: {e}")

_position_open_cache: dict[str, dict] = _load_open_cache()


# ─────────────────────────────────────────────────────────────
# 内部工具
# ─────────────────────────────────────────────────────────────

def _feed():
    f = data_feed._feed
    if f is None or not f.is_ready():
        raise RuntimeError("天勤未连接")
    return f


def _queue_op(op_type: str, timeout: float = 10.0, **kwargs) -> dict:
    """
    将交易指令放入 tqsdk 线程队列，阻塞等待执行结果。
    insert_order / cancel_order 都通过此路径执行。
    """
    event = threading.Event()
    req = {"type": op_type, "kwargs": kwargs, "result": None, "event": event}
    feed = _feed()
    with feed._lock:
        feed._pending_orders.append(req)
    executed = event.wait(timeout=timeout)
    if not executed:
        return {"ok": False, "message": "交易指令超时，tqsdk 线程可能未响应"}
    return req["result"]


def extract_price(price_str: str, fallback: float = None) -> float | None:
    """从 AI 返回的价格字符串中提取数字，如 '等待回调至3820' → 3820.0"""
    if price_str is None:
        return fallback
    try:
        return float(str(price_str).strip())
    except ValueError:
        pass
    nums = re.findall(r'\d+\.?\d*', str(price_str))
    return float(nums[0]) if nums else fallback


def _resolve_symbol(symbol: str) -> str | None:
    """
    主连格式 KQ.m@SHFE.rb → 读取 underlying_symbol 获得实际合约
    具体合约 SHFE.rb2605 → 直接返回
    """
    if "@" not in symbol:
        return symbol
    try:
        feed = data_feed._feed
        if feed and symbol in feed._quotes:
            u = feed._quotes[symbol].underlying_symbol
            return u if u is not None and u != "" else None
    except Exception:
        pass
    return None


def _get_price_tick(actual_symbol: str) -> float:
    """获取合约最小变动价位，获取失败时返回 1.0"""
    try:
        feed = data_feed._feed
        if feed:
            # 优先直接匹配（实际合约已订阅的情况）
            q = feed._quotes.get(actual_symbol)
            if q is None:
                # 实际合约不在 _quotes（key 为主连格式），按 underlying_symbol 反向查找
                for qval in feed._quotes.values():
                    if getattr(qval, 'underlying_symbol', None) == actual_symbol:
                        q = qval
                        break
            if q:
                pt = float(getattr(q, 'price_tick', 0) or 0)
                if pt > 0:
                    return pt
    except Exception:
        pass
    return 1.0


def _round_to_tick(price: float, tick: float) -> float:
    """将价格对齐到最小变动价位的整数倍"""
    import math
    # 用 round 避免浮点精度问题
    tick_digits = max(0, -int(math.floor(math.log10(tick)))) if tick < 1 else 0
    rounded = round(round(price / tick) * tick, tick_digits + 2)
    return rounded


def _close_offset(actual_symbol: str, is_today: bool) -> str:
    """
    返回正确的平仓 offset。
    只有 SHFE（上期所）和 INE（上期能源）支持 CLOSETODAY/CLOSE 区分。
    DCE/CZCE/CFFEX 不支持 CLOSETODAY，实盘下发送会被交易所拒单。
    """
    if not is_today:
        return "CLOSE"
    exchange = actual_symbol.split(".")[0] if "." in actual_symbol else ""
    return "CLOSETODAY" if exchange in ("SHFE", "INE") else "CLOSE"


def _get_last_price(symbol: str) -> float | None:
    """获取最新价（自动处理主连格式）"""
    import numpy as np
    try:
        feed = data_feed._feed
        if feed is None:
            return None
        for sym in [_resolve_symbol(symbol), symbol]:
            if sym and sym in feed._quotes:
                p = float(feed._quotes[sym].last_price)
                if not np.isnan(p):
                    return p
                p = float(feed._quotes[sym].pre_close)
                if not np.isnan(p):
                    return p
        # 兜底：手动平仓时传入的是实际合约(如SHFE.rb2605)，
        # 但 _quotes 里只有主连(KQ.m@SHFE.rb)——遍历找 underlying_symbol 匹配的主连
        for qs_sym, quote in feed._quotes.items():
            if "@" not in qs_sym:
                continue
            try:
                if quote.underlying_symbol == symbol:
                    p = float(quote.last_price)
                    if not np.isnan(p):
                        return p
                    p = float(quote.pre_close)
                    if not np.isnan(p):
                        return p
            except Exception:
                pass
    except Exception:
        pass
    return None


def _offset_cn(offset: str, symbol: str = "") -> str:
    if offset == "OPEN":
        return "开仓"
    if offset == "CLOSETODAY":
        return "平今"
    if offset == "CLOSE":
        exchange = symbol.split(".")[0] if "." in symbol else ""
        return "平昨" if exchange in ("SHFE", "INE") else "平仓"
    return offset


def _status_cn(status: str) -> str:
    return {"ALIVE": "未成交", "FINISHED": "已成交"}.get(status, status)


def _fmt_ns(ns) -> str:
    try:
        from datetime import datetime
        return datetime.fromtimestamp(int(ns) / 1e9).strftime("%H:%M:%S")
    except Exception:
        return ""


# ─────────────────────────────────────────────────────────────
# 账户 / 持仓 / 委托查询（只读，任意线程可调用）
# ─────────────────────────────────────────────────────────────

def get_account_info() -> dict:
    """账户资金：权益、可用、保证金、风险度"""
    try:
        import numpy as np
        api = _feed()._api
        acc = api.get_account()

        def fv(v):
            f = float(v)
            return round(f, 2) if not np.isnan(f) else 0.0

        return {
            "balance":    fv(acc.balance),
            "available":  fv(acc.available),
            "margin":     fv(acc.margin),
            "float_profit": fv(acc.float_profit) if hasattr(acc, "float_profit") else 0.0,
            "risk_ratio": round(float(acc.risk_ratio) * 100, 2)
                          if hasattr(acc, "risk_ratio") and not np.isnan(acc.risk_ratio)
                          else None,
        }
    except Exception as e:
        return {"error": str(e)}


def get_positions() -> list[dict]:
    """所有有持仓的合约"""
    try:
        import numpy as np
        api = _feed()._api
        positions = api.get_position()
        result = []

        def fv(v, d=2):
            f = float(v)
            return round(f, d) if not np.isnan(f) else 0.0

        for symbol, pos in positions.items():
            lv = data_feed._safe_vol(pos.volume_long)
            sv = data_feed._safe_vol(pos.volume_short)
            if lv == 0 and sv == 0:
                continue
            if lv > 0:
                result.append({
                    "symbol":       symbol,
                    "direction":    "多头",
                    "volume":       lv,
                    "open_price":   fv(pos.open_price_long),
                    "float_profit": fv(pos.float_profit_long),
                    "margin":       fv(pos.margin_long),
                    "volume_today": data_feed._safe_vol(pos.volume_long_today),
                    "volume_his":   data_feed._safe_vol(pos.volume_long_his),
                })
            if sv > 0:
                result.append({
                    "symbol":       symbol,
                    "direction":    "空头",
                    "volume":       sv,
                    "open_price":   fv(pos.open_price_short),
                    "float_profit": fv(pos.float_profit_short),
                    "margin":       fv(pos.margin_short),
                    "volume_today": data_feed._safe_vol(pos.volume_short_today),
                    "volume_his":   data_feed._safe_vol(pos.volume_short_his),
                })
        return result
    except Exception as e:
        logger.error(f"获取持仓失败: {e}")
        return []


def get_orders() -> list[dict]:
    """最近 20 条委托单"""
    try:
        api = _feed()._api
        orders = api.get_order()
        result = []
        for oid, order in orders.items():
            raw_ts = int(order.insert_date_time) if hasattr(order, "insert_date_time") else 0
            result.append({
                "order_id":  oid,
                "symbol":    getattr(order, "instrument_id", ""),
                "direction": "买" if order.direction == "BUY" else "卖",
                "offset":    _offset_cn(order.offset, getattr(order, "instrument_id", "")),
                "volume":    int(order.volume_orign),
                "filled":    int(order.volume_orign) if order.status == "FINISHED"
                             else int(order.volume_orign) - int(order.volume_left),
                "price":     round(float(order.limit_price), 2) if order.limit_price else 0,
                "status":    _status_cn(order.status),
                "time":      _fmt_ns(raw_ts) if raw_ts else "",
                "can_cancel": order.status == "ALIVE",
                "_ts":       raw_ts,
            })
        result.sort(key=lambda x: x.get("_ts", 0), reverse=True)
        for r in result:
            r.pop("_ts", None)
        return result[:30]
    except Exception as e:
        logger.error(f"获取委托单失败: {e}")
        return []


# ─────────────────────────────────────────────────────────────
# 下单 / 平仓 / 撤单（通过队列在 tqsdk 线程执行）
# ─────────────────────────────────────────────────────────────

def _guards_file() -> pathlib.Path:
    return pathlib.Path(config.get_data_root()) / "guards.json"


# ── 按日期分文件的路径与工具函数 ──────────────────────────────

def _orders_file(date_str: str = "") -> pathlib.Path:
    """orders/YYYY-MM-DD.jsonl"""
    d = date_str or datetime.now().strftime("%Y-%m-%d")
    p = pathlib.Path(config.get_data_root()) / "orders"
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{d}.jsonl"


def _trades_file(date_str: str = "") -> pathlib.Path:
    """trades/YYYY-MM-DD.jsonl"""
    d = date_str or datetime.now().strftime("%Y-%m-%d")
    p = pathlib.Path(config.get_data_root()) / "trades"
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{d}.jsonl"


def _date_range(start_date: str, end_date: str, default_days: int = 7) -> list[str]:
    """
    返回 [start_date, end_date] 闭区间内所有日期字符串列表。
    未传日期时默认返回最近 default_days 天（含今天）。
    """
    today = date.today()
    try:
        s = date.fromisoformat(start_date) if start_date else today - timedelta(days=default_days - 1)
    except ValueError:
        s = today - timedelta(days=default_days - 1)

    try:
        e = date.fromisoformat(end_date) if end_date else today
    except ValueError:
        e = today

    result = []
    cur = s
    while cur <= e:
        result.append(cur.isoformat())
        cur += timedelta(days=1)
    return result


def _save_guards():
    """将当前内存守卫持久化到 guards.json"""
    feed = data_feed._feed
    if feed is None:
        return
    try:
        with feed._lock:
            data = dict(feed._guards)
        gf = _guards_file()
        gf.parent.mkdir(parents=True, exist_ok=True)
        gf.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning(f"保存守卫失败: {e}")


def _load_guards():
    """从 guards.json 恢复守卫到内存（启动时调用）"""
    feed = data_feed._feed
    if feed is None:
        return
    gf = _guards_file()
    if not gf.exists():
        return
    try:
        data = json.loads(gf.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return
        # 兼容老格式守卫：补全新字段默认值
        for guard in data.values():
            guard.setdefault("order_tag",          "")
            guard.setdefault("entry_price",        None)
            guard.setdefault("bar_exit_ref_price", None)
            guard.setdefault("initial_stop", guard.get("stop_loss"))
            guard.setdefault("trailing_on",  False)   # 老守卫默认不追踪
            ep = guard.get("entry_price")
            sl = guard.get("stop_loss")
            if ep and sl:
                guard.setdefault("initial_risk", abs(ep - sl))
                guard.setdefault("peak_price",   ep)
            else:
                guard.setdefault("initial_risk", None)
                guard.setdefault("peak_price",   None)
            guard.setdefault("fill_confirmed",     True)  # 旧守卫重启后视为已确认成交
            guard.setdefault("entry_bar_count",    0)
            guard.setdefault("bar_exit_limit",     0)    # 旧守卫默认禁用跟进检测
            guard["bar_exit_triggered"] = False           # 重启后强制重置，避免僵尸触发
        with feed._lock:
            feed._guards.update(data)
        logger.info(f"已恢复 {len(data)} 个止盈止损守卫: {list(data.keys())}")
    except Exception as e:
        logger.warning(f"恢复守卫失败: {e}")


def _append_record(f: pathlib.Path, lock: threading.Lock, record: dict, label: str):
    """追加一条 JSON 记录到文件（线程安全）。由 save_order_record / save_trade_record 共用。"""
    try:
        with lock:
            with open(f, "a", encoding="utf-8") as fp:
                fp.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning(f"保存{label}记录失败: {e}")


def save_order_record(record: dict):
    """保存一条订单记录到当天文件（线程安全）"""
    _append_record(_orders_file(), _orders_file_lock, record, "订单")


def save_trade_record(record: dict):
    """保存一条成交记录到当天文件（线程安全）"""
    _append_record(_trades_file(), _trades_file_lock, record, "成交")


def _symbol_matches(pattern: str, target: str) -> bool:
    """检查 target 符号是否与 pattern 匹配，支持主连符号匹配实际合约。

    pattern 可以是：
      - 主连格式：KQ.m@SHFE.rb → 匹配 SHFE.rb2605、rb2605
      - 具体合约：SHFE.rb2605 → 只匹配完全相同的
      - 短格式：rb2605 → 只匹配完全相同的
    """
    if not pattern:
        return True
    if pattern == target:
        return True

    # 主连格式：KQ.m@SHFE.rb 或 KQ.d@CFFEX.IF
    if "@" in pattern:
        # 提取交易所.品种部分
        parts = pattern.split("@")
        if len(parts) != 2:
            return False
        underlying = parts[1]  # SHFE.rb

        # 提取品种代码（最后一个点之后的部分）
        if "." in underlying:
            exchange_product = underlying  # SHFE.rb
            product_code = underlying.split(".")[-1]  # rb
        else:
            exchange_product = underlying
            product_code = underlying

        # 检查 target 是否以品种代码开头（后面跟数字）
        # 可能格式：SHFE.rb2605 或 rb2605
        if target.startswith(exchange_product) and target[len(exchange_product):].isdigit():
            return True
        if target.startswith(product_code) and target[len(product_code):].isdigit():
            return True
        return False

    # 非主连格式：精确匹配
    return pattern == target


def _load_records(
    file_fn,
    error_label: str,
    symbol: str = "", limit: int = 100,
    start_date: str = "", end_date: str = "", model_id: str = "",
) -> list[dict]:
    """从按日期分文件的目录读取 JSONL 记录。由 load_orders / load_trades 共用。"""
    records = []
    for d in _date_range(start_date, end_date, default_days=7):
        f = file_fn(d)
        if not f.exists():
            continue
        try:
            for line in f.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    r = json.loads(line)
                    if symbol and not _symbol_matches(symbol, r.get("symbol", "")):
                        continue
                    if model_id and r.get("model_id") != model_id:
                        continue
                    records.append(r)
                except Exception:
                    continue
        except Exception as e:
            logger.warning(f"读取{error_label}文件失败 {f}: {e}")
    records.sort(key=lambda x: x.get("time", ""), reverse=True)
    return records[:limit]


def load_orders(symbol: str = "", limit: int = 100, start_date: str = "", end_date: str = "", model_id: str = "") -> list[dict]:
    """从按日期分文件的目录读取订单记录，默认最近7天"""
    return _load_records(_orders_file, "订单", symbol, limit, start_date, end_date, model_id)


def load_trades(symbol: str = "", limit: int = 100, start_date: str = "", end_date: str = "", model_id: str = "") -> list[dict]:
    """从按日期分文件的目录读取成交记录，默认最近7天"""
    return _load_records(_trades_file, "成交", symbol, limit, start_date, end_date, model_id)


def set_guard(actual_symbol: str, direction: str, stop_loss: float | None, take_profit: float | None, model_id: str = "", volume: int = 1, order_tag: str = "", entry_price: float | None = None, bar_exit_ref_price: float | None = None, trailing_on: bool = True):
    """
    注册止盈止损守卫。
    direction: "LONG" | "SHORT"
    model_id: AI 模型 ID，用于 PnL 归因（多模型同合约时区分守卫）
    volume: 该守卫负责的手数（触发时只平该手数，不平全部持仓）
    order_tag: 唯一标识符（通常用 order_id），防止同 model+symbol 多次下单互相覆盖守卫
    entry_price: 下单限价（用于追踪止损的保本/追踪计算）
    bar_exit_ref_price: 信号棒收盘价（AI 发送数据时的最新价），跟进不足 profit_r 计算基准
    _run_loop 中每次 wait_update 后自动检查，触发时自动平仓。
    """
    feed = data_feed._feed
    if feed is None:
        return
    initial_risk = abs(entry_price - stop_loss) if (entry_price and stop_loss) else None
    if order_tag:
        guard_key = f"{actual_symbol}::{model_id}::{order_tag}" if model_id else f"{actual_symbol}::{order_tag}"
    else:
        guard_key = f"{actual_symbol}::{model_id}" if model_id else actual_symbol
    with feed._lock:
        feed._guards[guard_key] = {
            "direction":    direction,
            "stop_loss":    stop_loss,
            "take_profit":  take_profit,
            "model_id":     model_id,
            "symbol":       actual_symbol,
            "volume":       volume,
            "order_tag":    order_tag,
            "entry_price":        entry_price,
            "bar_exit_ref_price": bar_exit_ref_price,  # 信号棒收盘价，跟进不足 profit_r 基准
            "initial_stop": stop_loss,
            "initial_risk": initial_risk,
            "peak_price":        entry_price,
            "trailing_on":       trailing_on,
            "fill_confirmed":    False,  # 成交回报确认前暂停追踪止损计算，防止用限价误算 profit_r
            "entry_bar_count":   0,     # 开仓后已收盘的K线数
            "bar_exit_limit":    2,     # N根K线内profit_r<0.1则提前退出（0=禁用）
            "bar_exit_triggered": False, # 跟进不足退出标记
        }
        logger.info(
            f"守卫已注册 {guard_key} {direction} "
            f"止损:{stop_loss} 止盈:{take_profit} 手数:{volume} "
            f"入场:{entry_price} 基准:{bar_exit_ref_price} 追踪:{'开' if initial_risk else '待补充'}"
        )
    _save_guards()


def update_guard(key_prefix: str, *, stop_loss: float | None = None, take_profit: float | None = None,
                 trailing_on: bool | None = None, recalc_risk: bool = True) -> list[dict]:
    """
    按前缀匹配守卫（支持 sym::model 或 sym::model::order_tag）并更新字段。
    - recalc_risk=True（默认，手动调整）：同时重算 initial_risk，追踪距离跟随新止损
    - recalc_risk=False（AI 持有更新）：只移动止损位置，保留原始追踪距离不变
    - 无论 recalc_risk 如何，保本底线始终生效
    - 返回被修改的所有守卫完整状态列表（含实际 gkey）
    """
    feed = data_feed._feed
    if feed is None:
        return []
    results = []
    with feed._lock:
        for gkey, guard in feed._guards.items():
            if gkey == key_prefix or gkey.startswith(key_prefix + "::"):
                if stop_loss is not None:
                    entry     = guard.get("entry_price")
                    direction = guard.get("direction")
                    cur_sl    = guard.get("stop_loss")
                    # 保本底线：若追踪已将止损移至保本线（cur_sl >= entry 多头 / <= entry 空头），
                    # AI 建议的新止损不得低于入场价
                    if entry and cur_sl is not None and direction:
                        if direction == "LONG" and cur_sl >= entry:
                            stop_loss = max(stop_loss, entry)
                        elif direction == "SHORT" and cur_sl <= entry:
                            stop_loss = min(stop_loss, entry)
                    guard["stop_loss"]    = stop_loss
                    guard["initial_stop"] = stop_loss
                    if recalc_risk and entry:
                        guard["initial_risk"] = abs(entry - stop_loss)
                if take_profit is not None:
                    guard["take_profit"] = take_profit
                if trailing_on is not None:
                    guard["trailing_on"] = trailing_on
                results.append({"gkey": gkey, "guard": dict(guard)})
    if results:
        _save_guards()
    return results


def clear_guard(actual_symbol: str, model_id: str = ""):
    """清除止盈止损守卫（平仓后调用）。
    使用前缀匹配，同时清除旧格式 (sym::model) 和新格式 (sym::model::order_tag) 的守卫。
    """
    feed = data_feed._feed
    if feed is None:
        return
    with feed._lock:
        if model_id:
            # 清除该 sym+model 的所有守卫（含每单独立守卫 sym::model::order_tag）
            prefix = f"{actual_symbol}::{model_id}"
            to_remove = [k for k in feed._guards
                         if k == prefix or k.startswith(f"{prefix}::")]
        else:
            # 清除该 symbol 的所有守卫
            to_remove = [k for k in feed._guards
                         if k == actual_symbol or k.startswith(f"{actual_symbol}::")]
        for k in to_remove:
            feed._guards.pop(k, None)
    _save_guards()


def place_order_from_analysis(analysis: dict, model_id: str = "", trailing_on: bool = True) -> dict:
    """
    根据 AI 分析结果下单。
    - 做多 → BUY / OPEN
    - 做空 → SELL / OPEN
    - 入场价：解析为限价；无法解析则用最新价
    - 手数：config.DEFAULT_TRADE_VOLUME（默认1手）
    """
    action    = analysis.get("操作建议", "观望")
    symbol    = analysis.get("symbol", "")
    entry_str = analysis.get("入场价", "")
    stop_str  = analysis.get("止损价", "")
    target_str = analysis.get("止盈价", "")

    if action == "观望":
        return {"ok": False, "message": "AI建议观望，不下单"}
    if action == "反手":
        # 反手：方向由 analysis 里的入场方向或历史方向反推
        # 约定：反手时 AI 的操作建议写"反手"，但入场价/止损/止盈按新方向填
        # 方向从 analysis["反手方向"] 取，fallback 到做多
        rev = analysis.get("反手方向", "做多")
        action = rev
    if not symbol:
        return {"ok": False, "message": "合约代码为空"}

    actual = _resolve_symbol(symbol)
    if not actual:
        return {"ok": False, "message": f"主连合约 {symbol} 尚未解析到实际合约，请稍后重试"}

    direction = "BUY" if action == "做多" else "SELL"
    volume    = config.DEFAULT_TRADE_VOLUME

    # 用下单时刻的对价（买一/卖一），规避 AI 分析延迟导致的入场价过时问题
    # AI 给出的 entry_str 仅作日志参考，不用于实际限价
    try:
        feed = data_feed._feed
        q = feed._quotes.get(symbol) or feed._quotes.get(actual)
        if q is None:
            raise ValueError("无行情对象")
        if direction == "BUY":
            raw_price = float(q.ask_price1) if not np.isnan(q.ask_price1) else float(q.last_price)
        else:
            raw_price = float(q.bid_price1) if not np.isnan(q.bid_price1) else float(q.last_price)
        if np.isnan(raw_price):
            raise ValueError("对价为 NaN")
    except Exception as e:
        raw_price = _get_last_price(symbol)
        if raw_price is None:
            return {"ok": False, "message": f"无法获取 {actual} 对价: {e}"}

    tick        = _get_price_tick(actual)
    entry_price = _round_to_tick(raw_price, tick)

    # ── Bug L-3 修复：实盘模式下预检保证金是否充足 ──────────────
    # tqsdk insert_order 立即返回，下单被拒只能事后从 order.status 得知。
    # 预检在 asyncio 线程内直接读账户对象（只读引用，线程安全），
    # 保证金不足时提前返回错误，避免下单→守卫注册→拒单→孤立守卫的完整链路。
    # 检查仅在 TQ_ENABLE_TRADING=true 时生效；检查本身出错时不阻止下单（宁可放行）。
    if config.TQ_ENABLE_TRADING:
        try:
            feed = data_feed._feed
            _q_margin = feed._quotes.get(actual) or feed._quotes.get(symbol) if feed else None
            if _q_margin is not None:
                _mult = float(getattr(_q_margin, 'volume_multiple', 0) or 0)
                _mr   = float(getattr(_q_margin,
                              'long_margin_ratio' if direction == 'BUY' else 'short_margin_ratio',
                              0) or 0)
                if _mult > 0 and _mr > 0:
                    _est = entry_price * volume * _mult * _mr
                    _acc = feed._api.get_account()
                    _avail = float(_acc.available)
                    if not np.isnan(_avail) and _avail < _est:
                        return {
                            "ok": False,
                            "message": f"保证金不足: 可用 {_avail:.0f} 元，预估需要 {_est:.0f} 元（{actual} {volume}手）",
                        }
        except Exception:
            pass  # 检查失败时不阻止下单

    guard_sym = symbol if "@" in symbol else actual
    result = _queue_op(
        "insert",
        symbol=actual,
        direction=direction,
        offset="OPEN",
        volume=volume,
        limit_price=entry_price,
        _model_id=model_id,    # 供 tqsdk 线程成交后更新守卫 entry_price
        _guard_sym=guard_sym,  # 守卫 key 前缀
    )

    if result and result.get("ok"):
        msg = (f"{'买入开多' if direction == 'BUY' else '卖出开空'} "
               f"{actual} {volume}手 @{entry_price} | "
               f"止损:{stop_str} 止盈:{target_str}")
        result["message"] = msg
        result["entry_price"] = entry_price   # 供调用方记录开仓成交价
        logger.info(f"下单成功: {msg}")

        # 注册止盈止损守卫（用 order_id 作为 order_tag，确保每笔订单有独立守卫）
        sl_price = extract_price(stop_str)
        tp_price = extract_price(target_str)
        guard_dir = "LONG" if direction == "BUY" else "SHORT"
        order_tag = result.get("order_id", "")
        # 信号棒收盘价：发送数据时的最新价，作为跟进不足 profit_r 的计算基准
        _bar_ref = None
        try:
            _klines = feed._klines_obj.get(symbol) or feed._klines_obj.get(actual)
            if _klines is not None and len(_klines) >= 2:
                _bar_ref = float(_klines['close'].iloc[-2])
        except Exception:
            pass
        set_guard(guard_sym, guard_dir, sl_price, tp_price,
                  model_id=model_id, volume=volume, order_tag=order_tag,
                  entry_price=entry_price, bar_exit_ref_price=_bar_ref,
                  trailing_on=trailing_on)
        logger.info(f"守卫注册: {guard_sym}::{model_id}::{order_tag} {guard_dir} SL={sl_price} TP={tp_price} 手数:{volume} 追踪:{'开' if trailing_on else '关'}")

    return result or {"ok": False, "message": "下单队列无响应"}




def reverse_order(symbol: str, new_direction: str, stop_loss: float | None, take_profit: float | None, model_id: str = "", trailing_on: bool = True) -> dict:
    """
    反手专用开仓：用对价（买一/卖一）反向开仓，注册新守卫。
    ⚠️ 此函数只负责开仓，平仓须由调用方在调用此函数前完成（见 main.py 反手逻辑）。
    new_direction: "BUY" | "SELL"
    """
    actual = _resolve_symbol(symbol)
    if not actual:
        return {"ok": False, "message": f"无法解析合约 {symbol}"}

    # 获取对价：BUY 用卖一价，SELL 用买一价
    try:
        feed = _feed()
        q = feed._quotes.get(symbol) or feed._quotes.get(actual)
        if q is None:
            return {"ok": False, "message": "无法获取行情，反手失败"}
        import numpy as np
        if new_direction == "BUY":
            price = float(q.ask_price1) if not np.isnan(q.ask_price1) else float(q.last_price)
        else:
            price = float(q.bid_price1) if not np.isnan(q.bid_price1) else float(q.last_price)
        if np.isnan(price):
            return {"ok": False, "message": "对价为 NaN，反手失败"}
    except Exception as e:
        return {"ok": False, "message": f"获取对价失败: {e}"}

    tick  = _get_price_tick(actual)
    price = _round_to_tick(price, tick)
    volume = config.DEFAULT_TRADE_VOLUME

    guard_sym = symbol if "@" in symbol else actual
    result = _queue_op(
        "insert",
        symbol=actual,
        direction=new_direction,
        offset="OPEN",
        volume=volume,
        limit_price=price,
        _model_id=model_id,    # Bug-4 修复：补传 _model_id/_guard_sym，确保 fill_tracker 被创建
        _guard_sym=guard_sym,  # 成交后实际价格可回填守卫 entry_price，追踪止损计算正确
    )
    if result and result.get("ok"):
        guard_dir = "LONG" if new_direction == "BUY" else "SHORT"
        order_tag = result.get("order_id", "")
        _rev_bar_ref = None
        try:
            _klines = feed._klines_obj.get(symbol) or feed._klines_obj.get(actual)
            if _klines is not None and len(_klines) >= 2:
                _rev_bar_ref = float(_klines['close'].iloc[-2])
        except Exception:
            pass
        set_guard(guard_sym, guard_dir, stop_loss, take_profit,
                  model_id=model_id, volume=volume, order_tag=order_tag,
                  entry_price=price, bar_exit_ref_price=_rev_bar_ref,
                  trailing_on=trailing_on)
        logger.info(f"反手开仓成功: {actual} {'做多' if new_direction=='BUY' else '做空'} @{price}（对价）")
        result["message"] = f"反手{'做多' if new_direction=='BUY' else '做空'} {actual} {volume}手 @{price}（对价）"
    return result or {"ok": False, "message": "反手下单无响应"}



def get_trades(since_ns: int = 0) -> list[dict]:
    """最近 50 条成交记录，含平仓收益。since_ns>0 时只返回该时间戳之后的成交（纳秒）"""
    try:
        api = _feed()._api
        trades = api.get_trade()
        result = []
        for tid, t in trades.items():
            price = float(t.price) if not np.isnan(float(t.price)) else 0.0
            # close_profit: only available for closing trades in tqsdk
            cp = getattr(t, 'close_profit', None)
            close_profit = round(float(cp), 2) if cp is not None and not np.isnan(float(cp)) else None
            # 从缓存提取开仓价与合约乘数（平仓记录用）
            open_price = None
            multiplier = None
            if t.offset in ('CLOSE', 'CLOSETODAY'):
                try:
                    instrument_id = getattr(t, 'instrument_id', '')
                    # 开仓价：优先短 key，再尝试 exchange.instrument_id 格式的长 key
                    cached = _position_open_cache.get(instrument_id)
                    cached_full = None
                    for _k, _v in _position_open_cache.items():
                        if _k != instrument_id and _k.endswith('.' + instrument_id):
                            cached_full = _v
                            break
                    _src = cached_full or cached
                    if _src:
                        if t.direction == 'SELL':
                            open_price = _src.get('open_price_long')
                        elif t.direction == 'BUY':
                            open_price = _src.get('open_price_short')
                    # 合约乘数：优先从 data_feed._quotes 取权威值，不依赖 open_cache 短 key
                    try:
                        import data_feed as _df_mod
                        _feed_inst = _df_mod._feed
                        if _feed_inst:
                            _q = _feed_inst._quotes.get(instrument_id)
                            if not _q:
                                for _qk in list(_feed_inst._quotes.keys()):
                                    if _qk.endswith('.' + instrument_id):
                                        _q = _feed_inst._quotes[_qk]
                                        break
                            if _q:
                                _m = float(getattr(_q, 'volume_multiple', 0) or 0)
                                if _m > 0:
                                    multiplier = _m
                    except Exception:
                        pass
                    # data_feed 取不到时回退到 open_cache
                    if multiplier is None and _src:
                        multiplier = _src.get('multiplier')
                except Exception:
                    pass

            # 若 tqsdk 未提供 close_profit，从开仓价推算
            if close_profit is None and open_price and multiplier:
                if t.direction == 'SELL':
                    close_profit = round((price - open_price) * int(t.volume) * multiplier, 2)
                elif t.direction == 'BUY':
                    close_profit = round((open_price - price) * int(t.volume) * multiplier, 2)

            raw_ts = int(t.trade_date_time) if hasattr(t, "trade_date_time") else 0
            trade_record = {
                "trade_id":     tid,
                "symbol":       getattr(t, "instrument_id", ""),
                "direction":    "买" if t.direction == "BUY" else "卖",
                "offset":       _offset_cn(t.offset, getattr(t, "instrument_id", "")),
                "volume":       int(t.volume),
                "price":        price,
                "open_price":   open_price,
                "multiplier":   multiplier,
                "close_profit": close_profit,
                "time":         _fmt_ns(raw_ts) if raw_ts else "",
                "_ts":          raw_ts,
            }
            result.append(trade_record)
        result.sort(key=lambda x: x.get("_ts", 0), reverse=True)
        if since_ns:
            result = [r for r in result if r.get("_ts", 0) >= since_ns]
        for r in result:
            r.pop("_ts", None)
        return result[:50]
    except Exception as e:
        logger.error(f"获取成交记录失败: {e}")
        return []

def close_position(symbol: str, model_id: str = "") -> dict:
    """平掉指定合约的全部持仓"""
    actual = _resolve_symbol(symbol)
    if not actual:
        return {"ok": False, "message": f"无法解析合约 {symbol}"}

    try:
        import numpy as np
        api = _feed()._api
        pos = api.get_position(actual)
        raw_price = _get_last_price(symbol)
        if raw_price is None:
            return {"ok": False, "message": f"无法获取 {actual} 的最新价，平仓中止"}
        tick  = _get_price_tick(actual)
        price = _round_to_tick(raw_price, tick)

        lv_today = data_feed._safe_vol(pos.volume_long_today)
        lv_his   = data_feed._safe_vol(pos.volume_long_his)
        sv_today = data_feed._safe_vol(pos.volume_short_today)
        sv_his   = data_feed._safe_vol(pos.volume_short_his)

        if lv_today + lv_his + sv_today + sv_his == 0:
            return {"ok": False, "message": f"{actual} 暂无持仓"}

        # 平仓前缓存开仓价与合约乘数，供 get_trades() 补全 close_profit
        try:
            feed = data_feed._feed
            quote = (feed._quotes.get(symbol) or feed._quotes.get(actual)) if feed else None
            mult = float(getattr(quote, 'volume_multiple', 0) or 0) if quote else 0.0
            op_l = float(getattr(pos, 'open_price_long', float('nan')))
            op_s = float(getattr(pos, 'open_price_short', float('nan')))
            existing = _position_open_cache.get(actual, {})
            _position_open_cache[actual] = {
                'open_price_long':  (op_l if not np.isnan(op_l) and op_l > 0 else None) or existing.get('open_price_long'),
                'open_price_short': (op_s if not np.isnan(op_s) and op_s > 0 else None) or existing.get('open_price_short'),
                'multiplier':       (mult if mult > 0 else None) or existing.get('multiplier'),
            }
            _save_open_cache(_position_open_cache)  # 修复：持久化，重启后可恢复
        except Exception as e:
            logger.warning(f"缓存开仓价失败（不影响平仓执行）: {e}")

        msgs = []
        if lv_today > 0:
            _queue_op("insert", symbol=actual, direction="SELL",
                      offset=_close_offset(actual, True), volume=lv_today, limit_price=price)
            msgs.append(f"平今多 {lv_today} 手")
        if lv_his > 0:
            _queue_op("insert", symbol=actual, direction="SELL",
                      offset=_close_offset(actual, False), volume=lv_his, limit_price=price)
            msgs.append(f"平昨多 {lv_his} 手")
        if sv_today > 0:
            _queue_op("insert", symbol=actual, direction="BUY",
                      offset=_close_offset(actual, True), volume=sv_today, limit_price=price)
            msgs.append(f"平今空 {sv_today} 手")
        if sv_his > 0:
            _queue_op("insert", symbol=actual, direction="BUY",
                      offset=_close_offset(actual, False), volume=sv_his, limit_price=price)
            msgs.append(f"平昨空 {sv_his} 手")

        msg = f"{actual} 平仓: " + "、".join(msgs)
        logger.info(msg)
        # 平仓后清除止盈止损守卫（key与注册时一致：主连符号优先）
        guard_sym = symbol if "@" in symbol else actual
        clear_guard(guard_sym, model_id=model_id)
        clear_guard(actual, model_id=model_id)  # 兜底：同时清除实际合约key（防止两种方式注册的守卫）
        return {"ok": True, "message": msg}

    except Exception as e:
        logger.error(f"平仓失败: {e}")
        return {"ok": False, "message": f"平仓失败: {e}"}


def cancel_order(order_id: str) -> dict:
    """撤销委托单"""
    try:
        api    = _feed()._api
        orders = api.get_order()
        order  = orders.get(order_id)
        if order is None:
            return {"ok": False, "message": "未找到委托单"}
        return _queue_op("cancel", order_obj=order)
    except Exception as e:
        return {"ok": False, "message": f"撤单失败: {e}"}


def cancel_pending_orders(symbol: str) -> dict:
    """撤销指定合约的所有未成交（ALIVE）委托，用于对价单未撮合时撤单重下"""
    try:
        actual = _resolve_symbol(symbol) or symbol
        api    = _feed()._api
        # list() 快照，避免 tqsdk 线程在迭代中修改 dict 引发 RuntimeError
        orders_snapshot = list(api.get_order().items())
        alive_orders = [
            (oid, order) for oid, order in orders_snapshot
            if (getattr(order, "instrument_id", "") == actual
                and getattr(order, "status", "") == "ALIVE")
        ]
        if alive_orders:
            logger.info(
                f"撤单检查 {actual}: 共 {len(orders_snapshot)} 笔委托，"
                f"其中 ALIVE {len(alive_orders)} 笔"
            )
        cancelled = []
        for oid, order in alive_orders:
            r = _queue_op("cancel", order_obj=order)
            if r.get("ok"):
                cancelled.append(oid)
                logger.info(f"撤单已提交: {actual} order_id={oid}")
            else:
                logger.warning(f"撤单失败 {oid}: {r.get('message')}")
        return {"ok": True, "message": f"已撤 {len(cancelled)} 笔", "cancelled": cancelled}
    except Exception as e:
        logger.warning(f"撤销未成交委托失败 {symbol}: {e}")
        return {"ok": False, "message": str(e)}
