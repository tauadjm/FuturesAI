"""
backtest/engine.py — 回测核心引擎

职责：
1. 从 Parquet 文件加载历史 K 线数据
2. 从 DataFrame 重建与 data_feed.fetch() 完全兼容的 market_data 字典
3. K 线级别追踪止损模拟（复用 data_feed._run_loop 的分段逻辑）
4. 单次交易 P&L 计算
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_feed import get_symbol_name
from strategies import get_strategy as _get_strategy

# 默认数据根目录
_DEFAULT_KLINES_DIR = Path("data/klines")

# ─────────────────────────────────────────────────────────────
# 合约每手每跳盈亏（元）
# ─────────────────────────────────────────────────────────────
_TICK_VALUE: dict[str, float] = {
    # 郑商所 (CZCE)
    "SA": 20, "TA": 10, "SR": 10, "FG": 20, "PF": 10,
    "SH": 30, "PX": 10, "MA": 10, "CF": 25, "CY": 25,
    "AP": 10, "CJ": 25, "RM": 10, "RS": 10, "OI": 10,
    "PK": 10, "SF": 10, "SM": 10, "UR": 20, "PR": 30,
    # 大商所 (DCE)
    "I":  50, "J":  50, "JM": 30, "M":  10, "Y":  10,
    "P":  10, "A":  10, "B":  10, "FB":  5, "C":  10,
    "CS": 10, "RR": 10, "L":   5, "V":   5, "PP":  5,
    "BZ": 30, "EB":  5, "EG": 10, "JD": 10, "LH": 80,
    "PG": 20, "LG": 45,
    # 能源中心 (INE)
    "SC": 100, "LU": 10, "NR": 50, "BC": 50, "EC": 5,
    # 上期所 (SHFE)
    "CU": 50, "AL": 25, "AO": 20, "ZN": 25, "PB": 25,
    "NI": 10, "SN": 10, "AG": 15, "AU": 20, "RB": 10,
    "HC": 10, "SS": 25, "FU": 10, "BU": 10, "RU": 50,
    "BR": 25, "SP": 20,
    # 广期所 (GFEX)
    "SI": 25, "LC": 50, "PS": 15, "PT": 50, "PD": 50,
}


def get_tick_value(symbol: str) -> float | None:
    """根据合约代码返回每手每跳盈亏（元）。

    支持格式：KQ.m@SHFE.rb / SHFE.rb / SHFE.RB2605 等。
    返回 None 表示品种未收录。
    """
    norm = normalize_symbol(symbol)          # e.g. SHFE.rb
    parts = norm.upper().split(".")          # ['SHFE', 'RB']
    if len(parts) < 2:
        return None
    code = re.sub(r"\d+$", "", parts[1])    # 去掉月份数字 RB2605→RB
    return _TICK_VALUE.get(code)


# ─────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────

def normalize_symbol(symbol: str) -> str:
    """KQ.m@SHFE.rb → SHFE.rb，SHFE.rb → SHFE.rb"""
    m = re.search(r'@([A-Z]+\.\w+)', symbol)
    return m.group(1) if m else symbol


# ─────────────────────────────────────────────────────────────
# 数据加载
# ─────────────────────────────────────────────────────────────

def load_klines(
    symbol: str,
    start_dt: datetime | None = None,
    end_dt: datetime | None = None,
    duration_seconds: int = 300,
    data_dir: Path = _DEFAULT_KLINES_DIR,
) -> pd.DataFrame:
    """
    加载合约的历史 K 线 Parquet 文件，合并多年数据，按时间过滤。

    返回 DataFrame，列：datetime(int64 ns), open, high, low, close, volume, time_unix
    已按 datetime 排序，index 为 0..N-1。
    """
    product = normalize_symbol(symbol)
    interval_min = duration_seconds // 60
    interval_dir = data_dir / product / f"{interval_min}m"

    if not interval_dir.exists():
        raise FileNotFoundError(
            f"未找到 K 线数据目录：{interval_dir}\n"
            f"请先运行：python download_klines.py --symbol {product} --interval {interval_min} --year YYYY"
        )

    parquet_files = sorted(interval_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"目录 {interval_dir} 下无 Parquet 文件")

    frames = []
    for f in parquet_files:
        try:
            frames.append(pd.read_parquet(str(f)))
        except Exception as e:
            print(f"警告：读取 {f} 失败 — {e}")

    if not frames:
        raise ValueError(f"所有 Parquet 文件均读取失败：{interval_dir}")

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)

    # 时间过滤（基于 time_unix，Unix 秒）
    if start_dt is not None:
        ts_start = int(start_dt.timestamp())
        df = df[df["time_unix"] >= ts_start]
    if end_dt is not None:
        ts_end = int(end_dt.timestamp())
        df = df[df["time_unix"] <= ts_end]

    df = df.reset_index(drop=True)
    return df


def list_available_symbols(data_dir: Path = _DEFAULT_KLINES_DIR) -> list[dict]:
    """扫描数据目录，按文件返回每个可用的合约+周期+年份条目。"""
    if not data_dir.exists():
        return []

    result = []
    for product_dir in sorted(data_dir.iterdir()):
        if not product_dir.is_dir():
            continue
        for interval_dir in sorted(product_dir.iterdir()):
            if not interval_dir.is_dir():
                continue
            # 解析周期分钟数，如 "5m" → 5
            try:
                interval_min = int(interval_dir.name.rstrip("m"))
            except ValueError:
                continue
            for parquet_file in sorted(interval_dir.glob("*.parquet")):
                try:
                    pf = pd.read_parquet(str(parquet_file), columns=["datetime"])
                    bar_count = len(pf)
                except Exception:
                    bar_count = 0
                result.append({
                    "symbol":       product_dir.name,
                    "interval_min": interval_min,
                    "interval":     interval_dir.name,
                    "year":         parquet_file.stem,
                    "bar_count":    bar_count,
                    "display_name": get_symbol_name(product_dir.name),
                })
    return result


# ─────────────────────────────────────────────────────────────
# market_data 重建
# ─────────────────────────────────────────────────────────────

def build_market_data_at(
    symbol: str,
    df_full: pd.DataFrame,
    bar_index: int,
    duration_seconds: int = 300,
    lookback: int = 250,
    ai_kline_count: int = 40,
    strategy=None,
) -> dict[str, Any]:
    """
    重建某时间点的 market_data 字典，格式与 data_feed.fetch() 完全一致。

    Args:
        symbol:           合约代码（原始格式，如 KQ.m@SHFE.rb）
        df_full:          完整历史 DataFrame（load_klines 返回）
        bar_index:        用户选择的 K 线在 df_full 中的位置（0-based）
        duration_seconds: K 线周期秒数
        lookback:         传给 build_price_structure 的窗口大小（≤250）
        ai_kline_count:   传给 AI 的 K 线根数
        strategy:         策略实例；None 时使用当前活跃策略

    Returns:
        与 data_feed.fetch() 格式完全一致的 market_data 字典
    """
    if strategy is None:
        strategy = _get_strategy()

    # ── 结构字段默认值 ─────────────────────────────────────────
    result: dict[str, Any] = {
        "symbol":               symbol,
        "product_name":         get_symbol_name(symbol),
        "timestamp":            "",
        "is_trading_time":      True,   # 历史数据均视为交易时间
        "is_session_first_bar": False,
        "is_session_last_bar":  False,
        "quote":      {},
        "ema20":      {},
        "klines":     [],
        "pending":    False,
        "error":      None,
        # 价格行为结构字段默认值
        "近期波段高点":    [],
        "近期波段低点":    [],
        "测量目标向上":    [],
        "测量目标向下":    [],
        "回调腿标签":      "未知",
        "回调深度%":       0.0,
        "回调根数":        0,
        "出现顺势实体棒":  False,
        "交易区间上沿":    None,
        "交易区间下沿":    None,
        "价格在区间位置":  None,
        "是否窄幅区间":    False,
        "前时段高点":      None,
        "前时段低点":      None,
        "微趋势线价位":    None,
        "微趋势线突破":    False,
        "主趋势线价位":    None,
        "主趋势线突破":    False,
        "趋势末期警告":    False,
        "回调形态":        None,
        "is_limit_up":    False,
        "is_limit_down":  False,
        "区间中点":        None,
        "最近突破价位":    None,
        "突破后K线数":     0,
        "突破方向":        None,
        "杯柄形态":        False,
        "柄深比例%":       None,
        "缺口方向":        None,
        "缺口磁铁价位":    None,
        "缺口幅度%":       None,
        "区间突破失败率%": None,
        "楔形反转类型":    None,
        "趋势方向":        "震荡",
        "_ema斜率_10根":   None,
        "_ema斜率_5根":    None,
        "_趋势阈值":       None,
        "_价格EMA偏离%":   None,
    }

    if bar_index < 1:
        result["error"] = "bar_index 太小（需要至少1根前置K线）"
        return result

    # ── 取回溯窗口（最多 lookback 根，含当前 bar） ─────────────
    start_idx = max(0, bar_index - lookback + 1)
    df_window = df_full.iloc[start_idx: bar_index + 1].copy().reset_index(drop=True)

    if df_window.empty or len(df_window) < 5:
        result["error"] = "历史数据不足（需要至少5根K线）"
        return result

    bar = df_window.iloc[-1]       # 当前 bar（用户选择的那根）
    prev_bar = df_window.iloc[-2]  # 前一根 bar

    # ── 追加 stub 棒，对齐实盘行为 ────────────────────────────
    # 实盘分析触发时 iloc[-1] 是刚开盘的空壳棒，iloc[-2] 才是刚收盘的分析目标棒。
    # build_price_structure 全局用 iloc[-2] 作为"最后完成K线"，
    # 故回测也须在末尾追加一根 stub，让目标棒落在 iloc[-2]。
    if bar_index + 1 < len(df_full):
        stub = df_full.iloc[bar_index + 1: bar_index + 2].copy()
    else:
        stub = df_window.iloc[[-1]].copy()
        stub["open"]   = stub["close"]
        stub["high"]   = stub["close"]
        stub["low"]    = stub["close"]
        stub["volume"] = 0
    df_window = pd.concat([df_window, stub], ignore_index=True)

    # ── timestamp ─────────────────────────────────────────────
    try:
        ts = pd.Timestamp(int(bar["datetime"]), unit="ns", tz="Asia/Shanghai") + pd.Timedelta(seconds=duration_seconds)
        result["timestamp"] = ts.strftime("%Y-%m-%d %H:%M:%S")
        # 首尾盘判断
        h, mn = ts.hour, ts.minute
        # 粗略判断（基于时间，不依赖 config 的精确交易时段）
        result["is_session_first_bar"] = (h == 9 and mn <= 5) or (h == 21 and mn <= 5)
        # 15:00 是日盘最后一根（15:00 K线 = 14:55~15:00），11:30 是上午最后一根
        result["is_session_last_bar"]  = (h == 11 and mn >= 25) or (h == 15 and mn == 0)
    except Exception:
        result["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ── quote ─────────────────────────────────────────────────
    close_price  = float(bar["close"])
    open_price   = float(bar["open"])
    high_price   = float(bar["high"])
    low_price    = float(bar["low"])
    prev_close   = float(prev_bar["close"])
    volume       = int(bar["volume"]) if not np.isnan(bar["volume"]) else 0
    chg          = round(close_price - prev_close, 4)
    chg_pct      = round((close_price - prev_close) / prev_close * 100, 2) if prev_close else None

    result["quote"] = {
        "最新价":          close_price,
        "今开":            open_price,
        "最高":            high_price,
        "最低":            low_price,
        "昨收":            prev_close,
        "涨跌额":          chg,
        "涨跌幅%":         chg_pct,
        "当日累计成交量手": volume,
        "持仓量":          0,
        "涨停价":          None,
        "跌停价":          None,
        "是否涨停":        False,
        "是否跌停":        False,
    }

    # ── 价格行为特征（调用策略层）─────────────────────────────
    try:
        strategy_out = strategy.build_price_structure(df_window)
        ema20 = strategy_out.pop("ema20", {})
        _ema_values = strategy_out.pop("_ema_values", None)
        result["ema20"] = ema20
        result.update(strategy_out)
    except Exception as e:
        result["error"] = f"价格结构计算失败：{e}"
        _ema_values = None

    # ── klines（最近 ai_kline_count 根，格式化时间列）──────────
    # 排除末尾 stub 棒（只用于对齐价格结构计算，不传给 AI）
    df_ai = df_window.iloc[:-1].tail(ai_kline_count).copy()

    # 时间列：开盘纳秒时间戳 → "YYYY-MM-DD HH:MM"（收盘时刻）
    try:
        ts_series = (
            pd.to_datetime(df_ai["datetime"].astype(float), unit="ns", utc=True)
            .dt.tz_convert("Asia/Shanghai")
            + pd.Timedelta(seconds=duration_seconds)
        )
        df_ai["time"] = ts_series.dt.strftime("%Y-%m-%d %H:%M").values
    except Exception:
        df_ai["time"] = df_ai["datetime"].astype(str).values

    df_ai["open"]   = df_ai["open"].astype(float).round(2)
    df_ai["high"]   = df_ai["high"].astype(float).round(2)
    df_ai["low"]    = df_ai["low"].astype(float).round(2)
    df_ai["close"]  = df_ai["close"].astype(float).round(2)
    df_ai["volume"] = df_ai["volume"].astype(float).fillna(0).astype(int)

    # EMA20 列（前端图表显示用）
    if _ema_values is not None and len(_ema_values) == len(df_window):
        # _ema_values 是 df_window 全长（含 stub），去掉最后1根再取尾部
        ema_tail = _ema_values[:-1][-ai_kline_count:]
        if len(ema_tail) == len(df_ai):
            df_ai["ema20"] = np.round(ema_tail, 2)
        else:
            df_ai["ema20"] = df_ai["close"].ewm(span=20, adjust=False).mean().round(2).values
    else:
        df_ai["ema20"] = df_ai["close"].ewm(span=20, adjust=False).mean().round(2).values

    # body_ratio
    body    = (df_ai["close"] - df_ai["open"]).abs()
    bar_rng = (df_ai["high"] - df_ai["low"]).clip(lower=1e-9)
    df_ai["body_ratio"] = (body / bar_rng).round(3).values

    # close_position
    df_ai["close_position"] = ((df_ai["close"] - df_ai["low"]) / bar_rng).round(3).values

    keep_cols = ["time", "open", "high", "low", "close", "volume", "ema20", "body_ratio", "close_position"]
    result["klines"] = df_ai[keep_cols].to_dict("records")

    return result


# ─────────────────────────────────────────────────────────────
# 追踪止损核心（纯函数，与 data_feed._run_loop 逻辑完全对齐）
# ─────────────────────────────────────────────────────────────

def _trail_stop_update(
    direction: str,
    price: float,
    entry_price: float,
    peak_price: float,
    stop_loss: float,
    initial_risk: float,
    take_profit: float | None,
    price_tick: float = 1.0,
) -> tuple[float, float]:
    """
    计算追踪止损的新值。

    Args:
        direction:    "LONG" | "SHORT"
        price:        当前价（K线的 close，用于更新追踪）
        entry_price:  开仓价
        peak_price:   历史最优价（多头=最高价，空头=最低价）
        stop_loss:    当前止损价
        initial_risk: 初始风险距离（= |entry - initial_sl|）
        take_profit:  止盈价（None 表示无止盈）
        price_tick:   最小价格变动单位

    Returns:
        (new_stop_loss, new_peak_price)
    """
    ini_risk = initial_risk
    if ini_risk <= 0:
        return stop_loss, peak_price

    if direction == "LONG":
        # 更新峰值
        new_peak = max(peak_price, price)
        profit_r = (new_peak - entry_price) / ini_risk

        if profit_r < 0.6:
            trail_dist = None
            floor = None
        elif profit_r < 1.0:
            trail_dist = 1.0 * ini_risk
            floor = entry_price + price_tick   # 保本 + 1跳
        elif profit_r < 2.0:
            trail_dist = 0.7 * ini_risk
            floor = entry_price + 0.3 * ini_risk
        else:
            trail_dist = 0.5 * ini_risk
            floor = entry_price + 1.5 * ini_risk

        # 止盈感知层
        if trail_dist is not None and take_profit is not None and entry_price:
            total_zone = take_profit - entry_price
            dist_to_tp = take_profit - price
            if total_zone > 0:
                dist_ratio = dist_to_tp / total_zone
                if 0 <= dist_ratio <= 0.15:
                    trail_dist = min(trail_dist, 0.15 * ini_risk)
                elif dist_ratio <= 0.35:
                    trail_dist = min(trail_dist, 0.3 * ini_risk)

        if trail_dist is not None:
            new_stop = new_peak - trail_dist
            if floor is not None:
                new_stop = max(new_stop, floor)
            # 止损只能上移
            if new_stop > stop_loss:
                stop_loss = new_stop

        return stop_loss, new_peak

    else:  # SHORT
        # 更新峰值（空头峰值是最低价）
        new_peak = min(peak_price, price)
        profit_r = (entry_price - new_peak) / ini_risk

        if profit_r < 0.6:
            trail_dist = None
            floor = None
        elif profit_r < 1.0:
            trail_dist = 1.0 * ini_risk
            floor = entry_price - price_tick   # 保本 - 1跳
        elif profit_r < 2.0:
            trail_dist = 0.7 * ini_risk
            floor = entry_price - 0.3 * ini_risk
        else:
            trail_dist = 0.5 * ini_risk
            floor = entry_price - 1.5 * ini_risk

        # 止盈感知层
        if trail_dist is not None and take_profit is not None and entry_price:
            total_zone = entry_price - take_profit  # 空头：正值
            dist_to_tp = price - take_profit
            if total_zone > 0:
                dist_ratio = dist_to_tp / total_zone
                if 0 <= dist_ratio <= 0.15:
                    trail_dist = min(trail_dist, 0.15 * ini_risk)
                elif dist_ratio <= 0.35:
                    trail_dist = min(trail_dist, 0.3 * ini_risk)

        if trail_dist is not None:
            new_stop = new_peak + trail_dist
            if floor is not None:
                new_stop = min(new_stop, floor)
            # 止损只能下移
            if new_stop < stop_loss:
                stop_loss = new_stop

        return stop_loss, new_peak


# ─────────────────────────────────────────────────────────────
# 盈亏模拟
# ─────────────────────────────────────────────────────────────

def simulate_trade(
    direction: str,
    entry_price: float,
    stop_loss: float,
    take_profit: float | None,
    df_future: pd.DataFrame,
    price_tick: float = 1.0,
    max_bars: int = 100,
) -> dict[str, Any]:
    """
    K 线级别追踪止损模拟。

    Args:
        direction:    "LONG" | "SHORT"
        entry_price:  开仓价
        stop_loss:    初始止损价
        take_profit:  止盈价（None 表示无止盈）
        df_future:    入场 bar 之后的 K 线 DataFrame（load_klines 返回的格式）
        price_tick:   最小价格变动单位
        max_bars:     最多模拟 N 根 K 线

    Returns:
        {
            "exit_bar_index": int,   # 出场 K 线在 df_future 中的索引（-1 = 未出场）
            "exit_price": float,
            "exit_type": "SL" | "TP" | "MAX_BARS" | "ONGOING",
            "pnl_r": float,          # 盈亏 R 值（盈利为正，亏损为负）
            "pnl_ticks": float,      # 盈亏跳数
            "peak_price": float,     # 持仓期间最优价
            "bars_held": int,        # 持仓根数
            "ambiguous": bool,       # 是否存在同一根 K 线 SL 和 TP 均触及的歧义
            "sl_trail_log": list,    # 追踪止损变化记录
        }
    """
    initial_risk = abs(entry_price - stop_loss)
    if initial_risk <= 0:
        return {
            "exit_bar_index": -1,
            "exit_price": entry_price,
            "exit_type": "ERROR",
            "pnl_r": 0.0,
            "pnl_ticks": 0.0,
            "peak_price": entry_price,
            "bars_held": 0,
            "ambiguous": False,
            "sl_trail_log": [],
        }

    peak_price  = entry_price
    current_sl  = stop_loss
    sl_trail_log = []
    ambiguous   = False

    df_sim = df_future.head(max_bars).reset_index(drop=True)

    for i, row in df_sim.iterrows():
        bar_open  = float(row["open"])
        bar_high  = float(row["high"])
        bar_low   = float(row["low"])
        bar_close = float(row["close"])

        if direction == "LONG":
            # 1. 跳空低开穿止损（Gap Down）
            if bar_open <= current_sl:
                exit_price = bar_open
                pnl = exit_price - entry_price
                return _build_result(
                    i, exit_price, "SL", pnl, initial_risk, price_tick,
                    peak_price, i + 1, ambiguous, sl_trail_log
                )
            # 2. 低点触及止损（最坏情况优先）
            if bar_low <= current_sl:
                sl_hit = True
                tp_hit = take_profit is not None and bar_high >= take_profit
                if sl_hit and tp_hit:
                    ambiguous = True
                exit_price = current_sl
                pnl = exit_price - entry_price
                return _build_result(
                    i, exit_price, "SL", pnl, initial_risk, price_tick,
                    peak_price, i + 1, ambiguous, sl_trail_log
                )
            # 3. 高点触及止盈
            if take_profit is not None and bar_high >= take_profit:
                exit_price = take_profit
                pnl = exit_price - entry_price
                return _build_result(
                    i, exit_price, "TP", pnl, initial_risk, price_tick,
                    peak_price, i + 1, ambiguous, sl_trail_log
                )
            # 4. 以收盘价更新追踪止损
            old_sl = current_sl
            current_sl, peak_price = _trail_stop_update(
                direction, bar_close, entry_price, peak_price,
                current_sl, initial_risk, take_profit, price_tick
            )
            if current_sl != old_sl:
                sl_trail_log.append({
                    "bar": i,
                    "price": bar_close,
                    "new_sl": round(current_sl, 4),
                    "peak":   round(peak_price, 4),
                })

        else:  # SHORT
            # 1. 跳空高开穿止损（Gap Up）
            if bar_open >= current_sl:
                exit_price = bar_open
                pnl = entry_price - exit_price
                return _build_result(
                    i, exit_price, "SL", pnl, initial_risk, price_tick,
                    peak_price, i + 1, ambiguous, sl_trail_log
                )
            # 2. 高点触及止损（最坏情况优先）
            if bar_high >= current_sl:
                sl_hit = True
                tp_hit = take_profit is not None and bar_low <= take_profit
                if sl_hit and tp_hit:
                    ambiguous = True
                exit_price = current_sl
                pnl = entry_price - exit_price
                return _build_result(
                    i, exit_price, "SL", pnl, initial_risk, price_tick,
                    peak_price, i + 1, ambiguous, sl_trail_log
                )
            # 3. 低点触及止盈
            if take_profit is not None and bar_low <= take_profit:
                exit_price = take_profit
                pnl = entry_price - exit_price
                return _build_result(
                    i, exit_price, "TP", pnl, initial_risk, price_tick,
                    peak_price, i + 1, ambiguous, sl_trail_log
                )
            # 4. 以收盘价更新追踪止损
            old_sl = current_sl
            current_sl, peak_price = _trail_stop_update(
                direction, bar_close, entry_price, peak_price,
                current_sl, initial_risk, take_profit, price_tick
            )
            if current_sl != old_sl:
                sl_trail_log.append({
                    "bar": i,
                    "price": bar_close,
                    "new_sl": round(current_sl, 4),
                    "peak":   round(peak_price, 4),
                })

    # 超过 max_bars 未出场
    last_close = float(df_sim.iloc[-1]["close"]) if not df_sim.empty else entry_price
    pnl = (last_close - entry_price) if direction == "LONG" else (entry_price - last_close)
    return _build_result(
        len(df_sim) - 1, last_close, "MAX_BARS", pnl, initial_risk, price_tick,
        peak_price, len(df_sim), ambiguous, sl_trail_log
    )


def _build_result(
    exit_idx: int,
    exit_price: float,
    exit_type: str,
    pnl: float,
    initial_risk: float,
    price_tick: float,
    peak_price: float,
    bars_held: int,
    ambiguous: bool,
    sl_trail_log: list,
) -> dict[str, Any]:
    pnl_r    = round(pnl / initial_risk, 3) if initial_risk > 0 else 0.0
    pnl_ticks = round(pnl / price_tick, 1) if price_tick > 0 else pnl
    return {
        "exit_bar_index": exit_idx,
        "exit_price":     round(exit_price, 4),
        "exit_type":      exit_type,
        "pnl_r":          pnl_r,
        "pnl_ticks":      pnl_ticks,
        "peak_price":     round(peak_price, 4),
        "bars_held":      bars_held,
        "ambiguous":      ambiguous,
        "sl_trail_log":   sl_trail_log,
    }


# ─────────────────────────────────────────────────────────────
# 会话统计
# ─────────────────────────────────────────────────────────────

def compute_stats(trades: list[dict]) -> dict[str, Any]:
    """
    计算一组已完成交易的统计指标。

    Args:
        trades: 每条包含 pnl_r、exit_type 字段的交易列表

    Returns:
        {win_rate, avg_win_r, avg_loss_r, profit_factor, expectancy_r, total, winners, losers}
    """
    completed = [t for t in trades if t.get("exit_type") in ("SL", "TP")]
    if not completed:
        return {
            "total": len(trades),
            "completed": 0,
            "winners": 0,
            "losers": 0,
            "win_rate": 0.0,
            "avg_win_r": 0.0,
            "avg_loss_r": 0.0,
            "profit_factor": 0.0,
            "expectancy_r": 0.0,
        }

    winners = [t for t in completed if t.get("pnl_r", 0) > 0]
    losers  = [t for t in completed if t.get("pnl_r", 0) <= 0]

    win_rate   = len(winners) / len(completed)
    avg_win_r  = sum(t["pnl_r"] for t in winners) / len(winners) if winners else 0.0
    avg_loss_r = abs(sum(t["pnl_r"] for t in losers) / len(losers)) if losers else 0.0

    gross_profit = sum(t["pnl_r"] for t in winners)
    gross_loss   = abs(sum(t["pnl_r"] for t in losers))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else None

    expectancy_r = win_rate * avg_win_r - (1 - win_rate) * avg_loss_r

    return {
        "total":         len(trades),
        "completed":     len(completed),
        "winners":       len(winners),
        "losers":        len(losers),
        "win_rate":      round(win_rate, 4),
        "avg_win_r":     round(avg_win_r, 3),
        "avg_loss_r":    round(avg_loss_r, 3),
        "profit_factor": round(profit_factor, 3) if profit_factor is not None else None,
        "expectancy_r":  round(expectancy_r, 3),
    }
