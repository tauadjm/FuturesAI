"""
backtest/router.py — 回测 API 路由

注册到 main.py：
    from backtest.router import backtest_router
    app.include_router(backtest_router)

端点前缀均为 /backtest/*
"""

import asyncio
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

import config
from backtest import engine
from download_klines import download as _download_klines
from strategies import get_strategy_by_id, list_strategies

backtest_router = APIRouter(prefix="/backtest", tags=["backtest"])

# 内存中的会话数据：analysis_id → 交易信息
_sessions: dict[str, dict[str, Any]] = {}

_KLINES_DIR    = Path("data/klines")
_SESSIONS_FILE = Path("data/backtest/sessions.json")

# ── 下载专用线程池（最多 5 并发 TqSdk 连接）──
_download_executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="kline-dl")

# ── 下载任务状态（内存，不持久化）──
_download_tasks: dict[str, dict] = {}

# ── 内置合约清单（源自「代码+名称+交易单位+最小变动价位+一跳盈亏.txt」）──
# CZCE 代码大写，其余交易所小写；PT/PD 归属 GFEX（以 txt 文件为准）
_BUILTIN_SYMBOLS: list[dict] = [
    # CZCE 郑商所
    {"exchange": "CZCE", "symbol": "CZCE.SA", "name": "纯碱"},
    {"exchange": "CZCE", "symbol": "CZCE.TA", "name": "PTA"},
    {"exchange": "CZCE", "symbol": "CZCE.SR", "name": "白糖"},
    {"exchange": "CZCE", "symbol": "CZCE.FG", "name": "玻璃"},
    {"exchange": "CZCE", "symbol": "CZCE.PF", "name": "短纤"},
    {"exchange": "CZCE", "symbol": "CZCE.SH", "name": "烧碱"},
    {"exchange": "CZCE", "symbol": "CZCE.PX", "name": "对二甲苯"},
    {"exchange": "CZCE", "symbol": "CZCE.MA", "name": "甲醇"},
    {"exchange": "CZCE", "symbol": "CZCE.CF", "name": "棉花"},
    {"exchange": "CZCE", "symbol": "CZCE.CY", "name": "棉纱"},
    {"exchange": "CZCE", "symbol": "CZCE.AP", "name": "苹果"},
    {"exchange": "CZCE", "symbol": "CZCE.CJ", "name": "红枣"},
    {"exchange": "CZCE", "symbol": "CZCE.RM", "name": "菜粕"},
    {"exchange": "CZCE", "symbol": "CZCE.RS", "name": "菜籽"},
    {"exchange": "CZCE", "symbol": "CZCE.OI", "name": "菜油"},
    {"exchange": "CZCE", "symbol": "CZCE.PK", "name": "花生"},
    {"exchange": "CZCE", "symbol": "CZCE.SF", "name": "硅铁"},
    {"exchange": "CZCE", "symbol": "CZCE.SM", "name": "锰硅"},
    {"exchange": "CZCE", "symbol": "CZCE.UR", "name": "尿素"},
    {"exchange": "CZCE", "symbol": "CZCE.PR", "name": "瓶片"},
    # DCE 大商所
    {"exchange": "DCE", "symbol": "DCE.i",  "name": "铁矿石"},
    {"exchange": "DCE", "symbol": "DCE.j",  "name": "焦炭"},
    {"exchange": "DCE", "symbol": "DCE.jm", "name": "焦煤"},
    {"exchange": "DCE", "symbol": "DCE.m",  "name": "豆粕"},
    {"exchange": "DCE", "symbol": "DCE.y",  "name": "豆油"},
    {"exchange": "DCE", "symbol": "DCE.p",  "name": "棕榈油"},
    {"exchange": "DCE", "symbol": "DCE.a",  "name": "豆一"},
    {"exchange": "DCE", "symbol": "DCE.b",  "name": "豆二"},
    {"exchange": "DCE", "symbol": "DCE.fb", "name": "纤维板"},
    {"exchange": "DCE", "symbol": "DCE.c",  "name": "玉米"},
    {"exchange": "DCE", "symbol": "DCE.cs", "name": "玉米淀粉"},
    {"exchange": "DCE", "symbol": "DCE.rr", "name": "粳米"},
    {"exchange": "DCE", "symbol": "DCE.l",  "name": "塑料"},
    {"exchange": "DCE", "symbol": "DCE.v",  "name": "PVC"},
    {"exchange": "DCE", "symbol": "DCE.pp", "name": "聚丙烯"},
    {"exchange": "DCE", "symbol": "DCE.bz", "name": "纯苯"},
    {"exchange": "DCE", "symbol": "DCE.eb", "name": "苯乙烯"},
    {"exchange": "DCE", "symbol": "DCE.eg", "name": "乙二醇"},
    {"exchange": "DCE", "symbol": "DCE.jd", "name": "鸡蛋"},
    {"exchange": "DCE", "symbol": "DCE.lh", "name": "生猪"},
    {"exchange": "DCE", "symbol": "DCE.pg", "name": "液化气"},
    {"exchange": "DCE", "symbol": "DCE.lg", "name": "原木"},
    # INE 能源中心
    {"exchange": "INE", "symbol": "INE.sc", "name": "原油"},
    {"exchange": "INE", "symbol": "INE.lu", "name": "低硫燃料油"},
    {"exchange": "INE", "symbol": "INE.nr", "name": "20号胶"},
    {"exchange": "INE", "symbol": "INE.bc", "name": "国际铜"},
    {"exchange": "INE", "symbol": "INE.ec", "name": "集运指数欧线"},
    # SHFE 上期所
    {"exchange": "SHFE", "symbol": "SHFE.cu", "name": "铜"},
    {"exchange": "SHFE", "symbol": "SHFE.al", "name": "铝"},
    {"exchange": "SHFE", "symbol": "SHFE.ao", "name": "氧化铝"},
    {"exchange": "SHFE", "symbol": "SHFE.zn", "name": "锌"},
    {"exchange": "SHFE", "symbol": "SHFE.pb", "name": "铅"},
    {"exchange": "SHFE", "symbol": "SHFE.ni", "name": "镍"},
    {"exchange": "SHFE", "symbol": "SHFE.sn", "name": "锡"},
    {"exchange": "SHFE", "symbol": "SHFE.ag", "name": "白银"},
    {"exchange": "SHFE", "symbol": "SHFE.au", "name": "黄金"},
    {"exchange": "SHFE", "symbol": "SHFE.rb", "name": "螺纹钢"},
    {"exchange": "SHFE", "symbol": "SHFE.hc", "name": "热轧卷板"},
    {"exchange": "SHFE", "symbol": "SHFE.ss", "name": "不锈钢"},
    {"exchange": "SHFE", "symbol": "SHFE.fu", "name": "燃料油"},
    {"exchange": "SHFE", "symbol": "SHFE.bu", "name": "沥青"},
    {"exchange": "SHFE", "symbol": "SHFE.ru", "name": "橡胶"},
    {"exchange": "SHFE", "symbol": "SHFE.br", "name": "丁二烯橡胶"},
    {"exchange": "SHFE", "symbol": "SHFE.sp", "name": "纸浆"},
    # GFEX 广期所（含铂pt、钯pd，以 txt 文件为准）
    {"exchange": "GFEX", "symbol": "GFEX.si", "name": "工业硅"},
    {"exchange": "GFEX", "symbol": "GFEX.lc", "name": "碳酸锂"},
    {"exchange": "GFEX", "symbol": "GFEX.ps", "name": "多晶硅"},
    {"exchange": "GFEX", "symbol": "GFEX.pt", "name": "铂"},
    {"exchange": "GFEX", "symbol": "GFEX.pd", "name": "钯"},
]


# ─────────────────────────────────────────────────────────────
# 会话持久化（跨重启保留模拟记录）
# ─────────────────────────────────────────────────────────────

def _save_sessions() -> None:
    """将 _sessions 序列化到磁盘（不含 _df_cache）。"""
    try:
        _SESSIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
        data = {
            k: {kk: vv for kk, vv in v.items() if kk != "_df_cache"}
            for k, v in _sessions.items()
        }
        _SESSIONS_FILE.write_text(
            json.dumps(data, ensure_ascii=False, default=str), encoding="utf-8"
        )
    except Exception as e:
        print(f"[backtest] 会话保存失败: {e}")


def _load_sessions() -> None:
    """启动时从磁盘恢复 _sessions。"""
    if not _SESSIONS_FILE.exists():
        return
    try:
        data = json.loads(_SESSIONS_FILE.read_text(encoding="utf-8"))
        _sessions.update(data)
        print(f"[backtest] 已恢复 {len(data)} 条历史会话")
    except Exception as e:
        print(f"[backtest] 会话加载失败（将忽略）: {e}")


# 模块加载时立即恢复
_load_sessions()


# ─────────────────────────────────────────────────────────────
# 页面
# ─────────────────────────────────────────────────────────────

@backtest_router.get("", response_class=HTMLResponse)
async def backtest_page():
    html_path = Path(__file__).parent.parent / "backtest.html"
    if not html_path.exists():
        return HTMLResponse("<h2>backtest.html 未找到</h2>", status_code=404)
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


# ─────────────────────────────────────────────────────────────
# 已下载合约列表
# ─────────────────────────────────────────────────────────────

@backtest_router.get("/api/symbols")
async def get_symbols():
    """列出 data/klines/ 下已下载的合约。"""
    symbols = engine.list_available_symbols(_KLINES_DIR)
    return JSONResponse({"symbols": symbols})


@backtest_router.get("/api/strategies")
async def get_strategies():
    """返回所有已注册策略 ID 列表。"""
    return JSONResponse({"strategies": list_strategies()})


# ─────────────────────────────────────────────────────────────
# K 线数据（供前端图表渲染）
# ─────────────────────────────────────────────────────────────

@backtest_router.get("/api/klines")
async def get_klines(
    symbol:           str = Query(..., description="合约代码，如 SHFE.rb"),
    start:            str = Query(None, description="开始日期 YYYY-MM-DD"),
    end:              str = Query(None, description="结束日期 YYYY-MM-DD"),
    interval_minutes: int = Query(5, description="K线周期（分钟）"),
    limit:            int = Query(2000, description="最多返回根数"),
):
    """
    返回 K 线数组，格式供 TradingView Lightweight Charts 使用：
    [{ time, open, high, low, close, volume, ema20 }, ...]
    time 为 Unix 秒（K线收盘时刻）。
    """
    duration_sec = interval_minutes * 60

    start_dt = datetime.strptime(start, "%Y-%m-%d") if start else None
    end_dt   = (datetime.strptime(end, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
                if end else None)

    try:
        df = engine.load_klines(symbol, start_dt, end_dt, duration_sec, _KLINES_DIR)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if df.empty:
        return JSONResponse({"klines": [], "total": 0})

    # 只取最近 limit 根（避免传输过大）
    df = df.tail(limit).reset_index(drop=True)

    # 计算 EMA20（用于前端图表叠加）
    closes = df["close"].astype(float)
    ema20  = closes.ewm(span=20, adjust=False).mean().round(2)

    # 向量化构建（比 iterrows 快 5-10 倍）
    df_out = df.copy()
    df_out["open"]   = df_out["open"].astype(float).round(2)
    df_out["high"]   = df_out["high"].astype(float).round(2)
    df_out["low"]    = df_out["low"].astype(float).round(2)
    df_out["close"]  = df_out["close"].astype(float).round(2)
    df_out["volume"] = df_out["volume"].astype(float).fillna(0).astype(int)
    df_out["ema20"]  = ema20.values
    df_out["time"]   = df_out["time_unix"].astype(int) + 28800   # UTC → CST 预移 +8h，前端直接渲染正确时区
    df_out["datetime_ns"] = df_out["datetime"].astype(int)

    keep = ["time", "open", "high", "low", "close", "volume", "ema20", "datetime_ns"]
    records = df_out[keep].to_dict("records")

    return JSONResponse({
        "klines": records,
        "total":  len(records),
        "symbol": symbol,
        "interval_minutes": interval_minutes,
    })


# ─────────────────────────────────────────────────────────────
# 预计算结构（点击K线后快速展示，不调用AI）
# ─────────────────────────────────────────────────────────────

class PrecomputeRequest(BaseModel):
    symbol:           str
    bar_time:         int
    interval_minutes: int = 5
    lookback:         int = 250
    strategy_id:      str = ""


@backtest_router.post("/api/precompute")
async def precompute_structure(req: PrecomputeRequest):
    """返回某K线时间点的预计算价格结构（不调用AI）。"""
    duration_sec = req.interval_minutes * 60

    try:
        df = engine.load_klines(req.symbol, duration_seconds=duration_sec, data_dir=_KLINES_DIR)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if df.empty:
        raise HTTPException(status_code=404, detail="无历史数据")

    matches = df[df["time_unix"] == req.bar_time]
    if matches.empty:
        mask = (df["time_unix"] >= req.bar_time - 1) & (df["time_unix"] <= req.bar_time + 1)
        matches = df[mask]
    if matches.empty:
        raise HTTPException(status_code=404, detail=f"未找到K线：{req.bar_time}")

    bar_index = int(matches.index[0])
    market_data = engine.build_market_data_at(
        symbol=req.symbol,
        df_full=df,
        bar_index=bar_index,
        duration_seconds=duration_sec,
        lookback=req.lookback,
        ai_kline_count=1,   # 只要结构，不需要AI K线格式化
        strategy=get_strategy_by_id(req.strategy_id) if req.strategy_id else None,
    )

    _SKIP = {
        "klines", "symbol", "product_name", "is_trading_time",
        "is_session_first_bar", "is_session_last_bar", "pending", "error",
        "近期波段高点", "近期波段低点",
    }
    # 前缀 _ 的内部字段默认不传，但前端展示需要的几个例外
    _EXPOSE_PRIVATE = {"_趋势阈值", "_ema斜率_10根"}
    out = {k: v for k, v in market_data.items()
           if (not k.startswith("_") or k in _EXPOSE_PRIVATE) and k not in _SKIP}
    # 确保关键子 dict 不丢失
    out["quote"] = market_data.get("quote", {})
    out["ema20"] = market_data.get("ema20", {})
    out["timestamp"] = market_data.get("timestamp", "")

    # 用 build_features 的结果覆盖测量目标（含 fallback 波段点补充）
    try:
        features = get_strategy_by_id(req.strategy_id).build_features(
            market_data, lang="zh", klines_completed=None
        )
        out["测量目标向上"] = features.get("测量目标向上", [])
        out["测量目标向下"] = features.get("测量目标向下", [])
        out["信号棒评级"] = features.get("_last_bar_rating")
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning("build_features failed in precompute: %s", e)

    return JSONResponse(out)


# ─────────────────────────────────────────────────────────────
# AI 分析
# ─────────────────────────────────────────────────────────────

class AnalyzeRequest(BaseModel):
    symbol:           str   # 合约代码，如 KQ.m@SHFE.rb 或 SHFE.rb
    bar_time:         int   # K线收盘时刻的 Unix 秒（前端 Lightweight Charts 的 time 字段）
    model_id:         str   # AI 模型 ID
    interval_minutes: int = 5
    lookback:         int = 250
    ai_kline_count:   int = 40
    strategy_id:      str = ""  # 留空则使用当前活跃策略


@backtest_router.post("/api/analyze")
async def analyze(req: AnalyzeRequest):
    """
    对选定的 K 线时间点发起 AI 分析。

    流程：
    1. 加载历史 K 线
    2. 定位到 bar_time 对应的 K 线索引
    3. 重建 market_data 字典
    4. 调用 analyzers.analyze()
    5. 返回分析结果 + analysis_id（供后续模拟使用）
    """
    import analyzers

    duration_sec = req.interval_minutes * 60
    product      = engine.normalize_symbol(req.symbol)

    # 加载全量数据（不过滤时间，便于定位 bar_index）
    try:
        df = engine.load_klines(req.symbol, duration_seconds=duration_sec, data_dir=_KLINES_DIR)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if df.empty:
        raise HTTPException(status_code=404, detail=f"无历史数据：{product}")

    # 定位 bar_time 对应的索引（time_unix 是 K 线收盘时刻）
    matches = df[df["time_unix"] == req.bar_time]
    if matches.empty:
        # 允许 ±1 秒误差
        mask = (df["time_unix"] >= req.bar_time - 1) & (df["time_unix"] <= req.bar_time + 1)
        matches = df[mask]
    if matches.empty:
        raise HTTPException(
            status_code=404,
            detail=f"未找到时间戳 {req.bar_time} 对应的 K 线（检查时区或数据是否已下载）"
        )
    bar_index = int(matches.index[0])

    # 重建 market_data（使用前端选定策略，或回退到活跃策略）
    _strategy = get_strategy_by_id(req.strategy_id) if req.strategy_id else None
    market_data = engine.build_market_data_at(
        symbol=req.symbol,
        df_full=df,
        bar_index=bar_index,
        duration_seconds=duration_sec,
        lookback=req.lookback,
        ai_kline_count=req.ai_kline_count,
        strategy=_strategy,
    )

    if market_data.get("error") and not market_data.get("klines"):
        raise HTTPException(status_code=400, detail=market_data["error"])

    # 震荡过滤：预计算显示交易区间或窄幅区间时跳过 AI 分析
    trend = market_data.get("趋势方向", "")
    is_tight = market_data.get("是否窄幅区间", False)
    if trend == "震荡" or is_tight:
        skip_reason = "窄幅区间" if is_tight else "交易区间（震荡）"
        return JSONResponse({
            "skipped":     True,
            "skip_reason": skip_reason,
            "趋势方向":    trend,
            "是否窄幅区间": is_tight,
        })

    # 检查模型是否可用
    enabled_models = {m["id"] for m in config.get_available_models() if m.get("enabled")}
    if req.model_id not in enabled_models:
        raise HTTPException(
            status_code=400,
            detail=f"模型 {req.model_id} 未配置 API Key，可用模型：{sorted(enabled_models)}"
        )

    # 调用 AI
    try:
        result = await analyzers.analyze(
            model_id=req.model_id,
            market_data=market_data,
            history=[],      # 回测中无历史上下文
            positions=[],    # 无持仓
            data_dir_override="data/backtest",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI 分析失败：{e}")

    if result.get("error"):
        raise HTTPException(status_code=500, detail=result["error"])

    # 生成 analysis_id，保存会话数据（含 df 缓存，供 simulate 复用，避免重复读盘）
    analysis_id = str(uuid.uuid4())
    _sessions[analysis_id] = {
        "analysis_id":      analysis_id,
        "symbol":           req.symbol,
        "bar_time":         req.bar_time,
        "bar_index":        bar_index,
        "model_id":         req.model_id,
        "strategy_id":      req.strategy_id or "",
        "interval_minutes": req.interval_minutes,
        "analysis":         result,
        "simulation":       None,
        "created_at":       datetime.now().isoformat(),
        "_df_cache":        df,   # 缓存已加载的 DataFrame，simulate 接口直接复用
    }

    # 内存防护：超过 500 条时清理最旧的一半
    if len(_sessions) > 500:
        oldest_keys = sorted(_sessions, key=lambda k: _sessions[k]["created_at"])[:250]
        for k in oldest_keys:
            del _sessions[k]

    return JSONResponse({
        "analysis_id": analysis_id,
        "bar_time":    req.bar_time,
        "symbol":      req.symbol,
        "model_id":    req.model_id,
        **{k: v for k, v in result.items() if not k.startswith("_")},
    })


# ─────────────────────────────────────────────────────────────
# 盈亏模拟
# ─────────────────────────────────────────────────────────────

class SimulateRequest(BaseModel):
    analysis_id:  str
    direction:    str          # "LONG" | "SHORT"
    entry_price:  float
    stop_loss:    float
    take_profit:  float | None = None
    price_tick:   float = 1.0
    max_bars:     int   = 100


@backtest_router.post("/api/simulate")
async def simulate(req: SimulateRequest):
    """
    根据 AI 建议执行 K 线级别追踪止损模拟，计算 P&L。
    """
    session = _sessions.get(req.analysis_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"找不到 analysis_id：{req.analysis_id}")

    symbol       = session["symbol"]
    bar_index    = session["bar_index"]
    interval_min = session["interval_minutes"]
    duration_sec = interval_min * 60

    # 复用 analyze 时已缓存的 DataFrame（避免重复读盘）
    df = session.get("_df_cache")
    if df is None:
        try:
            df = engine.load_klines(symbol, duration_seconds=duration_sec, data_dir=_KLINES_DIR)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # df_future：从下一根 K 线开始（模拟从入场后的下一根执行）
    df_future = df.iloc[bar_index + 1:].reset_index(drop=True)

    if df_future.empty:
        raise HTTPException(status_code=400, detail="入场点之后没有历史 K 线数据，无法模拟")

    sim_result = engine.simulate_trade(
        direction=req.direction,
        entry_price=req.entry_price,
        stop_loss=req.stop_loss,
        take_profit=req.take_profit,
        df_future=df_future,
        price_tick=req.price_tick,
        max_bars=req.max_bars,
    )

    # 获取出场 K 线时间戳（供前端标注）
    exit_bar_time = None
    if sim_result["exit_bar_index"] >= 0 and sim_result["exit_bar_index"] < len(df_future):
        exit_row = df_future.iloc[sim_result["exit_bar_index"]]
        exit_bar_time = int(exit_row["time_unix"])

    # 换算金额：pnl_ticks × 每手每跳盈亏
    tick_value = engine.get_tick_value(symbol)
    pnl_cny = round(sim_result["pnl_ticks"] * tick_value, 2) if tick_value is not None else None

    # 保存模拟结果到会话
    sim_with_meta = {
        **sim_result,
        "direction":     req.direction,
        "entry_price":   req.entry_price,
        "stop_loss":     req.stop_loss,
        "take_profit":   req.take_profit,
        "exit_bar_time": exit_bar_time,
        "tick_value":    tick_value,
        "pnl_cny":       pnl_cny,
    }
    session["simulation"] = sim_with_meta
    _save_sessions()

    return JSONResponse({
        "analysis_id":   req.analysis_id,
        "exit_bar_time": exit_bar_time,
        **sim_with_meta,
    })


# ─────────────────────────────────────────────────────────────
# 指定合约的历史模拟（供图表恢复标记）
# ─────────────────────────────────────────────────────────────

@backtest_router.get("/api/simulations")
async def get_simulations(
    symbol:           str = Query(..., description="合约代码，如 SHFE.rb"),
    interval_minutes: int = Query(5),
):
    """返回指定合约+周期的所有历史模拟，供前端重建图表标记。"""
    result = []
    for sid, session in _sessions.items():
        if session.get("symbol") != symbol:
            continue
        if session.get("interval_minutes") != interval_minutes:
            continue
        analysis = session.get("analysis") or {}
        sim      = session.get("simulation")
        result.append({
            "analysis_id": sid,
            "bar_time":    session.get("bar_time"),   # UTC time_unix（前端需 +28800 转 CST）
            "model_id":    session.get("model_id"),
            "created_at":  session.get("created_at"),
            "操作建议":    analysis.get("操作建议"),
            "市场状态":    analysis.get("市场状态"),
            "设置类型":    analysis.get("设置类型"),
            "风险等级":    analysis.get("风险等级"),
            "核心逻辑":    analysis.get("核心逻辑"),
            "入场价":      analysis.get("入场价"),
            "止损价":      analysis.get("止损价"),
            "止盈价":      analysis.get("止盈价"),
            "simulation":  sim,
        })
    # 按时间排序
    result.sort(key=lambda x: x.get("bar_time") or 0)
    return JSONResponse({"simulations": result})


# ─────────────────────────────────────────────────────────────
# 历史交易记录（实盘+模拟，供回测K线图显示标记）
# ─────────────────────────────────────────────────────────────

@backtest_router.get("/api/history_trades")
async def get_history_trades(
    symbol: str = Query(..., description="合约代码，如 SHFE.rb"),
):
    """返回指定合约的历史交易记录（来自 data/sim/trades/ 和 data/live/trades/）。"""
    norm_sym = engine.normalize_symbol(symbol)
    records: list[dict] = []

    for data_root in [Path("data/sim"), Path("data/live")]:
        trades_dir = data_root / "trades"
        if not trades_dir.exists():
            continue
        for fpath in sorted(trades_dir.glob("*.jsonl")):
            try:
                lines = fpath.read_text(encoding="utf-8").splitlines()
            except Exception:
                continue
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if engine.normalize_symbol(rec.get("symbol", "")) != norm_sym:
                    continue
                records.append({
                    "time":         rec.get("time"),          # CST ISO 字符串
                    "model":        rec.get("model"),
                    "direction":    rec.get("direction"),     # "买"/"卖"
                    "offset":       rec.get("offset"),        # "开仓"/"平仓"
                    "price":        rec.get("price"),
                    "close_profit": rec.get("close_profit"),  # 仅平仓记录有
                    "type":         rec.get("type"),          # "open"/"trade"
                })

    return JSONResponse({"trades": records, "symbol": symbol, "count": len(records)})


# ─────────────────────────────────────────────────────────────
# 会话统计
# ─────────────────────────────────────────────────────────────

@backtest_router.get("/api/stats")
async def get_stats(symbol: str | None = None):
    """返回当前会话中所有已完成模拟的统计数据。symbol 不为空时只返回该品种。"""
    trades = []
    for sid, session in _sessions.items():
        if symbol and session.get("symbol") != symbol:
            continue
        sim = session.get("simulation")
        if sim:
            trades.append({
                "analysis_id":   sid,
                "symbol":        session["symbol"],
                "model_id":      session["model_id"],
                "bar_time":      session["bar_time"],
                "direction":     sim.get("direction"),
                "entry_price":   sim.get("entry_price"),
                "exit_price":    sim.get("exit_price"),
                "exit_type":     sim.get("exit_type"),
                "pnl_r":         sim.get("pnl_r"),
                "pnl_ticks":     sim.get("pnl_ticks"),
                "pnl_cny":       sim.get("pnl_cny"),
                "tick_value":    sim.get("tick_value"),
                "bars_held":     sim.get("bars_held"),
                "ambiguous":     sim.get("ambiguous"),
                "created_at":    session.get("created_at"),
                # 分析摘要
                "操作建议":      (session.get("analysis") or {}).get("操作建议"),
                "市场状态":      (session.get("analysis") or {}).get("市场状态"),
                "设置类型":      (session.get("analysis") or {}).get("设置类型"),
            })

    stats = engine.compute_stats(trades)
    return JSONResponse({
        "stats":  stats,
        "trades": trades,
    })


# ─────────────────────────────────────────────────────────────
# 删除单条分析
# ─────────────────────────────────────────────────────────────

@backtest_router.delete("/api/session/{analysis_id}")
async def delete_analysis(analysis_id: str):
    """删除单条分析记录（含模拟结果）。"""
    if analysis_id not in _sessions:
        raise HTTPException(status_code=404, detail=f"找不到 analysis_id：{analysis_id}")
    del _sessions[analysis_id]
    _save_sessions()
    return JSONResponse({"ok": True})


# ─────────────────────────────────────────────────────────────
# 清空会话
# ─────────────────────────────────────────────────────────────

@backtest_router.delete("/api/session")
async def clear_session(symbol: str | None = None):
    """清空会话记录。symbol 不为空时只清除该品种；否则清空全部。"""
    if symbol:
        keys = [k for k, v in _sessions.items() if v.get("symbol") == symbol]
        for k in keys:
            del _sessions[k]
        msg = f"已清空 {symbol} 的 {len(keys)} 条记录"
    else:
        _sessions.clear()
        msg = "会话已全部清空"
    _save_sessions()
    return JSONResponse({"ok": True, "message": msg})


# ─────────────────────────────────────────────────────────────
# 批量下载 K 线
# ─────────────────────────────────────────────────────────────

class DownloadRequest(BaseModel):
    symbols:          list[str] = []   # 空列表 = 按 exchange_filter 或全部
    exchange_filter:  str        = ""  # "SHFE" / "DCE" / ... / "" = 全部
    interval_minutes: int        = 5
    bars:             int        = 100


async def _run_download_batch(task_id: str, items: list[dict],
                               interval_minutes: int, bars: int) -> None:
    """并发下载多个合约的 K 线，最多 5 个同时进行。"""
    task = _download_tasks[task_id]
    loop = asyncio.get_running_loop()
    sem  = asyncio.Semaphore(5)

    async def _dl_one(item: dict) -> None:
        async with sem:
            if task.get("cancelled"):
                item["status"] = "cancelled"
                return
            item["status"] = "running"
            try:
                saved = await loop.run_in_executor(
                    _download_executor,
                    lambda sym=item["symbol"]: _download_klines(
                        sym, interval_minutes,
                        datetime(2000, 1, 1), datetime.now(),
                        _KLINES_DIR, bars=bars,
                    )
                )
                item["status"]     = "ok"
                item["files_saved"] = len(saved)
            except Exception as exc:
                item["status"] = "error"
                item["error"]  = str(exc)[:300]
            finally:
                task["done"]   = sum(1 for x in task["items"] if x["status"] == "ok")
                task["failed"] = sum(1 for x in task["items"] if x["status"] == "error")

    await asyncio.gather(*[_dl_one(it) for it in items])
    task["status"] = "done"


@backtest_router.get("/api/builtin-symbols")
async def get_builtin_symbols():
    """返回内置合约清单，附带是否已下载标记。"""
    result = []
    for entry in _BUILTIN_SYMBOLS:
        sym     = entry["symbol"]
        sym_dir = _KLINES_DIR / sym
        downloaded = sym_dir.exists() and any(sym_dir.rglob("*.parquet"))
        result.append({
            "symbol":     sym,
            "exchange":   entry["exchange"],
            "name":       entry["name"],
            "downloaded": downloaded,
        })
    return JSONResponse({"symbols": result})


@backtest_router.post("/api/download")
async def start_download(req: DownloadRequest):
    """启动批量下载任务，立即返回 task_id，后台并发执行。"""
    # 确定要下载的合约列表
    if req.symbols:
        sym_set = set(req.symbols)
        targets = [e for e in _BUILTIN_SYMBOLS if e["symbol"] in sym_set]
    elif req.exchange_filter:
        targets = [e for e in _BUILTIN_SYMBOLS if e["exchange"] == req.exchange_filter]
    else:
        targets = list(_BUILTIN_SYMBOLS)

    if not targets:
        raise HTTPException(status_code=400, detail="没有匹配的合约")

    # 清理旧任务（超过 20 个时删前 10 个已完成的）
    if len(_download_tasks) >= 20:
        done_keys = [k for k, v in _download_tasks.items()
                     if v["status"] in ("done", "error", "cancelled")]
        for k in done_keys[:10]:
            del _download_tasks[k]

    task_id = str(uuid.uuid4())
    items   = [{"symbol": e["symbol"], "name": e["name"],
                "status": "pending", "files_saved": 0, "error": None}
               for e in targets]
    _download_tasks[task_id] = {
        "task_id":  task_id,
        "status":   "running",
        "total":    len(items),
        "done":     0,
        "failed":   0,
        "items":    items,
        "created_at": datetime.now().isoformat(),
    }

    asyncio.create_task(_run_download_batch(
        task_id, items, req.interval_minutes, req.bars
    ))

    return JSONResponse({
        "task_id": task_id,
        "total":   len(items),
        "message": f"下载任务已启动，共 {len(items)} 个合约",
    })


@backtest_router.get("/api/download/progress/{task_id}")
async def get_download_progress(task_id: str):
    """查询批量下载任务进度。"""
    task = _download_tasks.get(task_id)
    if task is None:
        return JSONResponse({"status": "not_found", "task_id": task_id})
    return JSONResponse(task)


@backtest_router.delete("/api/download/{task_id}")
async def cancel_download(task_id: str):
    """标记任务为取消（正在运行的当前品种下载完后停止）。"""
    task = _download_tasks.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    task["cancelled"] = True
    return JSONResponse({"ok": True, "task_id": task_id})
