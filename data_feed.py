"""
data_feed.py — 数据层

线程模型：
  - TqApi 在独立线程中创建和运行
  - 连接成功后立即订阅默认合约（不等循环）
  - wait_update(deadline) 每2秒超时返回，确保能处理新订阅请求
  - fetch() 只读内存，不调用任何 tqsdk 函数
"""

import asyncio
import json
import copy
import pathlib
import threading
import logging
import time as time_module
from datetime import datetime, timedelta
from typing import Any

import math
import numpy as np
import pandas as pd

import config
from strategies import get_strategy as _get_strategy

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# 通用工具
# ─────────────────────────────────────────────────────────────


def _safe_vol(v) -> int:
    """
    将 tqsdk 持仓量字段安全转换为 int（Bug-3 修复）。
    tqsdk 在网络抖动或数据未到时持仓量可能为 NaN，
    直接 int(NaN) 会抛 ValueError，在守卫平仓路径中被外层 except 吞掉，
    导致止损/止盈守卫触发时静默不平仓。
    NaN / 负数 / 非数字 均归零处理。
    """
    try:
        f = float(v)
        return int(f) if not np.isnan(f) and f > 0 else 0
    except (ValueError, TypeError):
        return 0

# ── 价格行为结构字段默认值（由活跃策略提供，保留模块级名称供兼容）──
PRICE_STRUCTURE_DEFAULTS: dict = _get_strategy().price_structure_defaults

# ─────────────────────────────────────────────────────────────
# TqFeed
# ─────────────────────────────────────────────────────────────


# ── 品种中文名映射 ──────────────────────────────────────────
SYMBOL_NAMES: dict[str, str] = {
    # 上期所 SHFE
    "SHFE.rb":"螺纹钢","SHFE.hc":"热轧卷板","SHFE.ss":"不锈钢","SHFE.wr":"线材",
    "SHFE.cu":"铜","SHFE.al":"铝","SHFE.zn":"锌","SHFE.pb":"铅",
    "SHFE.ni":"镍","SHFE.sn":"锡","SHFE.au":"黄金","SHFE.ag":"白银",
    "SHFE.ru":"天然橡胶","SHFE.bu":"沥青","SHFE.fu":"燃料油","SHFE.sp":"纸浆",
    "SHFE.ao":"氧化铝","SHFE.bc":"国际铜","SHFE.br":"丁二烯橡胶",
    # 大商所 DCE
    "DCE.i":"铁矿石","DCE.j":"焦炭","DCE.jm":"焦煤","DCE.a":"豆一",
    "DCE.b":"豆二","DCE.m":"豆粕","DCE.y":"豆油","DCE.p":"棕榈油",
    "DCE.c":"玉米","DCE.cs":"玉米淀粉","DCE.eg":"乙二醇","DCE.l":"塑料",
    "DCE.v":"PVC","DCE.pp":"聚丙烯","DCE.eb":"苯乙烯","DCE.rr":"粳稻",
    "DCE.pg":"LPG","DCE.lh":"生猪","DCE.jd":"鸡蛋",
    "DCE.bb":"胶合板","DCE.fb":"纤维板","DCE.bz":"纯苯","DCE.lg":"原木",
    # 郑商所 CZCE
    "CZCE.TA":"PTA","CZCE.MA":"甲醇","CZCE.PX":"对二甲苯","CZCE.PL":"丙烯",
    "CZCE.SA":"纯碱","CZCE.SH":"烧碱","CZCE.FG":"玻璃","CZCE.PF":"短纤",
    "CZCE.PR":"瓶片","CZCE.SR":"白糖","CZCE.CF":"棉花","CZCE.CY":"棉纱",
    "CZCE.AP":"苹果","CZCE.CJ":"红枣","CZCE.PK":"花生","CZCE.OI":"菜籽油",
    "CZCE.RM":"菜籽粕","CZCE.RS":"菜籽","CZCE.WH":"强麦","CZCE.PM":"普麦",
    "CZCE.RI":"早籼稻","CZCE.JR":"粳稻","CZCE.LR":"晚籼稻",
    "CZCE.SF":"硅铁","CZCE.SM":"锰硅","CZCE.UR":"尿素",
    # 能源中心 INE
    "INE.sc":"原油","INE.lu":"低硫燃料油","INE.nr":"20号胶","INE.bc":"国际铜",
    "INE.ec":"集运指数欧线",
    # 广期所 GFEX
    "GFEX.si":"工业硅","GFEX.lc":"碳酸锂","GFEX.ps":"多晶硅","GFEX.pt":"铂","GFEX.pd":"钯",
    # 中金所 CFFEX
    "CFFEX.IF":"沪深300股指","CFFEX.IC":"中证500股指","CFFEX.IM":"中证1000股指",
    "CFFEX.IH":"上证50股指","CFFEX.T":"10年期国债","CFFEX.TL":"30年期国债",
    "CFFEX.TF":"5年期国债","CFFEX.TS":"2年期国债",
}

def get_symbol_name(symbol: str) -> str:
    """从主力连续或实际合约代码提取品种中文名"""
    import re
    # KQ.m@SHFE.rb → SHFE.rb
    m = re.search(r'@([A-Z]+\.\w+)', symbol)
    key = m.group(1) if m else symbol
    # strip contract month: SHFE.rb2509 → SHFE.rb
    base = re.sub(r'\d+$', '', key)
    return SYMBOL_NAMES.get(base, base)

class TqFeed:
    def __init__(self):
        self._api        = None
        self._thread     = None
        self._ready      = threading.Event()
        self._stop_event = threading.Event()
        self._lock       = threading.Lock()

        self._connected = False
        self._error_msg = ""

        self._quotes:     dict[str, Any] = {}
        self._klines_obj: dict[str, Any] = {}
        self._subscribed: set[str]       = set()

        # 待订阅队列（任意线程写入，tqsdk 线程消费）
        self._pending_sub: set[str] = set()
        # 待执行交易指令队列（insert_order / cancel_order）
        self._pending_orders: list[dict] = []
        # 开仓订单成交监听：成交后用实际价更新守卫 entry_price
        self._fill_trackers: list[dict] = []  # [{order, model_id, guard_sym}]
        # 守卫平仓追踪：下单后每2s检测持仓是否清零，未清零则撤单重下，清零后调 on_guard_close
        self._pending_close_orders: list[dict] = []
        # 止盈止损守卫：{ symbol: {direction, stop_loss, take_profit} }
        # direction: "LONG" | "SHORT"
        self._guards: dict[str, dict] = {}
        self._guards_dirty = False  # 追踪止损变动标记，由定时任务批量写盘
        # 各品种最后K线datetime（纳秒），变化即代表新K线出现（tqsdk 窗口固定250行，不能用 len 检测）
        self._last_kline_dt: dict[str, int] = {}
        # 回调列表：(symbol, asyncio.loop) → main.py注册，K线收盘时通知
        self._kline_close_callbacks: list  = []
        # 行情更新回调列表（主循环每次 wait_update 后调用）
        self._on_update_callbacks: list = []
        self._loop = None  # asyncio 事件循环引用（由 main.py 注入）
        # 守卫触发平仓回调：on_guard_close(model_id, symbol) → coroutine
        # 由 main.py 注入，用于守卫平仓后在 asyncio 线程记录 PnL
        self.on_guard_close  = None
        # 追踪止损更新回调：on_guard_update(model_id, symbol, guard_dict) → coroutine
        # 由 main.py 注入，止损移动时推送 SSE 通知前端
        self.on_guard_update = None

    def start(self):
        self._thread = threading.Thread(
            target=self._run_loop, name="tqsdk-thread", daemon=True
        )
        self._thread.start()
        logger.info("TqFeed: 后台线程已启动")

    def stop(self):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=8)
        # api.close() is called inside _run_loop's finally block (within tqsdk's own thread).
        # Calling it here from the main async context causes Python 3.12+ asyncio task
        # conflicts: close() grabs get_event_loop() → main uvicorn loop → tries to cancel
        # tqsdk tasks on the wrong loop.

    def is_ready(self) -> bool:
        return self._connected

    def wait_ready(self, timeout: float = 30.0) -> bool:
        return self._ready.wait(timeout=timeout)

    def _save_guards_async(self, snapshot: dict) -> None:
        """将守卫快照异步写入 guards.json，不阻塞 tqsdk 线程。snapshot 必须已是深拷贝。"""
        async def _write(data=snapshot):
            try:
                pathlib.Path(config.get_data_root()).joinpath("guards.json").write_text(
                    json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
                )
            except Exception:
                pass
        if self._loop:
            asyncio.run_coroutine_threadsafe(_write(), self._loop)
        else:
            try:
                pathlib.Path(config.get_data_root()).joinpath("guards.json").write_text(
                    json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8"
                )
            except Exception:
                pass

    async def _flush_guards_loop(self) -> None:
        """每5秒检查 dirty flag，批量写盘一次，避免追踪止损每 tick 写盘。"""
        while True:
            await asyncio.sleep(5)
            if self._guards_dirty:
                self._guards_dirty = False
                with self._lock:
                    snap = copy.deepcopy(self._guards)
                self._save_guards_async(snap)

    def _do_subscribe(self, symbol: str):
        """在 tqsdk 线程内执行订阅，调用方保证只在该线程调用。"""
        if symbol in self._subscribed:
            return
        try:
            self._quotes[symbol]     = self._api.get_quote(symbol)
            self._klines_obj[symbol] = self._api.get_kline_serial(symbol, config.KLINE_INTERVAL_SECONDS, 250)
            self._subscribed.add(symbol)
            logger.info(f"TqFeed: 已订阅 {symbol}")
        except Exception as e:
            logger.error(f"TqFeed: 订阅 {symbol} 失败 — {e}")

    def _run_loop(self):
        try:
            from tqsdk import TqApi, TqAuth

            logger.info("TqFeed: 正在连接天勤...")
            auth = TqAuth(config.TQ_USERNAME, config.TQ_PASSWORD)
            if config.TQ_ENABLE_TRADING:
                if config.is_live_mode():
                    from tqsdk import TqAccount
                    self._api = TqApi(
                        TqAccount(config.TQ_BROKER, config.TQ_ACCOUNT_ID, config.TQ_TRADE_PASSWORD),
                        auth=auth,
                    )
                    logger.info(f"TqFeed: 实盘交易模式（{config.TQ_BROKER} {config.TQ_ACCOUNT_ID}）✅")
                else:
                    from tqsdk import TqKq
                    self._api = TqApi(TqKq(), auth=auth)
                    logger.info("TqFeed: 模拟交易模式（TqKq）✅")
            else:
                self._api = TqApi(auth=auth)
            self._connected = True
            logger.info("TqFeed: 天勤连接成功 ✅")

            # ── 连接后立即订阅默认合约（在第一次 wait_update 之前）────
            # 这样非交易时间也能获取到历史/收盘数据
            default_sym = config.DEFAULT_SYMBOL
            self._do_subscribe(default_sym)
            self._ready.set()

            _last_notify_ts = 0.0  # 行情更新回调节流时间戳（TqSdk线程局部）
            while not self._stop_event.is_set():

                # ── 处理待订阅队列 ────────────────────────────────────
                with self._lock:
                    pending = self._pending_sub - self._subscribed
                    self._pending_sub.clear()
                    pending_orders = list(self._pending_orders)
                    self._pending_orders.clear()

                for symbol in pending:
                    self._do_subscribe(symbol)

                # ── 守卫平仓追踪：每2s检测持仓是否清零，未成交则撤单重下 ─────────────────────
                # 放在守卫检测之前：守卫本轮新加入的条目，等下一次 wait_update 同步成交状态后再处理，
                # 避免订单刚发出尚未收到回报时就立即撤单（服务器已成交但本地仍显示 ALIVE）。
                if self._pending_close_orders:
                    _still_pending = []
                    for _pco in list(self._pending_close_orders):
                        try:
                            _asym = _pco["actual_sym"]
                            _dir  = _pco["direction"]
                            _gvol = _pco["guard_vol"]
                            _ppos = self._api.get_position(_asym)
                            if _dir == "LONG":
                                _rem = _safe_vol(_ppos.volume_long_today) + _safe_vol(_ppos.volume_long_his)
                            else:
                                _rem = _safe_vol(_ppos.volume_short_today) + _safe_vol(_ppos.volume_short_his)

                            if _rem == 0:
                                # 持仓已清零，调 on_guard_close 写历史和 PnL，追踪结束
                                asyncio.run_coroutine_threadsafe(
                                    self.on_guard_close(_pco["model_id"], _pco["guard_symbol"], _pco["close_info"]),
                                    self._loop,
                                )
                                continue  # 不加入 _still_pending

                            # 持仓未清零，按委托状态分三种情况处理
                            _orders    = _pco["orders"]
                            # volume_left == 0 的 ALIVE 单已全部成交，等 tqsdk 状态翻转，不触发撤单重下
                            _alive     = [o for o in _orders if getattr(o, "status", "") == "ALIVE"
                                          and getattr(o, "volume_left", 1) > 0]
                            # FINISHED 但 volume_left > 0 = 被撤销或拒单（未完全成交）
                            _cancelled = [o for o in _orders
                                          if getattr(o, "status", "") != "ALIVE"
                                          and getattr(o, "volume_left", 0) > 0]

                            if _alive or _cancelled:
                                # 有 ALIVE 单：撤掉后重下；有被撤销单：直接重下
                                # 服务器拒单时累计 _reject_count，超限则放弃重试（防无限循环）
                                if _cancelled:
                                    _pco["_reject_count"] = _pco.get("_reject_count", 0) + 1
                                    if _pco["_reject_count"] > 8:
                                        logger.error(
                                            f"TqFeed: 守卫平仓连续{_pco['_reject_count']}次被拒单，放弃重试 {_asym}"
                                            f"（可能已被其他途径平仓，请检查持仓）"
                                        )
                                        continue  # 不加入 _still_pending，放弃追踪
                                # 限速：撤单重下最快每10秒一次，避免 wait_update 事件驱动的紧密循环
                                # 背景：每次 cancel/insert_order 都会触发 wait_update 立即返回，
                                # 不加限速时可在1秒内循环数百次，产生大量无效委托。
                                _now_t = time_module.time()
                                if _now_t - _pco.get("_last_resubmit_t", 0) < 10.0:
                                    _still_pending.append(_pco)
                                    continue  # 等待冷却，本次不撤单重下
                                _pco["_last_resubmit_t"] = _now_t
                                for _o in _alive:
                                    try:
                                        self._api.cancel_order(_o)
                                    except Exception:
                                        pass
                                # 取当前对价重新下平仓单
                                _q_r  = self._quotes.get(_pco["contract_key"]) or self._quotes.get(_asym)
                                _cdir = _pco["close_info"]["close_direction"]
                                if _q_r is not None:
                                    _raw_cp = float(getattr(_q_r,
                                        'bid_price1' if _cdir == "SELL" else 'ask_price1',
                                        float('nan')))
                                    _new_cp = _pco["close_price"] if np.isnan(_raw_cp) else _raw_cp
                                else:
                                    _new_cp = _pco["close_price"]
                                _new_orders = []
                                _rpos = self._api.get_position(_asym)
                                _rrem = min(_rem, _gvol)
                                if _dir == "LONG":
                                    _rlv_t = _safe_vol(_rpos.volume_long_today)
                                    _rlv_h = _safe_vol(_rpos.volume_long_his)
                                    if _rlv_t > 0 and _rrem > 0:
                                        _n = min(_rlv_t, _rrem)
                                        _off = "CLOSETODAY" if _asym.split(".")[0] in ("SHFE", "INE") else "CLOSE"
                                        _new_orders.append(self._api.insert_order(_asym, "SELL", _off, _n, _new_cp))
                                        _rrem -= _n
                                    if _rlv_h > 0 and _rrem > 0:
                                        _n = min(_rlv_h, _rrem)
                                        _new_orders.append(self._api.insert_order(_asym, "SELL", "CLOSE", _n, _new_cp))
                                else:
                                    _rsv_t = _safe_vol(_rpos.volume_short_today)
                                    _rsv_h = _safe_vol(_rpos.volume_short_his)
                                    if _rsv_t > 0 and _rrem > 0:
                                        _n = min(_rsv_t, _rrem)
                                        _off = "CLOSETODAY" if _asym.split(".")[0] in ("SHFE", "INE") else "CLOSE"
                                        _new_orders.append(self._api.insert_order(_asym, "BUY", _off, _n, _new_cp))
                                        _rrem -= _n
                                    if _rsv_h > 0 and _rrem > 0:
                                        _n = min(_rsv_h, _rrem)
                                        _new_orders.append(self._api.insert_order(_asym, "BUY", "CLOSE", _n, _new_cp))
                                _pco["orders"]                    = _new_orders
                                _pco["close_price"]               = _new_cp
                                _pco["close_info"]["close_price"] = _new_cp
                                _pco["_wait_count"]               = 0
                                _reason = "撤单重下" if _alive else f"委托被拒/撤销，重新下单（第{_pco.get('_reject_count',1)}次）"
                                logger.info(
                                    f"TqFeed: 守卫平仓{_reason} @{_new_cp} {_asym} 剩余{_rem}手"
                                )
                                _still_pending.append(_pco)
                            else:
                                # 无 ALIVE(vol>0) 单、无被撤销单：委托已全部成交或状态翻转中，等 tqsdk 持仓数据刷新
                                _pco["_wait_count"] = _pco.get("_wait_count", 0) + 1
                                # 超时保护：wait_update deadline=2s，150次 ≈ 5分钟无持仓归零 → 强制丢弃
                                # 避免 tqsdk 网络抖动导致 ALIVE 单永不翻转时，追踪条目无限堆积
                                if _pco["_wait_count"] > 150:
                                    logger.error(
                                        f"TqFeed: 守卫平仓追踪超时（>150次未持仓归零），强制丢弃 {_asym}"
                                    )
                                    continue  # 不加入 _still_pending，放弃追踪
                                if _pco["_wait_count"] % 5 == 0:  # 每10s记一条日志，避免刷屏
                                    logger.info(
                                        f"TqFeed: 守卫委托已成交，等待持仓清零 {_asym} 剩余{_rem}手"
                                    )
                                _still_pending.append(_pco)
                        except Exception as _pce:
                            logger.warning(
                                f"TqFeed: 守卫平仓追踪异常 {_pco.get('actual_sym')}: {_pce}"
                            )
                            _still_pending.append(_pco)
                    self._pending_close_orders = _still_pending

                # ── 检查止盈止损守卫 ──────────────────────────────────
                with self._lock:
                    guards = dict(self._guards)

                for sym, guard in guards.items():
                    try:
                        # sym 格式可能是 "SHFE.au2509::gemini" 或旧格式 "SHFE.au2509"
                        # contract_key 是纯合约部分，用于行情查找
                        contract_key = sym.split("::")[0]
                        # Try direct match first, then search by underlying
                        q = self._quotes.get(contract_key)
                        if q is None:
                            # search for a quote whose underlying_symbol or key contains this contract
                            for qkey, qval in self._quotes.items():
                                try:
                                    u = getattr(qval, 'underlying_symbol', None)
                                    if u == contract_key or qkey == contract_key:
                                        q = qval
                                        break
                                except Exception:
                                    pass
                        if q is None:
                            continue
                        price = float(q.last_price)
                        if np.isnan(price):
                            continue

                        # ── Bug-A 修复：提前解析 actual_sym ──────────────────
                        # _get_price_tick(actual_sym) 在追踪止损段（LONG/SHORT 两个分支）
                        # 都需要用到 actual_sym，但原代码只在下方"守卫触发平仓"段才赋值，
                        # 导致每个 tick 抛 NameError 被 except 吞掉，
                        # _sl_changed 永远为 False，追踪止损完全失效。
                        actual_sym = contract_key
                        if "@" in contract_key:
                            try:
                                u = self._quotes[contract_key].underlying_symbol
                                if u:
                                    actual_sym = u
                            except Exception:
                                pass
                        # ── 修复结束 ─────────────────────────────────────────

                        direction = guard["direction"]

                        # ── 追踪止损更新（分段收紧策略）──────────────────────────────
                        # 盈利 < 0.6R : 不追踪，保持初始止损
                        # 盈利 0.6~1R : 追踪距离 = 1.0R，底线保本
                        # 盈利 1~2R   : 追踪距离 = 0.7R，底线锁定 0.3R 利润
                        # 盈利 > 2R   : 追踪距离 = 0.5R，底线锁定 1.5R 利润
                        if config.ENABLE_TRAILING_STOP and guard.get("trailing_on") and guard.get("initial_risk") and guard.get("fill_confirmed", True):
                            _entry    = guard.get("entry_price")
                            _ini_risk = guard["initial_risk"]   # 初始风险距离，全程不变
                            _sl_changed = False

                            with self._lock:
                                _g = self._guards.get(sym)
                                if _g:
                                    if direction == "LONG":
                                        # 1. 更新峰值
                                        _peak = _g.get("peak_price") or _entry or price
                                        if price > _peak:
                                            _g["peak_price"] = price
                                            _peak = price

                                        # 2. 计算当前盈利倍数（R）
                                        _profit_r = (_peak - _entry) / _ini_risk if _entry else 0

                                        # 3. 分段确定追踪距离 & 最低止损底线
                                        # price_tick：直接读已订阅的 quote，禁止调用 api.get_quote()（tqsdk 线程内禁止）
                                        _tick = 1.0
                                        try:
                                            _pt = float(getattr(q, 'price_tick', 0) or 0)
                                            if _pt > 0:
                                                _tick = _pt
                                        except Exception:
                                            pass
                                        if _profit_r < 0.6:
                                            # 尚未激活追踪，保持原始止损不动
                                            _trail_dist = None
                                            _floor      = None
                                        elif _profit_r < 1.0:
                                            _trail_dist = 1.0 * _ini_risk
                                            _floor      = _entry + _tick               # 保本+1跳，锁住1跳利润
                                        elif _profit_r < 2.0:
                                            _trail_dist = 0.7 * _ini_risk
                                            _floor      = _entry + 0.3 * _ini_risk      # 至少锁 0.3R
                                        else:
                                            _trail_dist = 0.5 * _ini_risk
                                            _floor      = _entry + 1.5 * _ini_risk      # 至少锁 1.5R

                                        # 4. 止盈感知层：接近止盈时超级收紧追踪距离
                                        # 用行程比例判断（dist_to_tp / total_zone），避免 TP 设置偏紧时
                                        # 1.0R 阈值覆盖过多行程导致感知层过早激活
                                        if config.ENABLE_TP_AWARENESS and _trail_dist is not None and _g.get("take_profit") and _entry:
                                            _tp = _g["take_profit"]
                                            _total_zone = _tp - _entry  # 多头：正值
                                            _dist_to_tp = _tp - price   # 当前价距止盈的剩余距离
                                            if _total_zone > 0:
                                                _dist_ratio = _dist_to_tp / _total_zone  # 0=到达TP, 1=在entry处
                                                if 0 <= _dist_ratio <= 0.15:
                                                    # 最后 15%：追踪距离压到 0.15R，几乎贴着走
                                                    _trail_dist = min(_trail_dist, 0.15 * _ini_risk)
                                                elif _dist_ratio <= 0.35:
                                                    # 最后 35%：追踪距离压到 0.3R
                                                    _trail_dist = min(_trail_dist, 0.3 * _ini_risk)

                                        # 5. 计算新止损并应用（原第4步，序号顺延）
                                        if _trail_dist is not None:
                                            _new_stop = _peak - _trail_dist
                                            if _floor is not None:
                                                _new_stop = max(_new_stop, _floor)
                                            _cur_sl = _g.get("stop_loss")
                                            # 止损只能上移，不能后退
                                            if _cur_sl is None or _new_stop > _cur_sl:
                                                _g["stop_loss"] = _new_stop
                                                _sl_changed = True

                                        guard["stop_loss"]  = _g["stop_loss"]
                                        guard["peak_price"] = _g["peak_price"]

                                    elif direction == "SHORT":
                                        # 1. 更新峰值（空头峰值是最低价）
                                        _peak = _g.get("peak_price") or _entry or price
                                        if price < _peak:
                                            _g["peak_price"] = price
                                            _peak = price

                                        # 2. 计算盈利倍数
                                        _profit_r = (_entry - _peak) / _ini_risk if _entry else 0

                                        # 3. 分段确定追踪距离 & 最高止损底线
                                        # price_tick：直接读已订阅的 quote，禁止调用 api.get_quote()（tqsdk 线程内禁止）
                                        _tick = 1.0
                                        try:
                                            _pt = float(getattr(q, 'price_tick', 0) or 0)
                                            if _pt > 0:
                                                _tick = _pt
                                        except Exception:
                                            pass
                                        if _profit_r < 0.6:
                                            _trail_dist = None
                                            _floor      = None
                                        elif _profit_r < 1.0:
                                            _trail_dist = 1.0 * _ini_risk
                                            _floor      = _entry - _tick               # 保本-1跳，锁住1跳利润
                                        elif _profit_r < 2.0:
                                            _trail_dist = 0.7 * _ini_risk
                                            _floor      = _entry - 0.3 * _ini_risk      # 至少锁 0.3R
                                        else:
                                            _trail_dist = 0.5 * _ini_risk
                                            _floor      = _entry - 1.5 * _ini_risk      # 至少锁 1.5R

                                        # 4. 止盈感知层：接近止盈时超级收紧追踪距离
                                        # 用行程比例判断（dist_to_tp / total_zone），避免 TP 设置偏紧时
                                        # 1.0R 阈值覆盖过多行程导致感知层过早激活
                                        if config.ENABLE_TP_AWARENESS and _trail_dist is not None and _g.get("take_profit") and _entry:
                                            _tp = _g["take_profit"]
                                            _total_zone = _entry - _tp  # 空头：正值
                                            _dist_to_tp = price - _tp   # 空头：当前价距止盈的剩余距离
                                            if _total_zone > 0:
                                                _dist_ratio = _dist_to_tp / _total_zone  # 0=到达TP, 1=在entry处
                                                if 0 <= _dist_ratio <= 0.15:
                                                    # 最后 15%：追踪距离压到 0.15R，几乎贴着走
                                                    _trail_dist = min(_trail_dist, 0.15 * _ini_risk)
                                                elif _dist_ratio <= 0.35:
                                                    # 最后 35%：追踪距离压到 0.3R
                                                    _trail_dist = min(_trail_dist, 0.3 * _ini_risk)

                                        # 5. 计算新止损并应用
                                        if _trail_dist is not None:
                                            _new_stop = _peak + _trail_dist
                                            if _floor is not None:
                                                _new_stop = min(_new_stop, _floor)
                                            _cur_sl = _g.get("stop_loss")
                                            # 止损只能下移，不能后退
                                            if _cur_sl is None or _new_stop < _cur_sl:
                                                _g["stop_loss"] = _new_stop
                                                _sl_changed = True

                                        guard["stop_loss"]  = _g["stop_loss"]
                                        guard["peak_price"] = _g["peak_price"]

                            # 止损变动后记录日志
                            if _sl_changed:
                                logger.debug(
                                    f"TqFeed: 追踪止损更新 {sym} | "
                                    f"price={price:.2f} peak={_g.get('peak_price'):.2f} "
                                    f"profit_r={_profit_r:.2f} trail_dist={_trail_dist:.2f} "
                                    f"new_sl={_g.get('stop_loss'):.2f} floor={_floor}"
                                )
                            # 止损变动后推送 SSE 通知前端
                            if _sl_changed and self.on_guard_update and self._loop:
                                try:
                                    asyncio.run_coroutine_threadsafe(
                                        self.on_guard_update(
                                            guard["model_id"], guard["symbol"], dict(guard)
                                        ),
                                        self._loop,
                                    )
                                except Exception:
                                    pass
                            # 追踪止损变动后标记 dirty，由 _flush_guards_loop 每5秒批量写盘
                            if _sl_changed:
                                self._guards_dirty = True

                        sl = guard.get("stop_loss")
                        tp = guard.get("take_profit")
                        triggered = None
                        # 跟进不足提前退出
                        if guard.get("bar_exit_triggered"):
                            triggered = (
                                f"跟进不足退出 {contract_key} "
                                f"{guard.get('bar_exit_limit', 2)}根K线内未进盈利区"
                            )
                        elif direction == "LONG":
                            if sl and price <= sl:
                                triggered = f"止损触发 {contract_key} 最新价{price} ≤ 止损{sl}"
                            elif tp and price >= tp:
                                triggered = f"止盈触发 {contract_key} 最新价{price} ≥ 止盈{tp}"
                        elif direction == "SHORT":
                            if sl and price >= sl:
                                triggered = f"止损触发 {contract_key} 最新价{price} ≥ 止损{sl}"
                            elif tp and price <= tp:
                                triggered = f"止盈触发 {contract_key} 最新价{price} ≤ 止盈{tp}"
                        # ── 尾盘强制平仓（tick级别，收盘前 EOD_FORCE_CLOSE_MINUTES 分钟）──
                        # 独立于K线回调，确保有持仓时必然在收盘前平仓
                        if not triggered and config.ENABLE_EOD_CLOSE:
                            _now_dt = datetime.now()
                            _m = _now_dt.minute
                            # 粗剪：只在接近整点或半点（收盘前5分钟窗口）时才进行精确会话检测
                            if _m >= 55 or _m <= 2 or 25 <= _m <= 32:
                                if config.is_near_session_end(contract_key, dt=_now_dt):
                                    triggered = f"尾盘强制平仓 {contract_key}（收盘前{config.EOD_FORCE_CLOSE_MINUTES}分钟）"
                        if triggered:
                            logger.info(
                                f"TqFeed: {triggered}，执行自动平仓 | "
                                f"entry={guard.get('entry_price')} "
                                f"peak={guard.get('peak_price')} "
                                f"sl={guard.get('stop_loss')} "
                                f"tp={guard.get('take_profit')} "
                                f"initial_risk={guard.get('initial_risk')} "
                                f"trailing_on={guard.get('trailing_on')}"
                            )
                            with self._lock:
                                self._guards.pop(sym, None)
                                _remaining_guards = dict(self._guards)
                            # 守卫弹出后写盘（异步抛到 asyncio 线程，不阻塞 wait_update）
                            self._save_guards_async(copy.deepcopy(_remaining_guards))
                            # actual_sym 已在本 guard 迭代开头（Bug-A 修复段）赋值，此处无需重复解析
                            # 构造平仓指令（用实际合约，只平守卫负责的手数）
                            guard_vol = int(guard.get("volume", 0)) or 1
                            pos = self._api.get_position(actual_sym)
                            # 平仓前缓存开仓价，供 trader.get_trades() 补全 close_profit
                            _mult = 0
                            try:
                                import trader as _trader_mod
                                # ✅ Bug1修复：从已订阅的 _quotes 中取合约信息，不调用 get_quote()
                                # 原代码 get_quote(actual_sym) 在 tqsdk 线程内触发订阅请求，
                                # 导致 wait_update 阻塞最长 2 秒，守卫触发时行情冻结。
                                _q2 = self._quotes.get(actual_sym) or self._quotes.get(contract_key)
                                _mult = float(getattr(_q2, 'volume_multiple', 0) or 0) if _q2 else 0
                                _op_l = float(getattr(pos, 'open_price_long', float('nan')))
                                _op_s = float(getattr(pos, 'open_price_short', float('nan')))
                                _ex   = _trader_mod._position_open_cache.get(actual_sym, {})
                                _trader_mod._position_open_cache[actual_sym] = {
                                    'open_price_long':  (None if (math.isnan(_op_l) or _op_l == 0) else _op_l) or _ex.get('open_price_long'),
                                    'open_price_short': (None if (math.isnan(_op_s) or _op_s == 0) else _op_s) or _ex.get('open_price_short'),
                                    'multiplier':       (_mult if _mult > 0 else None) or _ex.get('multiplier'),
                                }
                                # ✅ Bug2修复：磁盘写入异步抛到 asyncio 线程，不阻塞 wait_update
                                # 原代码直接同步调用 _save_open_cache，在 tqsdk 线程内写盘加重延迟。
                                if self._loop:
                                    _cache_snap = copy.deepcopy(_trader_mod._position_open_cache)
                                    async def _do_save_guard(cache=_cache_snap, mod=_trader_mod):
                                        try:
                                            mod._save_open_cache(cache)
                                        except Exception:
                                            pass
                                    asyncio.run_coroutine_threadsafe(_do_save_guard(), self._loop)
                                else:
                                    _trader_mod._save_open_cache(_trader_mod._position_open_cache)
                            except Exception:
                                pass

                            # ── 对价平仓：买平用卖一价，卖平用买一价 ──────────────
                            # 使用对价而非 last_price，确保止损/止盈单立即撮合成交，
                            # 避免限价单因行情反弹/回落无对手盘而挂单到收盘。
                            _close_dir = "SELL" if direction == "LONG" else "BUY"
                            try:
                                _q_close = self._quotes.get(contract_key) or self._quotes.get(actual_sym)
                                if _q_close is not None:
                                    if _close_dir == "SELL":
                                        # 卖平（平多）：用买一价，对手方是买盘
                                        _cp = float(getattr(_q_close, 'bid_price1', float('nan')))
                                    else:
                                        # 买平（平空）：用卖一价，对手方是卖盘
                                        _cp = float(getattr(_q_close, 'ask_price1', float('nan')))
                                    # 对价为 NaN 时降级用 last_price
                                    close_price = price if np.isnan(_cp) else _cp
                                else:
                                    close_price = price
                            except Exception:
                                close_price = price

                            # ── Bug-3 修复：用模块级 _safe_vol() 安全转换持仓量 ──────
                            # 原代码 int(pos.volume_long_today) 在 tqsdk 返回 NaN 时
                            # 抛 ValueError，被外层 except 吞掉，守卫静默不平仓。
                            # _safe_vol 已提升至模块级，此处直接调用。
                            _actual_pos_vol = 0
                            _close_orders_placed = []
                            if direction == "LONG":
                                remaining = guard_vol
                                lv_t = _safe_vol(pos.volume_long_today)
                                lv_h = _safe_vol(pos.volume_long_his)
                                _actual_pos_vol = lv_t + lv_h
                                if lv_t > 0 and remaining > 0:
                                    n = min(lv_t, remaining)
                                    _off_t = "CLOSETODAY" if actual_sym.split(".")[0] in ("SHFE", "INE") else "CLOSE"
                                    _close_orders_placed.append(
                                        self._api.insert_order(actual_sym, "SELL", _off_t, n, close_price))
                                    remaining -= n
                                if lv_h > 0 and remaining > 0:
                                    n = min(lv_h, remaining)
                                    _close_orders_placed.append(
                                        self._api.insert_order(actual_sym, "SELL", "CLOSE", n, close_price))
                            else:  # SHORT
                                remaining = guard_vol
                                sv_t = _safe_vol(pos.volume_short_today)
                                sv_h = _safe_vol(pos.volume_short_his)
                                _actual_pos_vol = sv_t + sv_h
                                if sv_t > 0 and remaining > 0:
                                    n = min(sv_t, remaining)
                                    _off_t = "CLOSETODAY" if actual_sym.split(".")[0] in ("SHFE", "INE") else "CLOSE"
                                    _close_orders_placed.append(
                                        self._api.insert_order(actual_sym, "BUY", _off_t, n, close_price))
                                    remaining -= n
                                if sv_h > 0 and remaining > 0:
                                    n = min(sv_h, remaining)
                                    _close_orders_placed.append(
                                        self._api.insert_order(actual_sym, "BUY", "CLOSE", n, close_price))

                            # ── 持仓为零：守卫已被清除，但跳过记录注入 ──────────
                            # 手动平仓后持仓归零，守卫若仍在内存中（clear_guard 未被调用）
                            # 会在价格穿越 SL/TP 时触发此分支；实际无持仓不需下单，
                            # 也不应写入假的"守卫止损"历史和委托记录。
                            if _actual_pos_vol == 0:
                                _log.getLogger(__name__).info(
                                    f"TqFeed: 守卫触发时持仓已为零（可能已手动平仓），守卫已清除，跳过记录注入 {sym}"
                                )
                                continue  # 跳过下方 save_order_record 和 on_guard_close

                            # ── 写入委托记录（与 trader.py 开仓记录格式对齐）──────
                            # 守卫直接调用 tqsdk 线程的 insert_order，绕过了 _queue_op 路径，
                            # 需在此手动补录，确保委托记录完整可查。
                            # status="守卫触发" 区别于普通 AI 下单，便于前端筛选和复盘。
                            try:
                                from datetime import datetime as _dt
                                _trader_mod.save_order_record({
                                    "time":      _dt.now().strftime("%Y-%m-%dT%H:%M:%S"),
                                    "model_id":  guard.get("model_id", ""),
                                    "symbol":    guard.get("symbol", contract_key),
                                    "direction": "卖" if _close_dir == "SELL" else "买",
                                    "offset":    "平仓",
                                    "price":     close_price,
                                    "volume":    guard_vol,
                                    "filled":    0,        # 挂单时未知，待成交回报确认
                                    "status":    (
                                        "尾盘强制平仓" if "尾盘强制平仓" in (triggered or "") else
                                        "跟进不足退出" if "跟进不足"    in (triggered or "") else
                                        "守卫触发"
                                    ),
                                    "order_id":  "",
                                })
                            except Exception as _oe:
                                _log.getLogger(__name__).warning(f"TqFeed: 守卫委托记录写入失败: {_oe}")

                            # 守卫触发后：加入平仓追踪列表，由 _run_loop 每2s检测持仓清零后再调 on_guard_close
                            guard_model_id = guard.get("model_id", "")
                            guard_symbol   = guard.get("symbol", contract_key)
                            if guard_model_id and self.on_guard_close and self._loop:
                                try:
                                    _ci = {
                                        "close_direction": _close_dir,
                                        "entry_price":     guard.get("entry_price"),
                                        "close_price":     close_price,
                                        "volume":          guard_vol,
                                        "multiplier":      _mult,
                                        "guard_direction": direction,
                                        "trigger_type": (
                                            "EOD"      if "尾盘强制平仓" in (triggered or "") else
                                            "BAR_EXIT" if "跟进不足"    in (triggered or "") else
                                            "SL"       if "止损"        in (triggered or "") else
                                            "TP"
                                        ),
                                    }
                                    self._pending_close_orders.append({
                                        "actual_sym":   actual_sym,
                                        "contract_key": contract_key,
                                        "direction":    direction,
                                        "guard_vol":    guard_vol,
                                        "orders":       _close_orders_placed,
                                        "close_price":  close_price,
                                        "model_id":     guard_model_id,
                                        "guard_symbol": guard_symbol,
                                        "close_info":   _ci,
                                        # 初始化限速时间戳为当前时刻，使首次检查也受 3 秒冷却保护，
                                        # 避免订单刚下出去还未成交就被立刻撤掉重下。
                                        "_last_resubmit_t": time_module.time(),
                                    })
                                except Exception as _ce:
                                    logger.warning(f"TqFeed: 守卫平仓追踪加入失败: {_ce}")
                    except Exception as _e:
                        logger.error(f"TqFeed: 守卫检查异常 {sym}: {_e}")

                # ── 检测K线收盘（最后一根K线datetime变化 = 新K线出现 = 上一根已收盘）────────────
                # 注意：tqsdk get_kline_serial 返回固定250行滑动窗口DataFrame，
                # len(kdf) 始终为250，不随新K线出现增大，必须用datetime变化检测新K线。
                for sym, kdf in list(self._klines_obj.items()):
                    try:
                        try:
                            cur_last_dt = int(kdf.iloc[-1]["datetime"])
                        except Exception:
                            cur_last_dt = None
                        prev_last_dt = self._last_kline_dt.get(sym)
                        if prev_last_dt is not None and cur_last_dt is not None and cur_last_dt != prev_last_dt:
                            # 新K线出现，上一根5分钟K线刚收盘
                            if self._loop and self._kline_close_callbacks:
                                for cb in list(self._kline_close_callbacks):
                                    try:
                                        self._loop.call_soon_threadsafe(cb, sym)
                                    except Exception:
                                        pass
                            # ── 跟进检测：K线收盘时更新守卫计数 ──────────────────
                            _q_obj = self._quotes.get(sym)
                            _underlying = getattr(_q_obj, 'underlying_symbol', None) if _q_obj else None
                            # 刚收盘K线的收盘价（新K线出现时倒数第2根即为刚收盘K线）
                            # 用收盘价而非 peak_price 判断跟进，避免影线轻触 0.1R 就永久解除检测
                            _closed_close = None
                            try:
                                if len(kdf) >= 2:
                                    _closed_close = float(kdf['close'].iloc[-2])
                            except Exception:
                                pass
                            _bar_guard_updated = False
                            with self._lock:
                                for _gval in self._guards.values():
                                    _g_sym = _gval.get("symbol", "")
                                    if _g_sym != sym and _g_sym != _underlying:
                                        continue
                                    _bar_limit = _gval.get("bar_exit_limit", 0)
                                    if not _bar_limit or not config.ENABLE_BAR_EXIT:
                                        continue
                                    _bar_count = _gval.get("entry_bar_count", 0) + 1
                                    _gval["entry_bar_count"] = _bar_count
                                    _bar_guard_updated = True
                                    # 每根K线都检查是否已达到0.1R，达到则永久豁免后续检测
                                    if not _gval.get("bar_exit_passed") and not _gval.get("bar_exit_triggered"):
                                        _ep  = _gval.get("bar_exit_ref_price") or _gval.get("entry_price")
                                        _ir  = _gval.get("initial_risk")
                                        _dir = _gval.get("direction")
                                        _cp  = _closed_close
                                        if _ep and _cp is not None and _ir and _ir > 0:
                                            _pr = (_cp - _ep) / _ir if _dir == "LONG" else (_ep - _cp) / _ir
                                            if _pr >= 0.1:
                                                _gval["bar_exit_passed"] = True
                                                logger.info(
                                                    f"跟进充分豁免 {_gval.get('symbol')} "
                                                    f"bar_count={_bar_count} profit_r={_pr:.2f}"
                                                )
                                    if _bar_count >= _bar_limit and not _gval.get("bar_exit_triggered") and not _gval.get("bar_exit_passed"):
                                        _ep  = _gval.get("bar_exit_ref_price") or _gval.get("entry_price")
                                        _ir  = _gval.get("initial_risk")
                                        _dir = _gval.get("direction")
                                        _cp  = _closed_close  # 用收盘价判断
                                        if _ep and _cp is not None:
                                            if _ir and _ir > 0:
                                                _pr = (_cp - _ep) / _ir if _dir == "LONG" else (_ep - _cp) / _ir
                                                if _pr < 0.1:
                                                    _gval["bar_exit_triggered"] = True
                                                    logger.info(
                                                        f"跟进不足标记 {_gval.get('symbol')} "
                                                        f"bar_count={_bar_count} profit_r={_pr:.2f}"
                                                    )
                                            else:
                                                # initial_risk 未知（SL 为 None 的早退情形）：
                                                # 仅当价格未朝有利方向移动时才退出，
                                                # 避免与两阶段止损补充流程产生竞态
                                                _moved_favorable = (
                                                    (_dir == "LONG"  and _cp > _ep) or
                                                    (_dir == "SHORT" and _cp < _ep)
                                                )
                                                if not _moved_favorable:
                                                    _gval["bar_exit_triggered"] = True
                                                    logger.info(
                                                        f"跟进不足标记(无初始风险) {_gval.get('symbol')} "
                                                        f"bar_count={_bar_count} "
                                                        f"close={_cp:.2f} ref={_ep:.2f}"
                                                    )
                            # entry_bar_count / bar_exit_triggered 变更后持久化守卫
                            if _bar_guard_updated:
                                with self._lock:
                                    _gsnap = copy.deepcopy(self._guards)
                                self._save_guards_async(_gsnap)
                        if cur_last_dt is not None:
                            self._last_kline_dt[sym] = cur_last_dt
                    except Exception:
                        pass

                # ── 触发行情更新回调（节流：最多 5 Hz，避免事件循环堆积）────
                _now = time_module.time()
                if self._loop and self._on_update_callbacks and (_now - _last_notify_ts >= 0.2):
                    _last_notify_ts = _now
                    for cb in list(self._on_update_callbacks):
                        try:
                            self._loop.call_soon_threadsafe(cb)
                        except Exception:
                            pass

                # ── 处理交易指令队列 ──────────────────────────────────
                for req in pending_orders:
                    try:
                        if req["type"] == "insert":
                            kw = req["kwargs"]
                            order = self._api.insert_order(
                                symbol=kw["symbol"],
                                direction=kw["direction"],
                                offset=kw["offset"],
                                volume=kw["volume"],
                                limit_price=kw.get("limit_price"),
                            )
                            req["result"] = {"ok": True, "order_id": order.order_id}
                            # 仅开仓订单需要追踪实际成交价以更新守卫 entry_price
                            if kw.get("offset") == "OPEN" and kw.get("_model_id"):
                                self._fill_trackers.append({
                                    "order":     order,
                                    "model_id":  kw["_model_id"],
                                    "guard_sym": kw["_guard_sym"],
                                })
                        elif req["type"] == "cancel":
                            self._api.cancel_order(req["kwargs"]["order_obj"])
                            req["result"] = {"ok": True, "message": "已撤单"}
                    except Exception as e:
                        req["result"] = {"ok": False, "message": str(e)}
                    finally:
                        req["event"].set()

                # ── 开仓成交监听：用实际成交价更新守卫 entry_price ──────
                done = []
                for item in self._fill_trackers:
                    order = item["order"]
                    if order.status == "FINISHED":
                        fill_price = getattr(order, "trade_price", 0) or 0
                        if order.volume_left == 0 and fill_price > 0:
                            prefix = f"{item['guard_sym']}::{item['model_id']}"
                            with self._lock:
                                for gkey, guard in self._guards.items():
                                    if gkey == prefix or gkey.startswith(prefix + "::"):
                                        old_ep = guard.get("entry_price") or fill_price
                                        if abs(old_ep - fill_price) > 0.001:
                                            guard["entry_price"] = fill_price
                                            guard["peak_price"]  = fill_price
                                            sl = guard.get("stop_loss")
                                            if sl:
                                                guard["initial_risk"] = abs(fill_price - sl)
                                        guard["fill_confirmed"] = True  # 解锁追踪止损计算
                            # 开仓成交：立即缓存实际成交价，确保平仓收益计算正确
                            # ── 数据冻结修复 ────────────────────────────────────────
                            # 修复1：禁止在 tqsdk 线程内调用 self._api.get_quote()
                            #        原代码 get_quote(_instr) 会触发新合约订阅请求，
                            #        导致 tqsdk 内部 pending_sub=True，wait_update 无法
                            #        及时响应 tick，行情数据冻结直到 2 秒 deadline 超时。
                            #        改为从已订阅的 self._quotes 中查找（零副作用）。
                            # 修复2：_save_open_cache 是同步磁盘 I/O，阻塞 tqsdk 线程。
                            #        改为通过 run_coroutine_threadsafe 抛到 asyncio 线程执行。
                            try:
                                import trader as _trader_mod
                                _instr = getattr(order, 'instrument_id', '')
                                _dir   = getattr(order, 'direction', '')
                                if _instr and _dir:
                                    _ex2 = _trader_mod._position_open_cache.get(_instr, {})
                                    _upd = dict(_ex2)
                                    if _dir == 'BUY':
                                        _upd['open_price_long']  = fill_price
                                    else:
                                        _upd['open_price_short'] = fill_price

                                    # ✅ 修复1：从已订阅的 _quotes 中查 multiplier，不触发新订阅
                                    # 开仓合约必然已订阅，_quotes 中存 actual_sym 或主连 key 两种形式
                                    if not _upd.get('multiplier'):
                                        try:
                                            _q_existing = (
                                                self._quotes.get(_instr)
                                                or self._quotes.get(item["guard_sym"])
                                            )
                                            if _q_existing:
                                                _m3 = float(getattr(_q_existing, 'volume_multiple', 0) or 0)
                                                if _m3 > 0:
                                                    _upd['multiplier'] = _m3
                                        except Exception:
                                            pass

                                    # 内存缓存立即更新（无 I/O，在 tqsdk 线程内安全）
                                    _trader_mod._position_open_cache[_instr] = _upd

                                    # ✅ 修复2：磁盘写入异步抛到 asyncio 线程，不阻塞 wait_update
                                    if self._loop:
                                        _cache_snapshot = copy.deepcopy(_trader_mod._position_open_cache)
                                        async def _do_save(cache=_cache_snapshot, mod=_trader_mod):
                                            try:
                                                mod._save_open_cache(cache)
                                            except Exception:
                                                pass
                                        asyncio.run_coroutine_threadsafe(_do_save(), self._loop)
                                    else:
                                        # asyncio 循环未就绪时的兜底（启动早期极少发生）
                                        _trader_mod._save_open_cache(_trader_mod._position_open_cache)
                            except Exception:
                                pass
                        # ── Bug L-2 修复：订单被拒/撤销（无成交）时清除孤立守卫 ──
                        # insert_order 立即返回 ok=True，守卫在下单时即注册。
                        # 若订单后来被交易所拒单或被撤销（volume_left > 0），
                        # 守卫仍留在内存中，等价格触及 SL/TP 时才被 Bug-B 路径清除。
                        # 此处主动在 tracker 失效时即清除，避免孤立守卫污染状态。
                        elif order.volume_left > 0:
                            try:
                                import trader as _trader_mod
                                _oid = getattr(order, 'order_id', '')
                                _gsym = item.get('guard_sym', '')
                                _mid  = item.get('model_id', '')
                                if _oid and _gsym and _mid:
                                    _gkey = f"{_gsym}::{_mid}::{_oid}"
                                    with self._lock:
                                        _removed = self._guards.pop(_gkey, None)
                                    if _removed:
                                        self._save_guards_async(copy.deepcopy(self._guards))
                                        logger.warning(
                                            f"TqFeed: 开仓订单 {_oid} 拒单/撤销（无成交），"
                                            f"已清除孤立守卫 {_gkey}"
                                        )
                            except Exception:
                                pass
                        done.append(item)
                for item in done:
                    self._fill_trackers.remove(item)

                # ── wait_update 带 deadline，每2秒必须返回 ─────────────
                # 这样即使非交易时间无数据推送，也能及时处理新订阅请求
                try:
                    self._api.wait_update(
                        deadline=time_module.time() + 2
                    )
                except Exception:
                    # deadline 到期时 tqsdk 某些版本会抛异常，属正常情况
                    pass

        except Exception as e:
            self._error_msg = str(e)
            self._connected = False
            self._ready.set()
            logger.error(f"TqFeed: 线程异常退出 — {e}")
        finally:
            # 在 tqsdk 自己的线程内关闭，避免从主 asyncio 线程调用 close() 导致
            # Python 3.12+ 事件循环冲突（close() 内部会调用 get_event_loop() 取到主循环）
            try:
                if self._api:
                    self._api.close()
            except Exception:
                pass

    def register_kline_close_callback(self, cb):
        """注册K线收盘回调：cb(symbol) 在每根5分钟K线收盘时调用（在asyncio线程中）"""
        self._kline_close_callbacks.append(cb)

    def request_subscribe(self, symbol: str):
        """非阻塞地请求订阅一个合约。"""
        if symbol not in self._subscribed:
            with self._lock:
                self._pending_sub.add(symbol)
            logger.info(f"TqFeed: 已加入订阅队列 {symbol}")

    def fetch(self, symbol: str) -> dict[str, Any]:
        """只读内存数据，不调用任何 tqsdk 函数。"""
        import config as _cfg
        _is_trading = _cfg.is_trading_time(symbol)
        _is_first_bar = _cfg.is_session_first_bar(symbol)
        _is_last_bar  = _cfg.is_session_last_bar(symbol)
        result: dict[str, Any] = {
            "symbol":               symbol,
            "product_name":         get_symbol_name(symbol),
            "timestamp":            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "is_trading_time":      _is_trading,
            "is_session_first_bar": _is_first_bar,
            "is_session_last_bar":  _is_last_bar,
            "quote":           {},
            "ema20":           {},
            "klines":          [],
            "pending":         False,
            "error":           None,
            # ── 价格行为结构字段默认值（_build_price_structure 正常运行时会覆盖）──
            # 连接失败/数据未到/异常时，analyzers.py 拿到的是这些安全默认值
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
            # ── 新增特征 ──────────────────────────────────────────
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

        if not self._connected:
            result["error"] = f"天勤未连接：{self._error_msg or '初始化中，请稍候'}"
            return result

        # 未订阅 → 加入队列，返回 pending
        if symbol not in self._subscribed:
            self.request_subscribe(symbol)
            result["pending"] = True
            result["error"]   = f"正在订阅 {symbol}，请稍候（约5-10秒）..."
            return result

        try:
            quote  = self._quotes.get(symbol)
            klines = self._klines_obj.get(symbol)

            if quote is None or klines is None:
                result["error"] = "数据对象未初始化"
                return result

            last_price = float(quote.last_price)
            pre_close  = float(quote.pre_close)

            # 非交易时间 last_price 可能为 NaN，用昨收价兜底
            if np.isnan(last_price):
                last_price = pre_close

            # 所有核心字段都是 NaN 说明数据还没到
            if np.isnan(last_price) and np.isnan(pre_close):
                result["pending"] = True
                result["error"]   = f"等待 {symbol} 行情数据推送..."
                return result

            chg     = round(last_price - pre_close, 4) if (pre_close and not np.isnan(pre_close)) else None
            chg_pct = round((last_price - pre_close) / pre_close * 100, 2) if (pre_close and not np.isnan(pre_close)) else None

            # ── 问题7: 用行情本身的时间戳，而不是服务器调用时刻 ────────
            quote_dt = getattr(quote, "datetime", None)
            if quote_dt is not None:
                try:
                    # tqsdk datetime 是纳秒整数
                    import pandas as pd
                    result["timestamp"] = pd.Timestamp(int(quote_dt), unit="ns", tz="Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass  # 保留 fetch() 入口处设置的服务器时间

            # ── 问题4: 非交易时间不用最新价兜底今开/最高/最低 ─────────
            def _safe(v, fallback=None):
                f = float(v)
                return round(f, 2) if not np.isnan(f) else fallback

            # ── 问题6: 5分钟K线最后一根的成交量才代表当前时段活跃度 ──
            # quote.volume 是当日累计量，单独标注避免误导 AI
            # ── 涨跌停价检测 ──────────────────────────────────────────────
            _ul = getattr(quote, "upper_limit", None)
            _ll = getattr(quote, "lower_limit", None)
            upper_limit = float(_ul) if (_ul is not None and not np.isnan(float(_ul or 0)) and float(_ul or 0) > 0) else None
            lower_limit = float(_ll) if (_ll is not None and not np.isnan(float(_ll or 0)) and float(_ll or 0) > 0) else None
            is_limit_up   = bool(upper_limit and last_price >= upper_limit)
            is_limit_down = bool(lower_limit and last_price <= lower_limit)
            result["is_limit_up"]  = is_limit_up
            result["is_limit_down"] = is_limit_down

            result["quote"] = {
                "最新价":      last_price,
                "今开":        _safe(quote.open),        # 非交易时间为 null，不兜底
                "最高":        _safe(quote.highest),     # 非交易时间为 null，不兜底
                "最低":        _safe(quote.lowest),      # 非交易时间为 null，不兜底
                "昨收":        pre_close,
                "涨跌额":      chg,
                "涨跌幅%":     chg_pct,
                "当日累计成交量手": int(quote.volume)          if not np.isnan(quote.volume)        else 0,
                "持仓量":      int(quote.open_interest)   if not np.isnan(quote.open_interest) else 0,
                "涨停价":      upper_limit,
                "跌停价":      lower_limit,
                "是否涨停":    is_limit_up,
                "是否跌停":    is_limit_down,
            }

            df = klines.copy().dropna(subset=["close"])
            if df.empty:
                result["pending"] = True
                result["error"]   = "K线数据尚未到达，请稍候..."
                return result

            # ── 取最近250根K线，委托策略计算所有价格行为特征（含EMA）──
            df = df.tail(250).copy()
            strategy_out = _get_strategy().build_price_structure(df)
            result["ema20"] = strategy_out.pop("ema20", {})
            # _ema_values：策略顺带返回的 EMA 数组，用于写入 klines 的 ema20 列（前端图表显示）
            _ema_values = strategy_out.pop("_ema_values", None)
            result.update(strategy_out)

            # ── 问题3: K线时间格式化为人类可读字符串 ─────────────────
            # P1优化：用向量化操作替换 iterrows()，消除150次 Python 循环。
            # 核心改动：
            #   1. pd.to_datetime 一次性批量转换所有时间戳
            #   2. 数值列用 numpy round/astype 批量处理
            #   3. body_ratio 用向量运算替换逐行 abs()/max()
            #   4. 最终 to_dict("records") 一次性生成列表
            df_out = df.copy()

            # 时间列：纳秒时间戳 → "YYYY-MM-DD HH:MM"（K线收盘时间 = 开盘+5分钟）
            # 注意：df_out 的 index 是 tail(150) 截断后的原始 index（如50..199），
            # fallback 中必须用 .values 或 numpy array 赋值，
            # 避免 pd.Series(index=0..149) 与 df_out(index=50..199) 对齐后产生 NaN。
            try:
                ts_series = pd.to_datetime(
                    df_out["datetime"].astype(float), unit="ns", utc=True
                ).dt.tz_convert("Asia/Shanghai") + pd.Timedelta(seconds=config.KLINE_INTERVAL_SECONDS)
                df_out["time"] = ts_series.dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                # fallback：直接赋 numpy array，绕过 pandas index 对齐
                if "datetime" in df_out.columns:
                    df_out["time"] = df_out["datetime"].astype(str).values
                else:
                    df_out["time"] = np.full(len(df_out), "", dtype=object)

            # 数值列：批量 round
            df_out["open"]  = df_out["open"].astype(float).round(2)
            df_out["high"]  = df_out["high"].astype(float).round(2)
            df_out["low"]   = df_out["low"].astype(float).round(2)
            df_out["close"] = df_out["close"].astype(float).round(2)
            df_out["volume"] = df_out["volume"].astype(float).fillna(0).astype(int)

            # EMA20 列：复用策略已算好的数组（前端图表显示用，不传给 AI）
            if _ema_values is not None and len(_ema_values) == len(df_out):
                df_out["ema20"] = _ema_values
            else:
                df_out["ema20"] = df_out["close"].astype(float).ewm(span=20, adjust=False).mean().values.round(2)

            # body_ratio：向量化计算实体比例
            body     = (df_out["close"] - df_out["open"]).abs()
            bar_rng  = (df_out["high"]  - df_out["low"]).clip(lower=1e-9)
            df_out["body_ratio"] = (body / bar_rng).round(3)

            # close_position：收盘在K线内的位置（0=最低价，1=最高价）
            # 用于信号棒方向性判断：看涨信号棒应 > 0.67，看跌应 < 0.33
            df_out["close_position"] = ((df_out["close"] - df_out["low"]) / bar_rng).round(3)

            # 只保留前端/AI 需要的字段，丢弃 tqsdk 内部列
            keep_cols = ["time", "open", "high", "low", "close", "volume", "ema20", "body_ratio", "close_position"]
            result["klines"] = df_out[keep_cols].to_dict("records")



        except Exception as e:
            result["error"] = f"读取数据异常：{e}"
            logger.exception(f"TqFeed.fetch({symbol})")

        return result


# ─────────────────────────────────────────────────────────────
# 全局单例
# ─────────────────────────────────────────────────────────────

_feed: TqFeed | None = None


def init_feed() -> TqFeed:
    global _feed
    _feed = TqFeed()
    _feed.start()
    return _feed


def get_market_data(symbol: str) -> dict[str, Any]:
    if _feed is None:
        return {
            "symbol":               symbol,
            "product_name":         "",
            "timestamp":            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "is_trading_time":      False,
            "is_session_first_bar": False,
            "is_session_last_bar":  False,
            "quote":   {}, "ema20": {}, "klines": [],
            "pending": False, "error": "数据层未初始化",
            # 价格行为结构字段兜底（与 PRICE_STRUCTURE_DEFAULTS 保持一致）
            **{k: list(v) if isinstance(v, list) else v for k, v in PRICE_STRUCTURE_DEFAULTS.items()},
        }
    return _feed.fetch(symbol)


# ─────────────────────────────────────────────────────────────
# 自检
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("data_feed 自检（模拟数据，验证策略委托）")
    print("=" * 60)

    np.random.seed(42)
    prices = 3800 + np.cumsum(np.random.randn(60) * 8 + 0.5)
    df_mock = pd.DataFrame({
        "open":   prices * (1 + np.random.randn(60) * 0.001),
        "high":   prices * (1 + np.abs(np.random.randn(60)) * 0.003),
        "low":    prices * (1 - np.abs(np.random.randn(60)) * 0.003),
        "close":  prices,
        "volume": np.random.randint(100, 1000, 60),
    })
    df_mock["high"] = df_mock[["open", "high", "close"]].max(axis=1)
    df_mock["low"]  = df_mock[["open", "low",  "close"]].min(axis=1)

    # ── 测试1：策略 build_price_structure ─────────────────────
    print("\n[1] strategy.build_price_structure:")
    s = _get_strategy()
    r = s.build_price_structure(df_mock)
    ema20 = r.pop("ema20", {})
    print(f"  strategy_id: {s.strategy_id}")
    print(f"  ema20 keys: {list(ema20.keys())}")
    print(f"  趋势方向: {r.get('趋势方向')}")
    print(f"  近期波段高点: {r.get('近期波段高点')}")
    assert isinstance(r.get("近期波段高点"), list),  "❌ 近期波段高点应为 list"
    assert isinstance(r.get("是否窄幅区间"), bool),  "❌ 是否窄幅区间应为 bool"
    assert ema20.get("EMA20") is not None,            "❌ EMA20 为 None"
    print("  ✅ 通过")

    # ── 测试2：PRICE_STRUCTURE_DEFAULTS ──────────────────────
    print("\n[2] PRICE_STRUCTURE_DEFAULTS:")
    print(f"  字段数: {len(PRICE_STRUCTURE_DEFAULTS)}")
    assert "ema20" not in PRICE_STRUCTURE_DEFAULTS,   "❌ ema20 不应在 PRICE_STRUCTURE_DEFAULTS 顶层"
    assert "近期波段高点" in PRICE_STRUCTURE_DEFAULTS, "❌ 缺少 近期波段高点"
    print("  ✅ 通过")

    print("\n" + "=" * 60)
    print("自检完成")
    print("=" * 60)
