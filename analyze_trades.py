"""
analyze_trades.py — 交易统计分析脚本

用法：
    python analyze_trades.py                  # 自动读取 config.get_data_root()（live/sim 隔离）
    python analyze_trades.py --data ./mydata  # 指定数据目录
    python analyze_trades.py --model claude   # 只看某个模型
    python analyze_trades.py --days 7         # 只看最近7天
    python analyze_trades.py --data "E:\AI期货交易\CLAUDE AI\DATA\sim"

输出：
    终端打印完整统计报告
    同目录生成 trade_report.txt（方便保存）

依赖：Python 3.8+ 标准库，无需安装任何第三方包
"""

import json
import argparse
import pathlib
from datetime import datetime, timedelta
from collections import defaultdict

try:
    import config as _config
    _default_data_dir = _config.get_data_root()
except Exception:
    _default_data_dir = "data"


# ─────────────────────────────────────────────────────────────
# 数据加载
# ─────────────────────────────────────────────────────────────

def load_jsonl(path: pathlib.Path) -> list[dict]:
    """读取 .jsonl 文件，跳过空行和损坏行"""
    records = []
    if not path.exists():
        return records
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except Exception:
            continue
    return records


def load_all_data(data_dir: pathlib.Path, model_filter: str = "", days: int = 0):
    """
    加载分析历史和 equity_*.jsonl
    返回 (history_records, trade_records)
    """
    cutoff = None
    if days > 0:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    def _in_range(record: dict) -> bool:
        if cutoff is None:
            return True
        t = record.get("time", "")
        return t[:10] >= cutoff if t else True

    # 加载分析历史（支持 history/YYYY-MM-DD/品种.jsonl 和 history/YYYY-MM-DD.jsonl 两种结构）
    history_dir = data_dir / "history"
    history_all: list[dict] = []
    if history_dir.is_dir():
        for entry in sorted(history_dir.iterdir()):
            if entry.is_dir():
                if cutoff and entry.name < cutoff:
                    continue
                for fp in sorted(entry.glob("*.jsonl")):
                    history_all.extend(load_jsonl(fp))
            elif entry.suffix == ".jsonl":
                if cutoff and entry.stem < cutoff:
                    continue
                history_all.extend(load_jsonl(entry))
    else:
        history_all = load_jsonl(data_dir / "history.jsonl")

    history = [
        r for r in history_all
        if _in_range(r)
        and (not model_filter or r.get("model_id", "") == model_filter)
    ]

    # 加载交易盈亏（type=trade）和开仓记录（type=open）
    trade_records = []
    open_records  = []
    pattern = f"equity_{model_filter}.jsonl" if model_filter else "equity_*.jsonl"
    for fp in sorted(data_dir.glob(pattern)):
        mid = fp.stem.replace("equity_", "")
        for r in load_jsonl(fp):
            r.setdefault("model", mid)
            if r.get("type") == "trade" and _in_range(r):
                trade_records.append(r)
            elif r.get("type") == "open" and _in_range(r):
                open_records.append(r)

    return history, trade_records, open_records


# ─────────────────────────────────────────────────────────────
# 核心统计
# ─────────────────────────────────────────────────────────────

def safe_div(a, b, default=0.0):
    return a / b if b else default


def stats_block(profits: list[float]) -> dict:
    """给一组盈亏数据计算核心指标"""
    if not profits:
        return {
            "交易次数": 0, "胜率": 0.0,
            "总盈亏": 0.0, "平均盈亏": 0.0,
            "平均盈利": 0.0, "平均亏损": 0.0,
            "盈亏比": 0.0, "期望值": 0.0,
            "最大单笔盈利": 0.0, "最大单笔亏损": 0.0,
        }
    wins   = [p for p in profits if p > 0]
    losses = [p for p in profits if p < 0]
    n      = len(profits)
    wr     = safe_div(len(wins), n)
    avg_w  = safe_div(sum(wins),   len(wins))
    avg_l  = safe_div(sum(losses), len(losses))
    rr     = safe_div(abs(avg_w), abs(avg_l))
    ev     = wr * avg_w + (1 - wr) * avg_l

    return {
        "交易次数":     n,
        "胜率":         round(wr * 100, 1),
        "总盈亏":       round(sum(profits), 2),
        "平均盈亏":     round(safe_div(sum(profits), n), 2),
        "平均盈利":     round(avg_w, 2),
        "平均亏损":     round(avg_l, 2),
        "盈亏比":       round(rr, 2),
        "期望值":       round(ev, 2),
        "最大单笔盈利": round(max(profits), 2),
        "最大单笔亏损": round(min(profits), 2),
    }


def parse_bar_strength(rating: str) -> str:
    """从"弱空头"/"强多头"/"无"等评级提取强度标签"""
    if not rating or rating in ("无", "无法匹配", "未知"):
        return rating or "未知"
    if rating.startswith("强"):
        return "强"
    if rating.startswith("弱"):
        return "弱"
    if rating.startswith("中"):
        return "中"
    return rating  # 原样保留未识别格式


def _trade_session(t_str: str) -> str:
    """根据成交时间判断所属交易时段"""
    try:
        h = datetime.fromisoformat(t_str.replace(" ", "T")).hour
    except Exception:
        return "未知"
    if 9 <= h < 12:
        return "早盘(09-11:30)"
    elif 13 <= h < 15:
        return "午后(13-15)"
    elif h >= 21 or h < 3:
        return "夜盘(21-03)"
    else:
        return "其他"


# ─────────────────────────────────────────────────────────────
# 关联：把分析信号和成交盈亏对应起来
# ─────────────────────────────────────────────────────────────

_ENTRY_ACTIONS = ("做多", "做空", "反手")


def match_signals_to_trades(
    history: list[dict],
    trades:  list[dict],
    opens:   list[dict] | None = None,
) -> list[dict]:
    """
    把每笔平仓交易匹配到最近一次入场信号（做多/做空/反手）。

    匹配策略（两阶段）：
      1. 用 type:open 记录（open_price 精确匹配）锁定实际开仓时间，
         在开仓时间前后 10 分钟内找最近的入场信号。
         _持仓分钟 = 平仓时间 - 开仓时间（真实持仓）
      2. 若无 open 记录可匹配，回退：在平仓时间前 6 小时内找最近信号。
         _持仓分钟 = 平仓时间 - 信号时间（近似）

    匹配后额外计算：
      _R倍数  — 若信号含止损价，用 close_profit / R_value 计算（None=无法计算）
    """
    sig_map: dict[str, list[dict]] = defaultdict(list)
    for r in history:
        if r.get("操作建议") in _ENTRY_ACTIONS:
            key = f"{r.get('symbol','')}::{r.get('model_id','')}"
            sig_map[key].append(r)
    for v in sig_map.values():
        v.sort(key=lambda x: x.get("time", ""))

    # open_map: symbol::model → 按时间排序的开仓记录列表
    open_map: dict[str, list[dict]] = defaultdict(list)
    for r in (opens or []):
        key = f"{r.get('symbol','')}::{r.get('model','')}"
        open_map[key].append(r)
    for v in open_map.values():
        v.sort(key=lambda x: x.get("time", ""))

    def _find_open(key: str, close_dt: datetime, open_price) -> tuple[datetime | None, int | None]:
        """返回 (open_dt, holding_minutes)；找不到返回 (None, None)。"""
        if open_price is None:
            return None, None
        for rec in reversed(open_map.get(key, [])):
            try:
                o_dt = datetime.fromisoformat(rec.get("time", "").replace(" ", "T"))
            except Exception:
                continue
            if o_dt >= close_dt:
                continue
            # open_price 精确匹配（浮点容差 0.5 个最小变动单位，实际上完全相等）
            if abs((rec.get("price") or 0) - open_price) < 0.01:
                holding = round((close_dt - o_dt).total_seconds() / 60)
                return o_dt, holding
        return None, None

    def _find_signal(key: str, anchor_dt: datetime, window_seconds: float):
        """在 anchor_dt 前后 window_seconds 内找最近的入场信号。"""
        best_sig  = None
        best_dist = float("inf")
        for sig in sig_map.get(key, []):
            try:
                s_dt = datetime.fromisoformat(sig.get("time", "").replace(" ", "T"))
            except Exception:
                continue
            dist = (anchor_dt - s_dt).total_seconds()  # 正 = 信号在锚点前
            if -120 <= dist and dist <= window_seconds:  # 允许信号最多晚 2 分钟（早退写入延迟）
                if abs(dist) < best_dist:
                    best_dist = abs(dist)
                    best_sig  = sig
        return best_sig

    matched = []
    for trade in trades:
        sym        = trade.get("symbol", "")
        model      = trade.get("model", "")
        t_str      = trade.get("time", "")
        open_price = trade.get("open_price")
        key        = f"{sym}::{model}"

        try:
            t_dt = datetime.fromisoformat(t_str.replace(" ", "T"))
        except Exception:
            continue

        # 阶段 1：用 open 记录精确锚定开仓时间
        open_dt, holding_min = _find_open(key, t_dt, open_price)
        if open_dt is not None:
            best_sig = _find_signal(key, open_dt, 10 * 60)
        else:
            # 阶段 2：回退——以平仓时间为锚，6 小时窗口
            best_sig = _find_signal(key, t_dt, 6 * 3600)
            if best_sig:
                try:
                    s_dt = datetime.fromisoformat(best_sig.get("time", "").replace(" ", "T"))
                    holding_min = round((t_dt - s_dt).total_seconds() / 60)
                except Exception:
                    holding_min = None

        merged = {**trade}
        if best_sig:
            merged["_设置类型"]    = best_sig.get("设置类型", "未知")
            merged["_市场状态"]    = best_sig.get("市场状态", "未知")
            merged["_信号棒评级"]  = best_sig.get("信号棒评级", "未知")
            merged["_信号棒强度"]  = parse_bar_strength(best_sig.get("信号棒评级", ""))
            merged["_操作建议"]    = best_sig.get("操作建议", "未知")
            merged["_核心逻辑"]    = best_sig.get("核心逻辑", "")
            merged["_止损依据"]    = best_sig.get("止损依据") or "未知"
            merged["_风险等级"]    = best_sig.get("风险等级", "未知")
            merged["_持仓分钟"]    = holding_min

            # R倍数计算
            sl_price   = best_sig.get("止损价")
            multiplier = trade.get("multiplier", 1)
            volume     = trade.get("volume", 1)
            pnl        = trade.get("close_profit", 0.0)
            if sl_price and open_price and sl_price != open_price:
                r_value = abs(open_price - sl_price) * multiplier * volume
                merged["_R倍数"] = round(pnl / r_value, 2)
            else:
                merged["_R倍数"] = None
        else:
            merged["_设置类型"]   = "无法匹配"
            merged["_市场状态"]   = "无法匹配"
            merged["_信号棒评级"] = "无法匹配"
            merged["_信号棒强度"] = "无法匹配"
            merged["_操作建议"]   = "未知"
            merged["_核心逻辑"]   = ""
            merged["_止损依据"]   = "无法匹配"
            merged["_风险等级"]   = "无法匹配"
            merged["_持仓分钟"]   = holding_min  # open 记录存在时仍可记录真实持仓
            merged["_R倍数"]      = None

        matched.append(merged)

    return matched


# ─────────────────────────────────────────────────────────────
# 报告生成
# ─────────────────────────────────────────────────────────────

SEP  = "═" * 64
SEP2 = "─" * 64


def fmt_stat(s: dict, indent: int = 4) -> str:
    pad = " " * indent
    bar_count = min(int(s["胜率"] / 5), 20)
    win_bar = "█" * bar_count + "░" * (20 - bar_count)
    lines = [
        f"{pad}交易次数 : {s['交易次数']}",
        f"{pad}胜  率   : {s['胜率']}%  [{win_bar}]",
        f"{pad}总盈亏   : {s['总盈亏']:+.2f}",
        f"{pad}平均盈亏 : {s['平均盈亏']:+.2f}  (盈利均值:{s['平均盈利']:+.2f}  亏损均值:{s['平均亏损']:+.2f})",
        f"{pad}盈亏比   : {s['盈亏比']:.2f}  期望值:{s['期望值']:+.2f}",
        f"{pad}最大单笔 : 盈利{s['最大单笔盈利']:+.2f}  亏损{s['最大单笔亏损']:+.2f}",
    ]
    return "\n".join(lines)


def generate_report(
    history: list[dict],
    trades: list[dict],
    matched: list[dict],
    model_filter: str,
    days: int,
) -> str:
    lines = []
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    day_str = f"最近{days}天" if days else "全部时间"

    lines.append(SEP)
    lines.append("  AI 期货交易系统 — 统计分析报告")
    lines.append(f"  生成时间：{now_str}")
    lines.append(f"  筛选范围：{day_str}  模型：{model_filter or '全部'}")
    lines.append(SEP)

    # ── 0. 数据概览 ─────────────────────────────────────────
    lines.append("\n【数据概览】")
    lines.append(f"  分析信号总条数 : {len(history)}")
    action_dist = defaultdict(int)
    for r in history:
        action_dist[r.get("操作建议", "未知")] += 1
    for act, cnt in sorted(action_dist.items(), key=lambda x: -x[1]):
        pct = safe_div(cnt, len(history)) * 100
        lines.append(f"    {act:<6} : {cnt:>5} 条  ({pct:.1f}%)")

    lines.append(f"\n  平仓交易总笔数 : {len(trades)}")
    if not trades:
        lines.append("\n  ⚠️  没有平仓交易记录，无法生成盈亏统计。")
        lines.append("     可能原因：交易功能未启用，或尚未有平仓成交。")
        lines.append(SEP)
        return "\n".join(lines)

    all_profits = [t.get("close_profit", 0.0) for t in trades]
    # 预计算 r_vals 供后续结论节复用，避免跨节隐式依赖
    r_vals = [t["_R倍数"] for t in matched if t.get("_R倍数") is not None]

    # ── 1. 整体盈亏 ─────────────────────────────────────────
    lines.append("\n" + SEP2)
    lines.append("【整体盈亏统计】")
    lines.append(fmt_stat(stats_block(all_profits)))

    # 做多 vs 做空 方向拆分（用 trade.direction 字段，比信号匹配更准确）
    # 注：direction 是平仓委托方向："卖"=卖出平多仓，"买"=买入平空仓
    long_profits  = [t.get("close_profit", 0.0) for t in trades if t.get("direction") == "卖"]
    short_profits = [t.get("close_profit", 0.0) for t in trades if t.get("direction") == "买"]
    if long_profits or short_profits:
        lines.append("\n  多空方向拆分：")
        for label, dir_profits in [("做多平仓(卖出平多)", long_profits), ("做空平仓(买入平空)", short_profits)]:
            if not dir_profits:
                continue
            s = stats_block(dir_profits)
            flag = "✅" if s["总盈亏"] > 0 else "❌"
            lines.append(f"    {flag} {label}  次数:{s['交易次数']:>3}  胜率:{s['胜率']:>5.1f}%  总盈亏:{s['总盈亏']:>+10.2f}  期望:{s['期望值']:>+8.2f}")

    # R 倍数整体分布
    if r_vals:
        lines.append(f"\n  R倍数分析（{len(r_vals)}笔有止损价可计算）：")
        positive_r = [r for r in r_vals if r > 0]
        negative_r = [r for r in r_vals if r <= 0]
        avg_r = safe_div(sum(r_vals), len(r_vals))
        lines.append(f"    平均R倍数：{avg_r:+.2f}  正收益均值：{safe_div(sum(positive_r), len(positive_r)):+.2f}R  "
                     f"负收益均值：{safe_div(sum(negative_r), len(negative_r)):+.2f}R")
        bands_r = [(-float('inf'), 0), (0, 0.5), (0.5, 1), (1, 2), (2, 3), (3, float('inf'))]
        labels_r = ["亏损", "0~0.5R", "0.5~1R", "1~2R", "2~3R", "3R以上"]
        dist_parts = []
        for (lo, hi), lbl in zip(bands_r, labels_r):
            cnt = sum(1 for r in r_vals if lo <= r < hi)
            if cnt:
                dist_parts.append(f"{lbl}:{cnt}笔")
        lines.append(f"    分布：{' | '.join(dist_parts)}")

    # ── 2. 按模型分组 ────────────────────────────────────────
    lines.append("\n" + SEP2)
    lines.append("【按模型统计】")
    by_model: dict[str, list[float]] = defaultdict(list)
    for t in trades:
        by_model[t.get("model", "未知")].append(t.get("close_profit", 0.0))
    for mid, profits in sorted(by_model.items(), key=lambda x: -sum(x[1])):
        s = stats_block(profits)
        flag = "✅" if s["总盈亏"] > 0 else "❌"
        lines.append(f"\n  {flag} {mid}")
        lines.append(fmt_stat(s))

    # ── 3. 按品种分组 ────────────────────────────────────────
    lines.append("\n" + SEP2)
    lines.append("【按品种统计】")
    by_sym: dict[str, list[float]] = defaultdict(list)
    for t in trades:
        # 显示时去掉主连前缀 KQ.m@ 方便阅读
        sym_display = t.get("symbol", "未知").replace("KQ.m@", "")
        by_sym[sym_display].append(t.get("close_profit", 0.0))
    for sym, profits in sorted(by_sym.items(), key=lambda x: -sum(x[1])):
        s = stats_block(profits)
        flag = "✅" if s["总盈亏"] > 0 else "❌"
        lines.append(f"  {flag} {sym:<20} 次数:{s['交易次数']:>3}  总盈亏:{s['总盈亏']:>+10.2f}  "
                     f"胜率:{s['胜率']:>5.1f}%  盈亏比:{s['盈亏比']:.2f}  期望:{s['期望值']:>+8.2f}")

    # ── 4. 按设置类型分组（Al Brooks 核心诊断）──────────────
    lines.append("\n" + SEP2)
    lines.append("【按设置类型统计（Al Brooks 入场设置诊断）】")
    lines.append("  说明：将平仓交易与最近6小时内的入场信号匹配（做多/做空/反手）")

    by_setup: dict[str, list[float]] = defaultdict(list)
    for t in matched:
        by_setup[t.get("_设置类型", "未知")].append(t.get("close_profit", 0.0))

    setup_stats = {setup: stats_block(profits) for setup, profits in by_setup.items()}

    for setup, s in sorted(setup_stats.items(), key=lambda x: -x[1]["总盈亏"]):
        flag = "✅" if s["总盈亏"] > 0 else ("⚠️" if s["总盈亏"] == 0 else "❌")
        ev_flag = "👍" if s["期望值"] > 0 else "👎"
        lines.append(f"\n  {flag} {setup}")
        lines.append(f"    次数:{s['交易次数']}  胜率:{s['胜率']}%  总盈亏:{s['总盈亏']:+.2f}")
        lines.append(f"    盈亏比:{s['盈亏比']:.2f}  期望值:{s['期望值']:+.2f} {ev_flag}")

    # ── 5. 按市场状态（趋势强度）分组 ────────────────────────
    # 五类：强上升趋势 / 弱上升趋势 / 交易区间 / 弱下降趋势 / 强下降趋势
    lines.append("\n" + SEP2)
    lines.append("【按市场状态（趋势强度五分类）统计】")
    STATE_ORDER = ["强上升趋势", "弱上升趋势", "交易区间", "弱下降趋势", "强下降趋势"]
    by_mkt: dict[str, list[float]] = defaultdict(list)
    for t in matched:
        by_mkt[t.get("_市场状态", "未知")].append(t.get("close_profit", 0.0))

    ordered = [s for s in STATE_ORDER if s in by_mkt]
    others  = sorted([s for s in by_mkt if s not in STATE_ORDER], key=lambda x: -sum(by_mkt[x]))
    for mkt in ordered + others:
        s = stats_block(by_mkt[mkt])
        flag = "✅" if s["总盈亏"] > 0 else "❌"
        lines.append(f"  {flag} {mkt:<10} 次数:{s['交易次数']:>3}  胜率:{s['胜率']:>5.1f}%  "
                     f"总盈亏:{s['总盈亏']:>+10.2f}  期望:{s['期望值']:>+8.2f}")

    # ── 6. 按信号棒强度分组 ──────────────────────────────────
    # 信号棒评级格式为"强多头"/"弱空头"/"无"，提取强度部分进行统计
    lines.append("\n" + SEP2)
    lines.append("【按信号棒强度统计】")
    lines.append('  （评级原始值含方向，如"弱空头"→提取"弱"作为强度）')
    by_strength: dict[str, list[float]] = defaultdict(list)
    for t in matched:
        by_strength[t.get("_信号棒强度", "未知")].append(t.get("close_profit", 0.0))
    for rating in [r for r in ["强", "中", "弱", "无", "未知", "无法匹配"] if r in by_strength]:
        s = stats_block(by_strength[rating])
        flag = "✅" if s["总盈亏"] > 0 else "❌"
        lines.append(f"  {flag} {rating:<4}  次数:{s['交易次数']:>3}  胜率:{s['胜率']:>5.1f}%  "
                     f"盈亏比:{s['盈亏比']:.2f}  期望:{s['期望值']:>+8.2f}")

    # ── 8. 按风险等级分组 ────────────────────────────────────
    lines.append("\n" + SEP2)
    lines.append("【按风险等级统计】")
    by_risk: dict[str, list[float]] = defaultdict(list)
    for t in matched:
        by_risk[t.get("_风险等级", "未知")].append(t.get("close_profit", 0.0))
    for risk in [r for r in ["低", "中", "高", "未知", "无法匹配"] if r in by_risk]:
        s = stats_block(by_risk[risk])
        flag = "✅" if s["总盈亏"] > 0 else "❌"
        lines.append(f"  {flag} {risk:<4}  次数:{s['交易次数']:>3}  胜率:{s['胜率']:>5.1f}%  "
                     f"总盈亏:{s['总盈亏']:>+10.2f}  期望:{s['期望值']:>+8.2f}")

    # ── 9. 按止损依据分组 ────────────────────────────────────
    lines.append("\n" + SEP2)
    lines.append("【按止损依据统计】")
    by_sl: dict[str, list[float]] = defaultdict(list)
    for t in matched:
        by_sl[t.get("_止损依据", "未知")].append(t.get("close_profit", 0.0))
    for basis, profits in sorted(by_sl.items(), key=lambda x: -sum(x[1])):
        s = stats_block(profits)
        flag = "✅" if s["总盈亏"] > 0 else "❌"
        lines.append(f"  {flag} {basis:<10}  次数:{s['交易次数']:>3}  胜率:{s['胜率']:>5.1f}%  "
                     f"期望:{s['期望值']:>+8.2f}")

    # ── 10. 按交易时段分组 ───────────────────────────────────
    lines.append("\n" + SEP2)
    lines.append("【按交易时段统计】")
    by_sess: dict[str, list[float]] = defaultdict(list)
    for t in trades:
        by_sess[_trade_session(t.get("time", ""))].append(t.get("close_profit", 0.0))
    for sess in [s for s in ["早盘(09-11:30)", "午后(13-15)", "夜盘(21-03)", "其他", "未知"] if s in by_sess]:
        s = stats_block(by_sess[sess])
        flag = "✅" if s["总盈亏"] > 0 else "❌"
        lines.append(f"  {flag} {sess:<13}  次数:{s['交易次数']:>3}  胜率:{s['胜率']:>5.1f}%  "
                     f"总盈亏:{s['总盈亏']:>+10.2f}  期望:{s['期望值']:>+8.2f}")

    # ── 11. 模型 × 设置类型 交叉矩阵 ─────────────────────────
    lines.append("\n" + SEP2)
    lines.append("【模型 × 设置类型 交叉矩阵（期望值 / 笔数）】")
    all_models = sorted(set(t.get("model", "未知") for t in matched))
    # 只取有实际交易匹配的设置类型，排除"无法匹配"和"无有效设置"，按总笔数降序取前8
    valid_setups = [
        s for s in by_setup
        if s not in ("无法匹配", "未知", "无有效设置")
    ]
    valid_setups.sort(key=lambda x: -setup_stats[x]["交易次数"])
    cross_setups = valid_setups[:8]  # 最多8列避免过宽
    if all_models and cross_setups:
        header = f"  {'模型':<12}" + "".join(f"  {s[:9]:<11}" for s in cross_setups)
        lines.append(header)
        for mid in all_models:
            row = f"  {mid:<12}"
            for setup in cross_setups:
                cell = [t.get("close_profit", 0.0) for t in matched
                        if t.get("model") == mid and t.get("_设置类型") == setup]
                if cell:
                    ev = safe_div(sum(cell), len(cell))
                    row += f"  {ev:>+7.0f}元({len(cell):>2})"
                else:
                    row += f"  {'—':>10}"
            lines.append(row)

    # ── 12. 最近10笔交易明细 ─────────────────────────────────
    lines.append("\n" + SEP2)
    lines.append("【最近10笔交易明细】")
    recent = sorted(matched, key=lambda x: x.get("time", ""), reverse=True)[:10]
    for t in recent:
        pnl  = t.get("close_profit", 0.0)
        flag = "盈" if pnl > 0 else "亏"
        r_str = f"  {t['_R倍数']:+.2f}R" if t.get("_R倍数") is not None else ""
        hold  = f"  持{t['_持仓分钟']}m" if t.get("_持仓分钟") is not None else ""
        lines.append(
            f"  [{t.get('time','')[:16]}] "
            f"{t.get('symbol','').replace('KQ.m@',''):<18} "
            f"{t.get('_操作建议',''):<4} "
            f"{t.get('_设置类型',''):<10} "
            f"信号:{t.get('_信号棒评级',''):<6}  "
            f"{flag}{abs(pnl):>8.2f}{r_str}{hold}"
        )

    # ── 13. 关键结论 ─────────────────────────────────────────
    lines.append("\n" + SEP)
    lines.append("【关键结论与建议】")

    positive_setups = [(s, st) for s, st in setup_stats.items() if st["期望值"] > 0 and st["交易次数"] >= 3]
    negative_setups = [(s, st) for s, st in setup_stats.items() if st["期望值"] < 0 and st["交易次数"] >= 3]

    if positive_setups:
        lines.append("\n  ✅ 正向期望设置（建议保留）：")
        for setup, s in sorted(positive_setups, key=lambda x: -x[1]["期望值"]):
            lines.append(f"     {setup:<15} 期望:{s['期望值']:+.2f}  胜率:{s['胜率']}%  ({s['交易次数']}笔)")
    else:
        lines.append("\n  ⚠️  暂无足够样本量的正向期望设置类型（每类需≥3笔）")

    if negative_setups:
        lines.append("\n  ❌ 负向期望设置（建议减少）：")
        for setup, s in sorted(negative_setups, key=lambda x: x[1]["期望值"]):
            lines.append(f"     {setup:<15} 期望:{s['期望值']:+.2f}  胜率:{s['胜率']}%  ({s['交易次数']}笔)")

    # 信号棒强度有效性
    strong = [t.get("close_profit", 0.0) for t in matched if t.get("_信号棒强度") == "强"]
    weak   = [t.get("close_profit", 0.0) for t in matched if t.get("_信号棒强度") == "弱"]
    if strong and weak:
        s_ev = safe_div(sum(strong), len(strong))
        w_ev = safe_div(sum(weak),   len(weak))
        cmp  = "✅ 有效" if s_ev > w_ev else "⚠️  区分度不足"
        lines.append(f"  强信号棒期望({s_ev:+.2f}) vs 弱信号棒({w_ev:+.2f}) → {cmp}")

    # 趋势 vs 区间
    trend_p = [t.get("close_profit", 0.0) for t in matched if "趋势" in t.get("_市场状态", "")]
    range_p = [t.get("close_profit", 0.0) for t in matched if t.get("_市场状态") == "交易区间"]
    if trend_p and range_p:
        t_ev = safe_div(sum(trend_p), len(trend_p))
        r_ev = safe_div(sum(range_p), len(range_p))
        lines.append(f"  趋势市期望({t_ev:+.2f} / {len(trend_p)}笔) vs 区间市({r_ev:+.2f} / {len(range_p)}笔)")

    # R倍数结论
    if r_vals:
        avg_r = safe_div(sum(r_vals), len(r_vals))
        r_win = sum(1 for r in r_vals if r > 0)
        lines.append(f"  R倍数：平均{avg_r:+.2f}R  正收益率:{safe_div(r_win,len(r_vals))*100:.0f}%  "
                     f"({'系统整体正期望' if avg_r > 0 else '系统整体负期望，需改进入场或止损策略'})")

    lines.append("\n" + SEP)
    lines.append("  报告结束")
    lines.append(SEP)

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="AI期货交易系统统计分析")
    parser.add_argument("--data",  default=_default_data_dir, help=f"数据目录路径（默认：{_default_data_dir}）")
    parser.add_argument("--model", default="",     help="只分析某个模型（如 claude / deepseek）")
    parser.add_argument("--days",  default=0, type=int, help="只看最近N天（默认：全部）")
    parser.add_argument("--out",   default="trade_report.txt", help="输出报告文件名")
    args = parser.parse_args()

    data_dir = pathlib.Path(args.data)
    if not data_dir.exists():
        print(f"❌ 数据目录不存在：{data_dir.resolve()}")
        print("   请确认 --data 参数正确，或在项目根目录运行此脚本")
        return

    print(f"📂 数据目录：{data_dir.resolve()}")
    print("🔍 加载数据中...")

    history, trades, opens = load_all_data(data_dir, args.model, args.days)
    print(f"   分析记录：{len(history)} 条")
    print(f"   平仓交易：{len(trades)} 笔")

    if not history and not trades:
        print("⚠️  没有找到任何数据，请检查目录路径")
        return

    print("🔗 匹配信号与交易中...")
    matched = match_signals_to_trades(history, trades, opens)
    matched_cnt = sum(1 for t in matched if t.get("_设置类型") != "无法匹配")
    r_cnt = sum(1 for t in matched if t.get("_R倍数") is not None)
    print(f"   成功匹配：{matched_cnt}/{len(trades)} 笔  其中可算R倍数：{r_cnt} 笔")

    print("📊 生成报告中...")
    report = generate_report(history, trades, matched, args.model, args.days)

    print("\n" + report)

    out_path = pathlib.Path(args.out)
    out_path.write_text(report, encoding="utf-8")
    print(f"\n✅ 报告已保存到：{out_path.resolve()}")


if __name__ == "__main__":
    main()
