# -*- coding: utf-8 -*-
"""
追踪止损推算器
输入成本价、止损、止盈，推算各阶段止损位和模拟价格运动表
逻辑与 data_feed.py _run_loop 完全对齐
"""
import sys
import unicodedata
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stdin.reconfigure(encoding="utf-8")


# ── 中文对齐辅助 ──────────────────────────────────────────
def _dw(s):
    """计算字符串在终端的显示宽度（CJK字符占2格）"""
    w = 0
    for c in str(s):
        ew = unicodedata.east_asian_width(c)
        w += 2 if ew in ('W', 'F') else 1
    return w

def _lj(s, width):
    """按显示宽度左对齐补空格"""
    s = str(s)
    return s + ' ' * max(width - _dw(s), 0)


# ── 核心追踪止损计算 ──────────────────────────────────────
def _calc_trailing(entry, ini_risk, peak, tp, tick, direction):
    """返回 (stop_loss, trail_dist, floor, profit_r)"""
    if direction == "LONG":
        profit_r = (peak - entry) / ini_risk if ini_risk else 0
        if profit_r < 0.6:
            trail_dist, floor = None, None
        elif profit_r < 1.0:
            trail_dist = 1.0 * ini_risk
            floor = entry + tick
        elif profit_r < 2.0:
            trail_dist = 0.7 * ini_risk
            floor = entry + 0.3 * ini_risk
        else:
            trail_dist = 0.5 * ini_risk
            floor = entry + 1.5 * ini_risk

        if trail_dist is not None and tp and entry:
            total_zone = tp - entry  # 多头：正值
            dist_to_tp = tp - peak
            if total_zone > 0:
                dist_ratio = dist_to_tp / total_zone
                if 0 <= dist_ratio <= 0.15:
                    trail_dist = min(trail_dist, 0.15 * ini_risk)
                elif dist_ratio <= 0.35:
                    trail_dist = min(trail_dist, 0.3 * ini_risk)

        if trail_dist is not None:
            new_stop = peak - trail_dist
            if floor is not None:
                new_stop = max(new_stop, floor)
        else:
            new_stop = entry - ini_risk

    else:  # SHORT
        profit_r = (entry - peak) / ini_risk if ini_risk else 0
        if profit_r < 0.6:
            trail_dist, floor = None, None
        elif profit_r < 1.0:
            trail_dist = 1.0 * ini_risk
            floor = entry - tick
        elif profit_r < 2.0:
            trail_dist = 0.7 * ini_risk
            floor = entry - 0.3 * ini_risk
        else:
            trail_dist = 0.5 * ini_risk
            floor = entry - 1.5 * ini_risk

        if trail_dist is not None and tp and entry:
            total_zone = entry - tp  # 空头：正值
            dist_to_tp = peak - tp
            if total_zone > 0:
                dist_ratio = dist_to_tp / total_zone
                if 0 <= dist_ratio <= 0.15:
                    trail_dist = min(trail_dist, 0.15 * ini_risk)
                elif dist_ratio <= 0.35:
                    trail_dist = min(trail_dist, 0.3 * ini_risk)

        if trail_dist is not None:
            new_stop = peak + trail_dist
            if floor is not None:
                new_stop = min(new_stop, floor)
        else:
            new_stop = entry + ini_risk

    return new_stop, trail_dist, floor, profit_r


def _fmt(v, decimals=1):
    if v is None:
        return "-"
    return f"{v:.{decimals}f}"


def main():
    SEP = "═" * 54
    print(SEP)
    print("  追踪止损推算器")
    print(SEP)

    raw_dir = input("\ndirection (l=long / s=short, default l): ").strip().lower() or "l"
    direction = "LONG" if raw_dir in ("l", "long") else "SHORT"

    entry    = float(input("entry price       : ").strip())
    sl       = float(input("stop loss price   : ").strip())
    tp       = float(input("take profit price : ").strip())
    raw_tick = input("min tick (default 1): ").strip()
    tick     = float(raw_tick) if raw_tick else 1.0

    if direction == "LONG":
        ini_risk = entry - sl
        if ini_risk <= 0:
            print("错误：多头止损价应低于开仓价")
            return
        reward = tp - entry
    else:
        ini_risk = sl - entry
        if ini_risk <= 0:
            print("错误：空头止损价应高于开仓价")
            return
        reward = entry - tp

    rr = reward / ini_risk if ini_risk else 0

    print("\n" + SEP)
    print(f"  {'LONG' if direction == 'LONG' else 'SHORT'}  entry={entry}  R={ini_risk:.2f}  R:R=1:{rr:.2f}")
    print(SEP)

    # ── 阶段表 ────────────────────────────────────────────
    # 列宽（显示宽度）
    C = [16, 12, 12, 14, 10]
    print("\n【追踪止损各阶段价位】")
    print(_lj("阶段",        C[0]) + _lj("激活峰值",   C[1]) +
          _lj("追踪距离",    C[2]) + _lj("底线(floor)", C[3]) + "止损位")
    print("-" * sum(C))

    if direction == "LONG":
        stage_peaks = [
            ("初始（<0.6R）", None),
            ("激活（0.6R）",  entry + 0.6 * ini_risk),
            ("加速（1R）",    entry + 1.0 * ini_risk),
            ("极速（2R）",    entry + 2.0 * ini_risk),
        ]
        for label, peak_at in stage_peaks:
            if peak_at is None:
                sl_at = entry - ini_risk
                print(_lj(label, C[0]) + _lj("< " + _fmt(entry + 0.6*ini_risk), C[1]) +
                      _lj("-", C[2]) + _lj("-", C[3]) + _fmt(sl_at))
            else:
                sl_at, td, fl, _ = _calc_trailing(entry, ini_risk, peak_at, tp, tick, direction)
                print(_lj(label, C[0]) + _lj(_fmt(peak_at), C[1]) +
                      _lj(_fmt(td), C[2]) + _lj(_fmt(fl), C[3]) + _fmt(sl_at))
    else:
        stage_peaks = [
            ("初始（<0.6R）", None),
            ("激活（0.6R）",  entry - 0.6 * ini_risk),
            ("加速（1R）",    entry - 1.0 * ini_risk),
            ("极速（2R）",    entry - 2.0 * ini_risk),
        ]
        for label, peak_at in stage_peaks:
            if peak_at is None:
                sl_at = entry + ini_risk
                print(_lj(label, C[0]) + _lj("> " + _fmt(entry - 0.6*ini_risk), C[1]) +
                      _lj("-", C[2]) + _lj("-", C[3]) + _fmt(sl_at))
            else:
                sl_at, td, fl, _ = _calc_trailing(entry, ini_risk, peak_at, tp, tick, direction)
                print(_lj(label, C[0]) + _lj(_fmt(peak_at), C[1]) +
                      _lj(_fmt(td), C[2]) + _lj(_fmt(fl), C[3]) + _fmt(sl_at))

    # ── 止盈感知层说明 ────────────────────────────────────
    print("\n【止盈感知层收紧区】（基于 entry→TP 行程比例）")
    if direction == "LONG":
        total_zone = tp - entry
        near_tp  = tp - 0.35 * total_zone
        xnear_tp = tp - 0.15 * total_zone
        print(f"  价格 ≥ {_fmt(near_tp)}（行程剩余 ≤ 35%） → 追踪距离压至 0.3R  = {0.3*ini_risk:.2f}")
        print(f"  价格 ≥ {_fmt(xnear_tp)}（行程剩余 ≤ 15%） → 追踪距离压至 0.15R = {0.15*ini_risk:.2f}")
    else:
        total_zone = entry - tp
        near_tp  = tp + 0.35 * total_zone
        xnear_tp = tp + 0.15 * total_zone
        print(f"  价格 ≤ {_fmt(near_tp)}（行程剩余 ≤ 35%） → 追踪距离压至 0.3R  = {0.3*ini_risk:.2f}")
        print(f"  价格 ≤ {_fmt(xnear_tp)}（行程剩余 ≤ 15%） → 追踪距离压至 0.15R = {0.15*ini_risk:.2f}")

    # ── 模拟价格运动表 ────────────────────────────────────
    print("\n【模拟价格运动（每0.1R一档）】")
    # 列宽
    D = [10, 7, 10, 10, 10, 12]
    print(_lj("峰值",     D[0]) + _lj("R",     D[1]) +
          _lj("追踪距离", D[2]) + _lj("底线",  D[3]) +
          _lj("止损位",   D[4]) + _lj("锁定利润", D[5]) + "状态")
    print("-" * (sum(D) + 6))

    prev_stop = None
    # 生成 0.1R 步进，包含止盈点
    n_steps = round(rr / 0.1) + 2
    r_steps = [round(i * 0.1, 10) for i in range(n_steps)]
    if rr > 0 and r_steps[-1] < rr:
        r_steps.append(rr)

    for r_mul in r_steps:
        if direction == "LONG":
            peak = entry + r_mul * ini_risk
        else:
            peak = entry - r_mul * ini_risk

        stop, td, fl, profit_r = _calc_trailing(entry, ini_risk, peak, tp, tick, direction)

        if direction == "LONG":
            locked = stop - entry
            is_tp = peak >= tp
        else:
            locked = entry - stop
            is_tp = peak <= tp

        locked_str = f"+{locked:.2f}" if locked > 0 else f"{locked:.2f}"
        if td is None and not is_tp:
            locked_str = "(初始止损)"

        if is_tp:
            status     = "止盈触发 ✓"
            stop_str   = "──"
            td_str     = "──"
            fl_str     = "──"
            locked_str = f"+{reward:.2f}"
        else:
            status   = ""
            stop_str = _fmt(stop)
            td_str   = _fmt(td) if td else "-"
            fl_str   = _fmt(fl) if fl else "-"
            if prev_stop is not None:
                if direction == "LONG" and stop < prev_stop - 0.001:
                    status = "⚠ 止损后退"
                elif direction == "SHORT" and stop > prev_stop + 0.001:
                    status = "⚠ 止损后退"

        print(_lj(_fmt(peak), D[0]) + _lj(f"{r_mul:.1f}", D[1]) +
              _lj(td_str,     D[2]) + _lj(fl_str,          D[3]) +
              _lj(stop_str,   D[4]) + _lj(locked_str,       D[5]) + status)

        if not is_tp:
            prev_stop = stop
        else:
            break

    print("\n" + SEP)


if __name__ == "__main__":
    main()
