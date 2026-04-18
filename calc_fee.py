#!/usr/bin/env python3
"""
手续费计算工具
计算 equity_*.jsonl 中每笔交易扣除交易手续费后的真实净收益。

用法：
  python calc_fee.py                                           # 默认 data/sim/equity_cursor.jsonl
  python calc_fee.py data/live/equity_claude.jsonl             # 指定文件
  python calc_fee.py --model claude                            # 按模型过滤（默认文件）
  python calc_fee.py data/live/equity_claude.jsonl --model gemini  # 指定文件 + 按模型过滤
  python calc_fee.py --all                                     # 扫描 data/ 下所有 equity_*.jsonl
  python calc_fee.py --all --model deepseek                    # 所有文件中只统计 deepseek
"""

import io
import json
import sys
import unicodedata
from collections import defaultdict, deque
from pathlib import Path

# Windows 终端强制 UTF-8 输出
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ─────────────────────────────────────────────────────────────────────────────
# 手续费表（直接维护此处，无需外部文件）
# 格式：
#   代码(大写): {
#     "std":      (type, value),   # 交易所手续费（开仓 + 平昨）
#     "intraday": (type, value),   # 日内平今仓手续费
#     "mult":     float,           # 合约乘数（pct 类型计费需要）
#   }
# type: "flat" = N元/手（固定），"pct" = 万分之N（按成交额比例）
# intraday 为 ("flat", 0.0) 表示平今免手续费（0元）
# ─────────────────────────────────────────────────────────────────────────────
FEE_TABLE = {
    # ── 郑商所（CZCE）────────────────────────────────────────────────────────
    "SA":  {"std": ("pct",  0.0002),   "intraday": ("pct",  0.0002),   "mult": 20},   # 纯碱
    "TA":  {"std": ("flat", 3.0),      "intraday": ("flat", 0.0),      "mult": 5},    # PTA（平今同标准）
    "SR":  {"std": ("flat", 3.0),      "intraday": ("flat", 3.0),      "mult": 10},   # 白糖
    "FG":  {"std": ("flat", 6.0),      "intraday": ("flat", 6.0),      "mult": 20},   # 玻璃
    "PF":  {"std": ("flat", 2.0),      "intraday": ("flat", 0.0),      "mult": 5},    # 短纤
    "SH":  {"std": ("pct",  0.0001),   "intraday": ("flat", 0.0),      "mult": 30},   # 烧碱
    "PX":  {"std": ("pct",  0.0001),   "intraday": ("flat", 0.0),      "mult": 5},    # 对二甲苯
    "MA":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.0001),   "mult": 10},   # 甲醇
    "PL":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.0001),   "mult": 20},   # 丙烯
    "CF":  {"std": ("flat", 4.3),      "intraday": ("flat", 0.0),      "mult": 5},    # 棉花
    "CY":  {"std": ("flat", 1.0),      "intraday": ("flat", 0.0),      "mult": 5},    # 棉纱
    "AP":  {"std": ("flat", 5.0),      "intraday": ("flat", 20.0),     "mult": 10},   # 苹果
    "CJ":  {"std": ("flat", 3.0),      "intraday": ("flat", 3.0),      "mult": 5},    # 红枣
    "RM":  {"std": ("flat", 1.5),      "intraday": ("flat", 1.5),      "mult": 10},   # 菜粕
    "RS":  {"std": ("flat", 2.0),      "intraday": ("flat", 2.0),      "mult": 10},   # 菜籽
    "OI":  {"std": ("flat", 2.0),      "intraday": ("flat", 2.0),      "mult": 10},   # 菜油
    "PK":  {"std": ("flat", 4.0),      "intraday": ("flat", 4.0),      "mult": 5},    # 花生
    "SF":  {"std": ("flat", 3.0),      "intraday": ("flat", 0.0),      "mult": 5},    # 硅铁
    "SM":  {"std": ("flat", 3.0),      "intraday": ("flat", 0.0),      "mult": 5},    # 锰硅
    "UR":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.0001),   "mult": 20},   # 尿素
    "WH":  {"std": ("flat", 30.0),     "intraday": ("flat", 30.0),     "mult": 20},   # 强麦
    "RI":  {"std": ("flat", 2.5),      "intraday": ("flat", 2.5),      "mult": 20},   # 早籼稻
    "LR":  {"std": ("flat", 3.0),      "intraday": ("flat", 3.0),      "mult": 20},   # 晚籼稻
    "JR":  {"std": ("flat", 3.0),      "intraday": ("flat", 3.0),      "mult": 20},   # 粳稻
    "PR":  {"std": ("pct",  0.00005),  "intraday": ("flat", 0.0),      "mult": 15},   # 瓶片
    # ── 大商所（DCE）────────────────────────────────────────────────────────
    "I":   {"std": ("pct",  0.0001),   "intraday": ("pct",  0.0001),   "mult": 100},  # 铁矿石
    "J":   {"std": ("pct",  0.0001),   "intraday": ("pct",  0.00014),  "mult": 100},  # 焦炭
    "JM":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.0001),   "mult": 60},   # 焦煤
    "M":   {"std": ("flat", 1.5),      "intraday": ("flat", 1.5),      "mult": 10},   # 豆粕
    "Y":   {"std": ("flat", 2.5),      "intraday": ("flat", 2.5),      "mult": 10},   # 豆油
    "P":   {"std": ("flat", 2.5),      "intraday": ("flat", 2.5),      "mult": 10},   # 棕榈油
    "A":   {"std": ("flat", 2.0),      "intraday": ("flat", 2.0),      "mult": 10},   # 豆一
    "B":   {"std": ("flat", 1.0),      "intraday": ("flat", 1.0),      "mult": 10},   # 豆二
    "BB":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.0001),   "mult": 500},  # 胶合板
    "FB":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.0001),   "mult": 10},   # 纤维板
    "C":   {"std": ("flat", 1.2),      "intraday": ("flat", 1.2),      "mult": 10},   # 玉米
    "CS":  {"std": ("flat", 1.5),      "intraday": ("flat", 1.5),      "mult": 10},   # 玉米淀粉
    "RR":  {"std": ("flat", 1.0),      "intraday": ("flat", 1.0),      "mult": 10},   # 粳米
    "L":   {"std": ("flat", 1.0),      "intraday": ("flat", 1.0),      "mult": 5},    # 塑料
    "V":   {"std": ("flat", 1.0),      "intraday": ("flat", 1.0),      "mult": 5},    # PVC
    "PP":  {"std": ("flat", 1.0),      "intraday": ("flat", 1.0),      "mult": 5},    # 聚丙烯
    "BZ":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.0001),   "mult": 30},   # 纯苯
    "EB":  {"std": ("flat", 3.0),      "intraday": ("flat", 3.0),      "mult": 5},    # 苯乙烯
    "EG":  {"std": ("flat", 3.0),      "intraday": ("flat", 3.0),      "mult": 10},   # 乙二醇
    "JD":  {"std": ("pct",  0.00015),  "intraday": ("pct",  0.00015),  "mult": 10},   # 鸡蛋
    "LH":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.0002),   "mult": 16},   # 生猪
    "PG":  {"std": ("flat", 6.0),      "intraday": ("flat", 6.0),      "mult": 20},   # 液化气
    "LG":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.0001),   "mult": 90},   # 原木
    # ── 能源中心（INE）──────────────────────────────────────────────────────
    "SC":  {"std": ("flat", 40.0),     "intraday": ("flat", 240.0),    "mult": 1000}, # 原油
    "LU":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.0003),   "mult": 10},   # LU燃油
    "NR":  {"std": ("pct",  0.0001),   "intraday": ("flat", 0.0),      "mult": 10},   # 20号胶
    "BC":  {"std": ("pct",  0.00001),  "intraday": ("flat", 0.0),      "mult": 5},    # 国际铜
    "EC":  {"std": ("pct",  0.0006),   "intraday": ("pct",  0.0012),   "mult": 50},   # 集运指数欧线
    # ── 上期所（SHFE）───────────────────────────────────────────────────────
    "CU":  {"std": ("pct",  0.00005),  "intraday": ("pct",  0.0001),   "mult": 5},    # 铜
    "AL":  {"std": ("flat", 3.0),      "intraday": ("flat", 3.0),      "mult": 5},    # 铝
    "AO":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.0001),   "mult": 20},   # 氧化铝
    "AD":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.0001),   "mult": 10},   # 铸造铝合金
    "ZN":  {"std": ("flat", 3.0),      "intraday": ("flat", 0.0),      "mult": 5},    # 锌
    "PB":  {"std": ("pct",  0.00004),  "intraday": ("flat", 0.0),      "mult": 5},    # 铅
    "NI":  {"std": ("flat", 3.0),      "intraday": ("flat", 3.0),      "mult": 1},    # 镍
    "SN":  {"std": ("flat", 3.0),      "intraday": ("flat", 3.0),      "mult": 1},    # 锡
    "AG":  {"std": ("pct",  0.00005),  "intraday": ("pct",  0.00025),  "mult": 15},   # 白银（活跃合约2603-2606/2612 std=万分之0.5）
    "AU":  {"std": ("flat", 20.0),     "intraday": ("flat", 0.0),      "mult": 1000}, # 黄金（活跃合约2603-2606/2612 std=20元；平今同标准）
    "RB":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.0001),   "mult": 10},   # 螺纹钢（活跃合约2603-2606/2610/2701 std=万分之1）
    "HC":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.0001),   "mult": 10},   # 热卷（活跃合约2603-2606/2610/2701 std=万分之1）
    "WR":  {"std": ("pct",  0.00004),  "intraday": ("flat", 0.0),      "mult": 10},   # 线材
    "SS":  {"std": ("flat", 2.0),      "intraday": ("flat", 0.0),      "mult": 5},    # 不锈钢
    "FU":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.0003),   "mult": 10},   # 燃料油
    "BU":  {"std": ("pct",  0.00005),  "intraday": ("flat", 0.0),      "mult": 10},   # 沥青
    "RU":  {"std": ("flat", 3.0),      "intraday": ("flat", 0.0),      "mult": 10},   # 橡胶
    "BR":  {"std": ("pct",  0.00002),  "intraday": ("pct",  0.00002),  "mult": 5},    # 合成橡胶
    "SP":  {"std": ("pct",  0.00005),  "intraday": ("flat", 0.0),      "mult": 10},   # 纸浆
    "OP":  {"std": ("pct",  0.0001),   "intraday": ("flat", 0.0),      "mult": 10},   # 胶版印刷纸
    "PT":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.00025),  "mult": 1000}, # 铂（活跃合约2606 intraday=万分之2.5）
    # ── 广期所（GFEX）───────────────────────────────────────────────────────
    "SI":  {"std": ("pct",  0.0001),   "intraday": ("flat", 0.0),      "mult": 5},    # 工业硅
    "LC":  {"std": ("pct",  0.00032),  "intraday": ("pct",  0.00032),  "mult": 1},    # 碳酸锂（活跃合约2604-2701 std/intraday=万分之3.2）
    "PS":  {"std": ("pct",  0.0005),   "intraday": ("flat", 0.0),      "mult": 5},    # 多晶硅（活跃合约2604-2701 std=万分之5；平今同标准）
    "PD":  {"std": ("pct",  0.0001),   "intraday": ("pct",  0.00025),  "mult": 100},  # 钯（活跃合约2606 intraday=万分之2.5）
    # ── 中金所（CFFEX）──────────────────────────────────────────────────────
    "IF":  {"std": ("pct",  0.000023), "intraday": ("pct",  0.00023),  "mult": 300},  # 沪深300
    "IC":  {"std": ("pct",  0.000023), "intraday": ("pct",  0.00023),  "mult": 200},  # 中证500
    "IM":  {"std": ("pct",  0.000023), "intraday": ("pct",  0.00023),  "mult": 200},  # 中证1000
    "IH":  {"std": ("pct",  0.000023), "intraday": ("pct",  0.00023),  "mult": 300},  # 上证50
    "T":   {"std": ("flat", 3.0),      "intraday": ("flat", 0.0),      "mult": 10000},# 10年国债
    "TL":  {"std": ("flat", 3.0),      "intraday": ("flat", 0.0),      "mult": 10000},# 30年国债
    "TF":  {"std": ("flat", 3.0),      "intraday": ("flat", 0.0),      "mult": 10000},# 5年国债
    "TS":  {"std": ("flat", 3.0),      "intraday": ("flat", 0.0),      "mult": 20000},# 2年国债
}


def _dw(s: str) -> int:
    """字符串显示宽度（CJK/全角字符计2列）"""
    return sum(2 if unicodedata.east_asian_width(c) in ('W', 'F') else 1 for c in s)

def _ljust(s: str, w: int) -> str:
    return s + ' ' * max(w - _dw(s), 0)

def _rjust(s: str, w: int) -> str:
    return ' ' * max(w - _dw(s), 0) + s


def parse_symbol(symbol: str) -> tuple[str, str]:
    """'KQ.m@SHFE.br' → ('SHFE', 'BR')"""
    after_at = symbol.split("@")[1]        # 'SHFE.br'
    exchange, code = after_at.split(".", 1)
    return exchange.upper(), code.upper()


def calc_leg_fee(price: float, volume: int, code: str,
                 use_intraday: bool, mult_override: float | None = None) -> float | None:
    """
    计算单边手续费（元）。
    use_intraday=True  → 使用日内平今仓费率
    mult_override      → 优先使用 trade 记录中的 multiplier（比表中更准）
    返回 None 表示合约不在 FEE_TABLE，调用方负责警告。
    """
    entry = FEE_TABLE.get(code)
    if entry is None:
        return None

    if use_intraday:
        ftype, rate = entry["intraday"]
    else:
        ftype, rate = entry["std"]

    mult = mult_override if mult_override is not None else entry["mult"]

    if ftype == "flat":
        return rate * volume * 1.2
    else:
        return price * mult * rate * volume * 1.2


def analyze(jsonl_path: Path, model_filter: str | None = None) -> None:
    records = []
    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rec = json.loads(line)
                if model_filter is None or rec.get("model") == model_filter:
                    records.append(rec)

    # FIFO 队列：symbol → deque of open records
    open_queues: dict[str, deque] = defaultdict(deque)
    # 已完成回合
    completed: list[dict] = []
    # 未识别合约
    unknown_codes: set[str] = set()

    for rec in records:
        rtype = rec.get("type")
        symbol = rec.get("symbol", "")
        _, code = parse_symbol(symbol)

        if rtype == "open":
            open_queues[symbol].append(rec)

        elif rtype == "trade":
            offset = rec.get("offset", "平仓")
            use_intraday = True  # 默认日内交易，统一用平今费率
            close_price = rec.get("price", 0.0)
            volume = rec.get("volume", 1)
            mult = rec.get("multiplier")  # trade 记录带乘数

            close_fee = calc_leg_fee(close_price, volume, code, use_intraday, mult)
            if close_fee is None:
                unknown_codes.add(f"{symbol}({code})")

            # 匹配对应的 open 记录
            open_rec = open_queues[symbol].popleft() if open_queues[symbol] else None
            if open_rec is not None:
                open_price = open_rec.get("price", 0.0)
                open_vol = open_rec.get("volume", 1)
                open_fee = calc_leg_fee(open_price, open_vol, code, False, mult)
            else:
                open_fee = None  # 无匹配 open（反手等特殊情况）

            completed.append({
                "time": rec.get("time", ""),
                "symbol": symbol,
                "code": code,
                "offset": offset,
                "gross": rec.get("close_profit", 0.0),
                "open_fee": open_fee,
                "close_fee": close_fee,
            })

    # 统计未平仓
    unclosed = []
    for sym, q in open_queues.items():
        for o in q:
            _, code = parse_symbol(sym)
            open_price = o.get("price", 0.0)
            vol = o.get("volume", 1)
            fee = calc_leg_fee(open_price, vol, code, False)
            unclosed.append({
                "symbol": sym,
                "price": open_price,
                "volume": vol,
                "open_fee": fee,
            })

    # ── 输出 ────────────────────────────────────────────────────────────────
    # 列宽（显示列数，CJK字符占2列）
    W_DT    = 11   # 03-16 09:50
    W_SYM   = 10   # SHFE.rb / CZCE.SA
    W_OFF   =  4   # 平今/平仓/平昨
    W_NUM   = 10   # +12345.00
    W_FEE   =  9   # -1234.56
    W = 2 + W_DT + 2 + W_SYM + 2 + W_OFF + 2 + W_NUM + 2 + W_FEE + 2 + W_FEE + 2 + W_NUM
    bar = "━" * W
    sep = "─" * W

    def row(dt, sym, off, gross, ofee, cfee, net):
        return ("  " + _ljust(dt,  W_DT)  + "  "
                     + _ljust(sym, W_SYM) + "  "
                     + _ljust(off, W_OFF) + "  "
                     + _rjust(gross, W_NUM) + "  "
                     + _rjust(ofee,  W_FEE) + "  "
                     + _rjust(cfee,  W_FEE) + "  "
                     + _rjust(net,   W_NUM))

    model_label = f"  模型：{model_filter}" if model_filter else ""
    print(bar)
    print(f"  手续费分析 — {jsonl_path}{model_label}")
    print(bar)
    print(row("时间", "合约", "偏移", "毛利润", "开仓费", "平仓费", "净利润"))
    print(sep)

    total_gross = 0.0
    total_fee = 0.0
    total_net = 0.0

    for r in completed:
        dt = r["time"][5:16].replace("T", " ") if r["time"] else "—"
        sym_short = r["symbol"].split("@")[1] if "@" in r["symbol"] else r["symbol"]
        offset = r["offset"]
        gross = r["gross"]
        ofee = r["open_fee"]
        cfee = r["close_fee"]

        ofee_str = f"{-ofee:+9.2f}" if ofee is not None else "N/A"
        cfee_str = f"{-cfee:+9.2f}" if cfee is not None else "N/A"

        if ofee is not None and cfee is not None:
            net = gross - ofee - cfee
            net_str = f"{net:+10.2f}"
            total_gross += gross
            total_fee += ofee + cfee
            total_net += net
        else:
            net_str = "N/A"

        print(row(dt, sym_short, offset, f"{gross:+10.2f}", ofee_str, cfee_str, net_str))

    print(sep)
    trade_count = len(completed)
    fee_pct = (-total_fee / total_gross * 100) if total_gross != 0 else 0.0
    print(f"  汇总")
    print(f"    交易笔数：  {trade_count:>6} 笔")
    print(f"    毛收益：    {total_gross:>+10.2f} 元")
    print(f"    总手续费：  {-total_fee:>+10.2f} 元")
    print(f"    净收益：    {total_net:>+10.2f} 元   ({fee_pct:.1f}%)")
    print(bar)

    if unknown_codes:
        print(f"  ⚠ 未识别合约（请在 FEE_TABLE 中添加）：{', '.join(sorted(unknown_codes))}")
    else:
        print(f"  未识别合约：无")

    if unclosed:
        print(f"  未平仓（已计开仓手续费）：")
        for u in unclosed:
            sym_short = u["symbol"].split("@")[1] if "@" in u["symbol"] else u["symbol"]
            fee_str = f"-{u['open_fee']:.2f}元" if u["open_fee"] is not None else "N/A"
            print(f"    {sym_short} ×{u['volume']}手  开仓价={u['price']}  开仓费={fee_str}")
    else:
        print(f"  未平仓：无")

    print(bar)


def main():
    args = sys.argv[1:]

    # 提取 --model 参数
    model_filter = None
    if "--model" in args:
        idx = args.index("--model")
        if idx + 1 >= len(args):
            print("错误：--model 需要指定模型名称，例如 --model claude")
            sys.exit(1)
        model_filter = args[idx + 1]
        args = args[:idx] + args[idx + 2:]

    if "--all" in args:
        base = Path(".")
        files = sorted(base.glob("data/*/equity_*.jsonl"))
        if not files:
            print("未找到任何 equity_*.jsonl 文件")
            return
        for f in files:
            analyze(f, model_filter)
            print()
    elif args:
        path = Path(args[0])
        if not path.exists():
            print(f"文件不存在: {path}")
            sys.exit(1)
        analyze(path, model_filter)
    else:
        default = Path("data/sim/equity_cursor.jsonl")
        if not default.exists():
            print(f"默认文件不存在: {default}")
            print("用法: python calc_fee.py [文件路径] [--model 模型名] 或 python calc_fee.py --all [--model 模型名]")
            sys.exit(1)
        analyze(default, model_filter)


if __name__ == "__main__":
    main()
