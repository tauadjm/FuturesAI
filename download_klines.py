"""
download_klines.py — 历史 K 线下载工具

用法：
    python download_klines.py --symbol SHFE.rb --interval 5 --year 2024
    python download_klines.py --symbol DCE.m --interval 5 --start 2024-01-01 --end 2024-12-31
    python download_klines.py --symbol SHFE.rb --interval 5 --start 2024-06-01 --end 2024-06-30

下载完成后保存为 Parquet 文件：
    data/klines/{product}/{interval}m/{year}.parquet
    例：data/klines/SHFE.rb/5m/2024.parquet

依赖：tqsdk、pandas、pyarrow（或 fastparquet）
"""

import argparse
import re
import sys
import time
from datetime import datetime
from pathlib import Path


def _to_tq_symbol(symbol: str) -> str:
    """确保传给 TqSdk 的合约代码合法：
    - KQ.m@SHFE.rb   → 原样（已是主连格式）
    - SHFE.rb2506    → 原样（已是具体月份合约）
    - SHFE.rb        → KQ.m@SHFE.rb（仅交易所+品种，自动补主连前缀）
    - CZCE/CFFEX 品种代码必须大写（如 CZCE.sr → CZCE.SR）
    """
    # CZCE 和 CFFEX 的品种代码必须大写
    _UPPERCASE_EXCHANGES = ("CZCE.", "CFFEX.")
    normalized = symbol
    for prefix in _UPPERCASE_EXCHANGES:
        if symbol.upper().startswith(prefix):
            exchange = prefix.rstrip(".")
            rest = symbol[len(prefix):]
            normalized = f"{exchange}.{rest.upper()}"
            break

    if "@" in normalized or re.search(r'\d{4}$', normalized):
        return normalized
    # 形如 SHFE.rb / DCE.m / CZCE.SR，补主连前缀
    return f"KQ.m@{normalized}"


def _normalize_symbol(symbol: str) -> str:
    """KQ.m@SHFE.rb → SHFE.rb（用于存储路径）；先经过 _to_tq_symbol 规范化大小写"""
    tq = _to_tq_symbol(symbol)
    m = re.search(r'@([A-Z]+\.\w+)', tq)
    if m:
        return m.group(1)
    return tq


def download(symbol: str, interval_minutes: int, start_dt: datetime, end_dt: datetime,
             output_dir: Path, bars: int = 0) -> list[Path]:
    """
    下载指定合约和时间范围的 K 线数据，按年保存为 Parquet。
    返回已保存的文件路径列表。
    """
    import pandas as pd
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        print("警告：未安装 pyarrow，尝试用 fastparquet 替代（pip install pyarrow 可获更好兼容性）")

    product = _normalize_symbol(symbol)
    dur_sec = interval_minutes * 60

    # 拉取全量数据（一次请求，覆盖完整日期范围）
    df = _fetch_klines(symbol, dur_sec, start_dt, bars_override=bars)

    if df.empty:
        print("  ✗ 数据为空，请检查合约代码和时间范围")
        return []

    # 按日期范围过滤（纳秒 → datetime 比较）
    df["_dt"] = pd.to_datetime(df["datetime"].astype("int64"), unit="ns")
    start_ns = int(start_dt.timestamp() * 1e9)
    end_ns   = int(end_dt.timestamp()   * 1e9)
    df = df[(df["datetime"] >= start_ns) & (df["datetime"] <= end_ns)].copy()

    if df.empty:
        print(f"  ✗ 过滤后无数据（{start_dt.date()} → {end_dt.date()}）")
        return []

    df = df[["datetime", "open", "high", "low", "close", "volume"]].copy()
    df["datetime"] = df["datetime"].astype("int64")

    # 增加前端 Lightweight Charts 用的 Unix 秒时间戳（K线收盘时刻）
    df["time_unix"] = (
        (df["datetime"] // 1_000_000_000).astype("int64")  # 纳秒 → 秒（开盘）
        + dur_sec                                           # +周期秒数 = 收盘时刻
    )

    # 按年分组保存 Parquet
    interval_dir = output_dir / product / f"{interval_minutes}m"
    interval_dir.mkdir(parents=True, exist_ok=True)

    df["_year"] = pd.to_datetime(df["datetime"].astype("int64"), unit="ns").dt.year
    saved_files = []

    for year, year_df in df.groupby("_year"):
        year_df = year_df.drop(columns=["_year"]).reset_index(drop=True)
        out_path = interval_dir / f"{year}.parquet"

        # 如果目标文件已存在，合并去重（支持追加下载）
        if out_path.exists():
            old_df = pd.read_parquet(str(out_path))
            year_df = pd.concat([old_df, year_df], ignore_index=True)
            year_df = (year_df.drop_duplicates(subset=["datetime"])
                              .sort_values("datetime")
                              .reset_index(drop=True))

        year_df.to_parquet(str(out_path), index=False)
        print(f"  ✓ {year} 年：已保存 {len(year_df)} 根K线 → {out_path}")
        saved_files.append(out_path)

    return saved_files


def _fetch_klines(symbol: str, dur_sec: int, start_dt: datetime, bars_override: int = 0):
    """
    使用 TqSdk 同步 get_kline_serial 拉取 K 线数据。
    bars_override > 0 时直接使用该值作为 data_length，否则按日期范围自动估算。
    参考：Kronos/examples/download_futures_kline_tqsdk.py
    """
    import pandas as pd
    import config
    from tqsdk import TqApi, TqAuth

    if bars_override > 0:
        bars_needed = bars_override
    else:
        now = datetime.now()
        # 中国商品期货每日实际交易约 6 小时（日盘+夜盘），
        # 折算为 bar 数：6h × 60min / dur_min × 日历天数 × 1.2 缓冲
        dur_min       = dur_sec // 60
        calendar_days = max((now - start_dt).days + 1, 1)
        bars_per_day  = int(6 * 60 / dur_min)   # 每日约 6 小时 = 72 根（5分钟K线）
        bars_needed   = int(calendar_days * bars_per_day * 1.2) + 300
        bars_needed   = max(bars_needed, 500)

    print(f"  正在连接天勤，请求最近 {bars_needed} 根K线...")
    api = TqApi(auth=TqAuth(config.TQ_USERNAME, config.TQ_PASSWORD))
    try:
        tq_symbol = _to_tq_symbol(symbol)
        klines = api.get_kline_serial(tq_symbol, duration_seconds=dur_sec, data_length=bars_needed)

        # 等待数据加载（参照 Kronos 的停滞检测逻辑）
        target_valid      = max(bars_needed // 2, 1)
        max_rounds        = 300
        no_progress_rounds = 0
        max_no_progress   = 30
        last_valid        = -1

        for _ in range(max_rounds):
            api.wait_update(deadline=time.time() + 2.0)
            valid_count = int(klines["close"].notna().sum())

            print(f"\r  已加载 {valid_count} 根K线（目标 ≥{target_valid}）...", end="", flush=True)

            if valid_count >= target_valid:
                break

            if valid_count > last_valid:
                last_valid = valid_count
                no_progress_rounds = 0
            else:
                no_progress_rounds += 1

            if no_progress_rounds >= max_no_progress:
                print(f"\n  数据加载停滞（已加载 {valid_count} 根），继续处理...")
                break

        print()
        df = pd.DataFrame(klines).copy()
    finally:
        api.close()

    required = ["datetime", "open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"TqSdk 返回数据缺少列：{missing}")

    return df.dropna(subset=["close"]).reset_index(drop=True)


def list_available(data_dir: Path):
    """列出已下载的数据"""
    import pandas as pd

    klines_dir = data_dir / "klines"
    if not klines_dir.exists():
        print("尚无已下载数据。")
        return

    print(f"\n已下载 K 线数据（目录：{klines_dir}）：")
    print(f"{'合约':<15} {'周期':<8} {'文件':<30} {'数据量'}")
    print("-" * 70)
    for product_dir in sorted(klines_dir.iterdir()):
        if not product_dir.is_dir():
            continue
        for interval_dir in sorted(product_dir.iterdir()):
            if not interval_dir.is_dir():
                continue
            for parquet_file in sorted(interval_dir.glob("*.parquet")):
                try:
                    df = pd.read_parquet(str(parquet_file))
                    count = f"{len(df)} 根"
                except Exception:
                    count = "读取失败"
                print(f"{product_dir.name:<15} {interval_dir.name:<8} {parquet_file.name:<30} {count}")


def main():
    parser = argparse.ArgumentParser(
        description="天勤历史 K 线下载工具",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--symbol", type=str, help="合约代码，如 SHFE.rb 或 KQ.m@SHFE.rb")
    parser.add_argument("--interval", type=int, default=5, help="K线周期（分钟），默认 5")
    parser.add_argument("--year", type=int, help="下载整年数据（与 --start/--end 互斥）")
    parser.add_argument("--start", type=str, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end",   type=str, help="结束日期 YYYY-MM-DD（默认今天）")
    parser.add_argument("--bars", type=int, default=0, help="直接指定拉取根数（与 --year/--start 互斥，不过滤日期）")
    parser.add_argument("--list",  action="store_true", help="列出已下载数据")
    parser.add_argument("--data-dir", type=str, default="data", help="数据根目录，默认 data/")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)

    if args.list:
        list_available(data_dir)
        return

    if not args.symbol:
        parser.print_help()
        print("\n错误：请指定 --symbol")
        sys.exit(1)

    # 解析时间范围
    if args.bars:
        # --bars 模式：不限日期，直接拉最近 N 根
        start_dt = datetime(2000, 1, 1)
        end_dt   = datetime.now()
    elif args.year:
        start_dt = datetime(args.year, 1, 1)
        end_dt   = datetime(args.year, 12, 31, 23, 59, 59)
    elif args.start:
        start_dt = datetime.strptime(args.start, "%Y-%m-%d")
        if args.end:
            end_dt = datetime.strptime(args.end, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        else:
            end_dt = datetime.now()
    else:
        # 默认下载去年全年
        year = datetime.now().year - 1
        start_dt = datetime(year, 1, 1)
        end_dt   = datetime(year, 12, 31, 23, 59, 59)
        print(f"未指定时间范围，默认下载 {year} 年全年数据")

    output_dir = data_dir / "klines"
    if args.bars:
        print(f"\n[{_normalize_symbol(args.symbol)}] 下载最近 {args.bars} 根K线，周期 {args.interval} 分钟")
    else:
        print(f"\n[{_normalize_symbol(args.symbol)}] 下载 {start_dt.date()} → {end_dt.date()}，K线周期 {args.interval} 分钟")

    try:
        files = download(args.symbol, args.interval, start_dt, end_dt, output_dir, bars=args.bars)
    except Exception as e:
        print(f"\n  ✗ 下载失败：{e}")
        sys.exit(1)

    if files:
        print(f"\n✓ 下载完成，共 {len(files)} 个文件：")
        for f in files:
            print(f"  {f}")
    else:
        print("\n✗ 未下载到任何数据，请检查合约代码和时间范围")


if __name__ == "__main__":
    main()
