#!/usr/bin/env python3
import bisect
import csv
import json
import os
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta

SIM_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SIM_DIR)
import run_backtests as rb  # noqa: E402
from v2_pipeline import build_filtered_signals, bootstrap_ci, max_drawdown_compounded  # noqa: E402

DATA_PATH = os.path.abspath(os.path.join(SIM_DIR, "..", "data", "long-short-ratio-5m.cleaned.jsonl"))
OUT_ROOT = os.path.join(SIM_DIR, "v3")
OUT_DIR = os.path.join(OUT_ROOT, "results")
CHART_DIR = os.path.join(OUT_ROOT, "charts")
KST = timezone(timedelta(hours=9))
INITIAL_CAPITAL = 10000.0


def apply_capital_path(trades, initial_capital=INITIAL_CAPITAL):
    balance = float(initial_capital)
    out = []
    for i, t in enumerate(trades, 1):
        r = float(t["net_return"])
        before = balance
        pnl = before * r
        balance = before + pnl
        nt = dict(t)
        nt["trade_no"] = i
        nt["capital_before"] = before
        nt["pnl_amount"] = pnl
        nt["capital_after"] = balance
        out.append(nt)
    return out, balance


def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(CHART_DIR, exist_ok=True)


def write_csv(path, rows, fields):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def hourly_returns(hourly):
    out = [None]
    for i in range(1, len(hourly)):
        p0 = hourly[i - 1][2]
        p1 = hourly[i][2]
        out.append((p1 / p0 - 1) if p0 > 0 else None)
    return out


def prev_24h_vol(hrets, idx):
    if idx - 24 < 1:
        return None
    vals = [x for x in hrets[idx - 24 : idx] if x is not None]
    if len(vals) < 12:
        return None
    return statistics.pstdev(vals)


def blended_cost(cfg):
    # round-trip cost
    maker_rt = cfg.get("maker_roundtrip", 0.0003)
    taker_rt = cfg.get("taker_roundtrip", 0.0008)
    maker_ratio = cfg.get("maker_ratio", 0.0)
    return maker_rt * maker_ratio + taker_rt * (1.0 - maker_ratio)


def position_size(cfg, vol24):
    model = cfg.get("size_model", "fixed")
    if model == "fixed":
        return 1.0

    # vol-target sizing
    target = cfg.get("vol_target", 0.01)  # 1% hourly std target baseline
    min_size = cfg.get("min_size", 0.5)
    max_size = cfg.get("max_size", 1.8)
    if vol24 is None or vol24 <= 0:
        return 1.0
    raw = target / vol24
    if raw < min_size:
        return min_size
    if raw > max_size:
        return max_size
    return raw


def eval_path(rows_ts, rows_px, hourly, signals, cfg):
    hrets = hourly_returns(hourly)

    tp = cfg["tp"]
    sl = cfg["sl"]
    max_hold_h = cfg["max_hold_hours"]
    max_ms = max_hold_h * 3600_000
    rt_cost = blended_cost(cfg)

    trades = []
    values = []

    for s in signals:
        ts0 = s["ts"]
        idx_h = s["idx"]
        i0 = bisect.bisect_left(rows_ts, ts0)
        if i0 >= len(rows_ts):
            continue
        p0 = rows_px[i0]
        if p0 <= 0:
            continue

        d = s["direction"]
        end_ts = ts0 + max_ms

        out = None
        exit_type = "timeout"
        exit_ts = None

        i = i0 + 1
        while i < len(rows_ts) and rows_ts[i] <= end_ts:
            rr = d * (rows_px[i] / p0 - 1)
            if rr >= tp:
                out = rr
                exit_type = "tp"
                exit_ts = rows_ts[i]
                break
            if rr <= -sl:
                out = rr
                exit_type = "sl"
                exit_ts = rows_ts[i]
                break
            i += 1

        if out is None:
            j = min(i, len(rows_ts) - 1)
            while j > i0 and rows_ts[j] > end_ts:
                j -= 1
            if j <= i0:
                continue
            out = d * (rows_px[j] / p0 - 1)
            exit_type = "timeout"
            exit_ts = rows_ts[j]

        vol24 = prev_24h_vol(hrets, idx_h)
        size = position_size(cfg, vol24)

        net = size * out - size * rt_cost
        values.append(net)

        trades.append(
            {
                "timestamp": ts0,
                "timestamp_kst": datetime.fromtimestamp(ts0 / 1000, tz=timezone.utc).astimezone(KST).strftime("%Y-%m-%d %H:%M"),
                "direction": "long" if d == 1 else "short",
                "gross_return": out,
                "size": size,
                "cost": rt_cost,
                "net_return": net,
                "exit_type": exit_type,
                "exit_timestamp": exit_ts,
            }
        )

    st = rb.stats(values)
    mdd = max_drawdown_compounded(values)
    bt = bootstrap_ci(values, b=2500, seed=21)
    return st, trades, values, mdd, bt


def yearly_rows(sid, trades):
    by = defaultdict(list)
    for t in trades:
        y = datetime.fromtimestamp(t["timestamp"] / 1000, tz=timezone.utc).year
        by[y].append(t["net_return"])
    out = []
    for y in sorted(by):
        st = rb.stats(by[y])
        out.append({"scenario_id": sid, "year": y, "n": st["n"], "avg": st["avg"], "win_rate": st["win_rate"], "sum": st["sum"]})
    return out


def walkforward_rows(sid, trades):
    by = defaultdict(list)
    for t in trades:
        y = datetime.fromtimestamp(t["timestamp"] / 1000, tz=timezone.utc).year
        by[y].append(t["net_return"])
    years = sorted(by)
    out = []
    for i in range(1, len(years)):
        ty = years[i]
        tr_ys = years[:i]
        tr_vals = [x for yy in tr_ys for x in by[yy]]
        te_vals = by[ty]
        st_tr = rb.stats(tr_vals)
        st_te = rb.stats(te_vals)
        out.append(
            {
                "scenario_id": sid,
                "test_year": ty,
                "train_years": ",".join(str(x) for x in tr_ys),
                "train_n": st_tr["n"],
                "train_avg": st_tr["avg"],
                "test_n": st_te["n"],
                "test_avg": st_te["avg"],
                "test_win_rate": st_te["win_rate"],
            }
        )
    return out


def main():
    ensure_dirs()

    print("[v3 1/5] load")
    rows = rb.load_rows(DATA_PATH)
    hourly = rb.aggregate_hourly(rows)
    ts5 = [r[0] for r in rows]
    px5 = [r[2] for r in rows]

    # Base signal logic from tuned V2E
    base_signal_cfg = {
        "threshold": 0.10,
        "mode": "alternating",
        "trend_abs_max": 0.02,
        "vol24_min": 0.0008,
        "vol24_max": 0.025,
        "cooldown_hours": 8,
    }
    signals = build_filtered_signals(hourly, base_signal_cfg)

    scenarios = [
        {
            "id": "V3A_taker_fixed",
            "desc": "taker-only cost, fixed size",
            "tp": 0.025,
            "sl": 0.012,
            "max_hold_hours": 24,
            "maker_ratio": 0.0,
            "taker_roundtrip": 0.0008,
            "maker_roundtrip": 0.0003,
            "size_model": "fixed",
        },
        {
            "id": "V3B_blend50_fixed",
            "desc": "50% maker blended cost, fixed size",
            "tp": 0.025,
            "sl": 0.012,
            "max_hold_hours": 24,
            "maker_ratio": 0.5,
            "taker_roundtrip": 0.0008,
            "maker_roundtrip": 0.0003,
            "size_model": "fixed",
        },
        {
            "id": "V3C_blend80_fixed",
            "desc": "80% maker blended cost, fixed size",
            "tp": 0.025,
            "sl": 0.012,
            "max_hold_hours": 24,
            "maker_ratio": 0.8,
            "taker_roundtrip": 0.0008,
            "maker_roundtrip": 0.0003,
            "size_model": "fixed",
        },
        {
            "id": "V3D_blend80_volTarget",
            "desc": "80% maker blended cost + vol target sizing",
            "tp": 0.025,
            "sl": 0.012,
            "max_hold_hours": 24,
            "maker_ratio": 0.8,
            "taker_roundtrip": 0.0008,
            "maker_roundtrip": 0.0003,
            "size_model": "vol_target",
            "vol_target": 0.01,
            "min_size": 0.5,
            "max_size": 1.8,
        },
        {
            "id": "V3E_blend50_volTarget_tightSL",
            "desc": "50% maker blended cost + vol target sizing + tighter SL",
            "tp": 0.025,
            "sl": 0.010,
            "max_hold_hours": 24,
            "maker_ratio": 0.5,
            "taker_roundtrip": 0.0008,
            "maker_roundtrip": 0.0003,
            "size_model": "vol_target",
            "vol_target": 0.01,
            "min_size": 0.5,
            "max_size": 1.8,
        },
    ]

    print("[v3 2/5] evaluate")
    summary = []
    yearly = []
    wf = []

    for cfg in scenarios:
        st, trades, vals, mdd, bt = eval_path(ts5, px5, hourly, signals, cfg)
        trades, final_capital = apply_capital_path(trades, INITIAL_CAPITAL)

        summary.append(
            {
                "scenario_id": cfg["id"],
                "desc": cfg["desc"],
                "n": st["n"],
                "win_rate": st["win_rate"],
                "avg": st["avg"],
                "median": st["median"],
                "std": st["std"],
                "sum": st["sum"],
                "mdd": mdd,
                "ci05": bt["ci05"],
                "ci95": bt["ci95"],
                "prob_mean_gt_0": bt["prob_gt_0"],
                "maker_ratio": cfg["maker_ratio"],
                "size_model": cfg["size_model"],
                "initial_capital": INITIAL_CAPITAL,
                "final_capital": final_capital,
                "total_return_pct": (final_capital / INITIAL_CAPITAL) - 1.0,
            }
        )

        write_csv(
            os.path.join(OUT_DIR, f"trades_{cfg['id']}.csv"),
            trades,
            [
                "trade_no",
                "timestamp",
                "timestamp_kst",
                "direction",
                "gross_return",
                "size",
                "cost",
                "net_return",
                "pnl_amount",
                "capital_before",
                "capital_after",
                "exit_type",
                "exit_timestamp",
            ],
        )

        yearly.extend(yearly_rows(cfg["id"], trades))
        wf.extend(walkforward_rows(cfg["id"], trades))

    summary = sorted(summary, key=lambda x: x["avg"], reverse=True)

    write_csv(
        os.path.join(OUT_DIR, "v3_summary.csv"),
        summary,
        [
            "scenario_id",
            "desc",
            "n",
            "win_rate",
            "avg",
            "median",
            "std",
            "sum",
            "mdd",
            "ci05",
            "ci95",
            "prob_mean_gt_0",
            "maker_ratio",
            "size_model",
            "initial_capital",
            "final_capital",
            "total_return_pct",
        ],
    )
    write_csv(os.path.join(OUT_DIR, "v3_yearly.csv"), yearly, ["scenario_id", "year", "n", "avg", "win_rate", "sum"])
    write_csv(
        os.path.join(OUT_DIR, "v3_walkforward.csv"),
        wf,
        ["scenario_id", "test_year", "train_years", "train_n", "train_avg", "test_n", "test_avg", "test_win_rate"],
    )

    print("[v3 3/5] charts")
    data1 = [{"type": "bar", "x": [r["scenario_id"] for r in summary], "y": [r["avg"] * 100 for r in summary], "text": [f"n={r['n']} P>0={r['prob_mean_gt_0']:.2f}" for r in summary], "textposition": "auto"}]
    layout1 = {"title": "V3 Avg Net Return per Trade", "xaxis": {"title": "Scenario"}, "yaxis": {"title": "Avg Net %"}}
    rb.write_plotly_html(os.path.join(CHART_DIR, "v3_avg_bar.html"), "V3 Avg", json.dumps(data1), json.dumps(layout1))

    # equity compare (capital-based, initial=10,000)
    data2 = []
    for r in summary:
        sid = r["scenario_id"]
        p = os.path.join(OUT_DIR, f"trades_{sid}.csv")
        cum = []
        with open(p, "r", encoding="utf-8") as f:
            rr = csv.DictReader(f)
            for row in rr:
                cap = float(row["capital_after"])
                cum.append(((cap / INITIAL_CAPITAL) - 1.0) * 100)
        data2.append({"type": "scatter", "mode": "lines", "name": sid, "x": list(range(1, len(cum) + 1)), "y": cum})
    layout2 = {"title": "V3 Equity Compare (Initial 10,000)", "xaxis": {"title": "Trade #"}, "yaxis": {"title": "Cumulative %"}}
    rb.write_plotly_html(os.path.join(CHART_DIR, "v3_equity_compare.html"), "V3 Equity", json.dumps(data2), json.dumps(layout2))

    # index
    with open(os.path.join(CHART_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write("""<!doctype html>
<html lang=\"ko\"><head><meta charset=\"utf-8\"/><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"/>
<title>BTC LSR V3 Charts</title>
<style>body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;margin:24px;line-height:1.6}</style></head>
<body>
<h1>BTC LSR V3 Charts</h1>
<ul>
<li><a href=\"v3_avg_bar.html\">V3 Avg Bar</a></li>
<li><a href=\"v3_equity_compare.html\">V3 Equity Compare</a></li>
</ul>
</body></html>""")

    print("[v3 4/5] report")
    report = os.path.join(OUT_ROOT, "README.md")
    with open(report, "w", encoding="utf-8") as f:
        f.write("# BTC LSR V3 Execution Model Report\n\n")
        f.write(f"Generated at: {datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S %Z')}\n\n")
        f.write("## Summary\n")
        f.write("|rank|scenario|n|win|avg|final_capital(10,000기준)|mdd|ci05|ci95|P(mean>0)|\n")
        f.write("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for i, r in enumerate(summary, 1):
            f.write(
                f"|{i}|{r['scenario_id']}|{r['n']}|{r['win_rate']*100:.2f}%|{r['avg']*100:.4f}%|{r['final_capital']:.2f}|{r['mdd']*100:.2f}%|{r['ci05']*100:.4f}%|{r['ci95']*100:.4f}%|{r['prob_mean_gt_0']:.2f}|\n"
            )

        f.write("\n## Files\n")
        f.write("### Results\n")
        f.write("- `results/v3_summary.csv`\n")
        f.write("- `results/v3_yearly.csv`\n")
        f.write("- `results/v3_walkforward.csv`\n")
        f.write("- `results/trades_<scenario>.csv`\n")
        f.write("\n### Charts\n")
        f.write("- `charts/index.html`\n")

    print("[v3 5/5] done")
    print(f"Report: {report}")


if __name__ == "__main__":
    main()
