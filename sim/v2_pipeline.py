#!/usr/bin/env python3
import bisect
import csv
import json
import os
import random
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta

SIM_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SIM_DIR)
import run_backtests as rb  # noqa: E402

DATA_PATH = os.path.abspath(os.path.join(SIM_DIR, "..", "data", "long-short-ratio-5m.cleaned.jsonl"))
OUT_ROOT = os.path.join(SIM_DIR, "v2")
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


def max_drawdown_compounded(returns):
    eq = 1.0
    peak = 1.0
    mdd = 0.0
    for r in returns:
        step = max(-0.9999, r)
        eq *= (1.0 + step)
        if eq > peak:
            peak = eq
        dd = (eq / peak) - 1.0
        if dd < mdd:
            mdd = dd
    return mdd


def bootstrap_ci(values, b=2000, seed=42):
    if not values:
        return {"ci05": 0.0, "ci95": 0.0, "prob_gt_0": 0.0}
    rng = random.Random(seed)
    n = len(values)
    means = []
    gt = 0
    for _ in range(b):
        sample = [values[rng.randrange(n)] for _ in range(n)]
        m = sum(sample) / n
        means.append(m)
        if m > 0:
            gt += 1
    means.sort()
    i05 = max(0, int(0.05 * b) - 1)
    i95 = min(b - 1, int(0.95 * b) - 1)
    return {"ci05": means[i05], "ci95": means[i95], "prob_gt_0": gt / b}


def hourly_returns(hourly):
    rets = [None]
    for i in range(1, len(hourly)):
        p0 = hourly[i - 1][2]
        p1 = hourly[i][2]
        rets.append(p1 / p0 - 1 if p0 > 0 else None)
    return rets


def prev_24h_vol(hourly_rets, idx):
    start = idx - 24
    if start < 1:
        return None
    vals = [r for r in hourly_rets[start:idx] if r is not None]
    if len(vals) < 12:
        return None
    return statistics.pstdev(vals)


def build_filtered_signals(hourly, cfg):
    sigs = rb.build_signals(hourly, cfg["threshold"])
    if cfg.get("mode") == "alternating":
        sigs = rb.filter_alternating(sigs)

    hrets = hourly_returns(hourly)

    out = []
    last_ts = None
    cooldown_ms = cfg.get("cooldown_hours", 0) * 3600_000

    for s in sigs:
        idx = s["idx"]
        ts = s["ts"]

        # trend filter by previous 24h momentum
        t_cut = cfg.get("trend_abs_max")
        if t_cut is not None:
            m24 = rb.prev_momentum(hourly, idx, 24)
            if m24 is None or abs(m24) > t_cut:
                continue

        # vol filter by previous 24h hourly return std
        vol_min = cfg.get("vol24_min")
        vol_max = cfg.get("vol24_max")
        if vol_min is not None or vol_max is not None:
            v = prev_24h_vol(hrets, idx)
            if v is None:
                continue
            if vol_min is not None and v < vol_min:
                continue
            if vol_max is not None and v > vol_max:
                continue

        # cooldown filter
        if last_ts is not None and cooldown_ms > 0 and ts - last_ts < cooldown_ms:
            continue

        out.append(s)
        last_ts = ts

    return out


def evaluate_path_based(rows_ts, rows_px, hourly, signals, cfg):
    tp = cfg.get("tp")
    sl = cfg.get("sl")
    max_hold_h = cfg.get("max_hold_hours", 24)
    cost = cfg.get("cost", 0.0008)
    max_ms = max_hold_h * 3600_000

    trades = []
    values = []

    for s in signals:
        ts0 = s["ts"]
        i0 = bisect.bisect_left(rows_ts, ts0)
        if i0 >= len(rows_ts):
            continue
        p0 = rows_px[i0]
        if p0 <= 0:
            continue

        direction = s["direction"]
        end_ts = ts0 + max_ms

        out_rr = None
        exit_type = "timeout"
        exit_ts = None

        i = i0 + 1
        while i < len(rows_ts) and rows_ts[i] <= end_ts:
            rr = direction * (rows_px[i] / p0 - 1)
            if tp is not None and rr >= tp:
                out_rr = rr
                exit_type = "tp"
                exit_ts = rows_ts[i]
                break
            if sl is not None and rr <= -sl:
                out_rr = rr
                exit_type = "sl"
                exit_ts = rows_ts[i]
                break
            i += 1

        if out_rr is None:
            j = min(i, len(rows_ts) - 1)
            while j > i0 and rows_ts[j] > end_ts:
                j -= 1
            if j <= i0:
                continue
            out_rr = direction * (rows_px[j] / p0 - 1)
            exit_ts = rows_ts[j]
            exit_type = "timeout"

        net = out_rr - cost
        values.append(net)
        trades.append(
            {
                "timestamp": ts0,
                "timestamp_kst": datetime.fromtimestamp(ts0 / 1000, tz=timezone.utc).astimezone(KST).strftime("%Y-%m-%d %H:%M"),
                "direction": "long" if direction == 1 else "short",
                "gross_return": out_rr,
                "net_return": net,
                "exit_type": exit_type,
                "exit_timestamp": exit_ts,
                "exit_timestamp_kst": datetime.fromtimestamp(exit_ts / 1000, tz=timezone.utc).astimezone(KST).strftime("%Y-%m-%d %H:%M"),
            }
        )

    st = rb.stats(values)
    mdd = max_drawdown_compounded(values)
    bt = bootstrap_ci(values, b=2500, seed=13)

    return st, trades, values, mdd, bt


def yearly_stats_from_trades(sid, trades):
    by = defaultdict(list)
    for t in trades:
        y = datetime.fromtimestamp(t["timestamp"] / 1000, tz=timezone.utc).year
        by[y].append(t["net_return"])

    out = []
    for y in sorted(by):
        st = rb.stats(by[y])
        out.append(
            {
                "scenario_id": sid,
                "year": y,
                "n": st["n"],
                "avg": st["avg"],
                "win_rate": st["win_rate"],
                "sum": st["sum"],
            }
        )
    return out


def walkforward_fixed_from_trades(sid, trades):
    by = defaultdict(list)
    for t in trades:
        y = datetime.fromtimestamp(t["timestamp"] / 1000, tz=timezone.utc).year
        by[y].append(t["net_return"])

    years = sorted(by.keys())
    out = []
    for i in range(1, len(years)):
        test_year = years[i]
        train_years = years[:i]
        train_vals = [x for yy in train_years for x in by[yy]]
        test_vals = by[test_year]
        st_tr = rb.stats(train_vals)
        st_te = rb.stats(test_vals)
        out.append(
            {
                "scenario_id": sid,
                "test_year": test_year,
                "train_years": ",".join(str(y) for y in train_years),
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

    print("[v2 1/6] load data")
    rows = rb.load_rows(DATA_PATH)
    hourly = rb.aggregate_hourly(rows)
    ts5 = [r[0] for r in rows]
    px5 = [r[2] for r in rows]

    # v2 candidate set
    scenarios = [
        {
            "id": "B0_baseline_raw4h_fixed",
            "desc": "baseline comparator (fixed 4h hold, no TP/SL)",
            "eval": "fixed",
            "threshold": 0.0988,
            "mode": "raw",
            "hold_hours": 4,
            "cost": 0.0008,
            "trend_abs_max": None,
            "vol24_min": None,
            "vol24_max": None,
            "cooldown_hours": 0,
        },
        {
            "id": "V2A_alt_th009_flat_vol_cool6_tp20_sl12_h24",
            "desc": "alt, th=0.09, flat(2%), vol<=2.5%, cooldown6h, TP2.0/SL1.2, max24h",
            "eval": "path",
            "threshold": 0.09,
            "mode": "alternating",
            "cost": 0.0008,
            "trend_abs_max": 0.02,
            "vol24_min": 0.0008,
            "vol24_max": 0.025,
            "cooldown_hours": 6,
            "tp": 0.02,
            "sl": 0.012,
            "max_hold_hours": 24,
        },
        {
            "id": "V2B_alt_th018_vol_cool8_tp15_sl10_h12",
            "desc": "alt, th=0.18, vol<=3.0%, cooldown8h, TP1.5/SL1.0, max12h",
            "eval": "path",
            "threshold": 0.18,
            "mode": "alternating",
            "cost": 0.0008,
            "trend_abs_max": None,
            "vol24_min": 0.0006,
            "vol24_max": 0.03,
            "cooldown_hours": 8,
            "tp": 0.015,
            "sl": 0.010,
            "max_hold_hours": 12,
        },
        {
            "id": "V2C_alt_th007_flat_cool24_tp30_sl20_h48",
            "desc": "alt, th=0.07, flat(2%), cooldown24h, TP3.0/SL2.0, max48h",
            "eval": "path",
            "threshold": 0.07,
            "mode": "alternating",
            "cost": 0.0008,
            "trend_abs_max": 0.02,
            "vol24_min": None,
            "vol24_max": None,
            "cooldown_hours": 24,
            "tp": 0.03,
            "sl": 0.02,
            "max_hold_hours": 48,
        },
        {
            "id": "V2D_raw_th014_flat_vol_tp25_sl15_h24",
            "desc": "raw, th=0.14, flat(2%), vol<=2.5%, TP2.5/SL1.5, max24h",
            "eval": "path",
            "threshold": 0.14,
            "mode": "raw",
            "cost": 0.0008,
            "trend_abs_max": 0.02,
            "vol24_min": 0.0008,
            "vol24_max": 0.025,
            "cooldown_hours": 6,
            "tp": 0.025,
            "sl": 0.015,
            "max_hold_hours": 24,
        },
        {
            "id": "V2E_alt_th010_flat_vol_cool8_tp25_sl12_h24",
            "desc": "alt, th=0.10, flat(2%), vol<=2.5%, cooldown8h, TP2.5/SL1.2, max24h (tuned)",
            "eval": "path",
            "threshold": 0.10,
            "mode": "alternating",
            "cost": 0.0008,
            "trend_abs_max": 0.02,
            "vol24_min": 0.0008,
            "vol24_max": 0.025,
            "cooldown_hours": 8,
            "tp": 0.025,
            "sl": 0.012,
            "max_hold_hours": 24,
        },
    ]

    print("[v2 2/6] evaluate")
    summary = []
    yearly_rows = []
    wf_rows = []
    trade_files = []

    for cfg in scenarios:
        sigs = build_filtered_signals(hourly, cfg)

        if cfg["eval"] == "fixed":
            st, trades = rb.evaluate_fixed_horizon(
                hourly,
                sigs,
                cfg["hold_hours"],
                cfg["cost"],
                trend_filter=None,
            )
            values = [t["net_return"] for t in trades]
            mdd = max_drawdown_compounded(values)
            bt = bootstrap_ci(values, b=2500, seed=13)

            # normalize trade schema
            norm = []
            for t in trades:
                ts = t["timestamp"]
                norm.append(
                    {
                        "timestamp": ts,
                        "timestamp_kst": datetime.fromtimestamp(ts / 1000, tz=timezone.utc).astimezone(KST).strftime("%Y-%m-%d %H:%M"),
                        "direction": t["direction"],
                        "gross_return": t["gross_return"],
                        "net_return": t["net_return"],
                        "exit_type": f"fixed_{cfg['hold_hours']}h",
                        "exit_timestamp": "",
                        "exit_timestamp_kst": "",
                    }
                )
            trades = norm
        else:
            st, trades, values, mdd, bt = evaluate_path_based(ts5, px5, hourly, sigs, cfg)

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
                "initial_capital": INITIAL_CAPITAL,
                "final_capital": final_capital,
                "total_return_pct": (final_capital / INITIAL_CAPITAL) - 1.0,
            }
        )

        # save trade log
        trade_path = os.path.join(OUT_DIR, f"trades_{cfg['id']}.csv")
        write_csv(
            trade_path,
            trades,
            [
                "trade_no",
                "timestamp",
                "timestamp_kst",
                "direction",
                "gross_return",
                "net_return",
                "pnl_amount",
                "capital_before",
                "capital_after",
                "exit_type",
                "exit_timestamp",
                "exit_timestamp_kst",
            ],
        )
        trade_files.append((cfg["id"], trade_path))

        yearly_rows.extend(yearly_stats_from_trades(cfg["id"], trades))
        wf_rows.extend(walkforward_fixed_from_trades(cfg["id"], trades))

    summary = sorted(summary, key=lambda x: x["avg"], reverse=True)

    write_csv(
        os.path.join(OUT_DIR, "v2_summary.csv"),
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
            "initial_capital",
            "final_capital",
            "total_return_pct",
        ],
    )
    write_csv(
        os.path.join(OUT_DIR, "v2_yearly.csv"),
        yearly_rows,
        ["scenario_id", "year", "n", "avg", "win_rate", "sum"],
    )
    write_csv(
        os.path.join(OUT_DIR, "v2_walkforward.csv"),
        wf_rows,
        ["scenario_id", "test_year", "train_years", "train_n", "train_avg", "test_n", "test_avg", "test_win_rate"],
    )

    print("[v2 3/6] charts")
    # chart 1 summary bar
    x = [r["scenario_id"] for r in summary]
    y = [r["avg"] * 100 for r in summary]
    txt = [f"n={r['n']} | P>0={r['prob_mean_gt_0']:.2f}" for r in summary]
    data1 = [{"type": "bar", "x": x, "y": y, "text": txt, "textposition": "auto"}]
    layout1 = {"title": "V2 Candidates - Avg Net % per trade", "xaxis": {"title": "Scenario"}, "yaxis": {"title": "Avg Net %"}}
    rb.write_plotly_html(
        os.path.join(CHART_DIR, "v2_avg_bar.html"),
        "V2 Avg Return",
        json.dumps(data1),
        json.dumps(layout1),
    )

    # chart 2 equity compare top3 + baseline
    ids = [r["scenario_id"] for r in summary]
    selected = []
    for sid in ids:
        if sid.startswith("B0_"):
            selected.append(sid)
    for sid in ids:
        if not sid.startswith("B0_") and len(selected) < 4:
            selected.append(sid)

    data2 = []
    for sid in selected:
        p = os.path.join(OUT_DIR, f"trades_{sid}.csv")
        vals = []
        with open(p, "r", encoding="utf-8") as f:
            rr = csv.DictReader(f)
            for row in rr:
                cap = float(row["capital_after"])
                vals.append(((cap / INITIAL_CAPITAL) - 1.0) * 100)
        data2.append({"type": "scatter", "mode": "lines", "name": sid, "x": list(range(1, len(vals) + 1)), "y": vals})

    layout2 = {"title": "V2 Equity Compare (Initial 10,000)", "xaxis": {"title": "Trade #"}, "yaxis": {"title": "Cumulative Return %"}}
    rb.write_plotly_html(
        os.path.join(CHART_DIR, "v2_equity_compare.html"),
        "V2 Equity Compare",
        json.dumps(data2),
        json.dumps(layout2),
    )

    # chart 3 yearly compare
    by_sid = defaultdict(list)
    for r in yearly_rows:
        by_sid[r["scenario_id"]].append(r)
    data3 = []
    for sid, vals in by_sid.items():
        vals = sorted(vals, key=lambda x: x["year"])
        data3.append({"type": "scatter", "mode": "lines+markers", "name": sid, "x": [v["year"] for v in vals], "y": [v["avg"] * 100 for v in vals]})
    layout3 = {"title": "V2 Yearly Average Return", "xaxis": {"title": "Year"}, "yaxis": {"title": "Avg Net % / trade"}}
    rb.write_plotly_html(
        os.path.join(CHART_DIR, "v2_yearly_compare.html"),
        "V2 Yearly Compare",
        json.dumps(data3),
        json.dumps(layout3),
    )

    # chart 4 walkforward compare
    wf_by = defaultdict(list)
    for r in wf_rows:
        wf_by[r["scenario_id"]].append(r)
    data4 = []
    for sid, vals in wf_by.items():
        vals = sorted(vals, key=lambda x: x["test_year"])
        data4.append({"type": "scatter", "mode": "lines+markers", "name": sid, "x": [v["test_year"] for v in vals], "y": [v["test_avg"] * 100 for v in vals]})
    layout4 = {"title": "V2 Walk-forward Test Average", "xaxis": {"title": "Test year"}, "yaxis": {"title": "Test avg %"}}
    rb.write_plotly_html(
        os.path.join(CHART_DIR, "v2_walkforward_compare.html"),
        "V2 Walkforward",
        json.dumps(data4),
        json.dumps(layout4),
    )

    # chart index
    with open(os.path.join(CHART_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write("""<!doctype html>
<html lang=\"ko\"><head><meta charset=\"utf-8\"/><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"/>
<title>BTC LSR V2 Charts</title>
<style>body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;margin:24px;line-height:1.6}</style></head>
<body>
<h1>BTC LSR V2 Charts</h1>
<ul>
<li><a href=\"v2_avg_bar.html\">V2 Avg Bar</a></li>
<li><a href=\"v2_equity_compare.html\">V2 Equity Compare</a></li>
<li><a href=\"v2_yearly_compare.html\">V2 Yearly Compare</a></li>
<li><a href=\"v2_walkforward_compare.html\">V2 Walk-forward Compare</a></li>
</ul>
</body></html>""")

    print("[v2 4/6] report")
    report = os.path.join(OUT_ROOT, "README.md")
    with open(report, "w", encoding="utf-8") as f:
        f.write("# BTC LSR V2 Candidate Report\n\n")
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
        f.write("- `results/v2_summary.csv`\n")
        f.write("- `results/v2_yearly.csv`\n")
        f.write("- `results/v2_walkforward.csv`\n")
        f.write("- `results/trades_<scenario>.csv`\n")
        f.write("\n### Charts\n")
        f.write("- `charts/index.html`\n")

    print("[v2 5/6] done")
    print(f"Report: {report}")
    print("[v2 6/6] complete")


if __name__ == "__main__":
    main()
