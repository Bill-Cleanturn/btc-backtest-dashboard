#!/usr/bin/env python3
import csv
import json
import math
import os
import random
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta

SIM_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SIM_DIR)
import run_backtests as rb  # noqa: E402

BASE_DIR = SIM_DIR
DATA_PATH = os.path.abspath(os.path.join(BASE_DIR, "..", "data", "long-short-ratio-5m.cleaned.jsonl"))
ADV_DIR = os.path.join(BASE_DIR, "advanced")
OUT_DIR = os.path.join(ADV_DIR, "results")
CHART_DIR = os.path.join(ADV_DIR, "charts")
KST = timezone(timedelta(hours=9))


def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(CHART_DIR, exist_ok=True)


def to_pct(x):
    return x * 100.0


def max_drawdown(cum_values):
    if not cum_values:
        return 0.0
    peak = cum_values[0]
    mdd = 0.0
    for v in cum_values:
        if v > peak:
            peak = v
        dd = v - peak
        if dd < mdd:
            mdd = dd
    return mdd


def max_drawdown_compounded(returns):
    if not returns:
        return 0.0
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


def write_csv(path, rows, fields):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)


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


def scenario_signals(hourly, threshold, mode):
    sigs = rb.build_signals(hourly, threshold)
    if mode == "alternating":
        sigs = rb.filter_alternating(sigs)
    return sigs


def evaluate_scenario(hourly, scenario):
    sigs = scenario_signals(hourly, scenario["threshold"], scenario["mode"])
    st, trades = rb.evaluate_fixed_horizon(
        hourly,
        sigs,
        scenario["hold_hours"],
        scenario["cost"],
        scenario.get("trend_filter"),
    )

    # sort trades by timestamp for consistent equity curve
    trades = sorted(trades, key=lambda x: x["timestamp"])
    c = 0.0
    cum = []
    values = []
    for t in trades:
        c += t["net_return"]
        cum.append(c)
        values.append(t["net_return"])

    mdd = max_drawdown_compounded(values)
    boot = bootstrap_ci(values, b=2500, seed=7)

    return {
        "stats": st,
        "trades": trades,
        "cum": cum,
        "values": values,
        "max_drawdown": mdd,
        "bootstrap": boot,
    }


def yearly_for_scenario(hourly, scenario):
    sigs = scenario_signals(hourly, scenario["threshold"], scenario["mode"])
    out = []
    by_year = defaultdict(list)

    for s in sigs:
        idx = s["idx"]
        if scenario.get("trend_filter") is not None:
            m = rb.prev_momentum(hourly, idx, 24)
            if m is None or abs(m) > scenario["trend_filter"]:
                continue

        r = rb.forward_return(hourly, idx, scenario["hold_hours"])
        if r is None:
            continue
        net = s["direction"] * r - scenario["cost"]
        y = datetime.fromtimestamp(s["ts"] / 1000, tz=timezone.utc).year
        by_year[y].append(net)

    for y in sorted(by_year.keys()):
        st = rb.stats(by_year[y])
        out.append(
            {
                "scenario_id": scenario["id"],
                "year": y,
                "n": st["n"],
                "avg": st["avg"],
                "win_rate": st["win_rate"],
                "sum": st["sum"],
            }
        )
    return out


def walkforward_fixed(hourly, scenario):
    sigs = scenario_signals(hourly, scenario["threshold"], scenario["mode"])

    # collect trade points with year labels
    pts = []
    for s in sigs:
        idx = s["idx"]
        if scenario.get("trend_filter") is not None:
            m = rb.prev_momentum(hourly, idx, 24)
            if m is None or abs(m) > scenario["trend_filter"]:
                continue
        r = rb.forward_return(hourly, idx, scenario["hold_hours"])
        if r is None:
            continue
        net = s["direction"] * r - scenario["cost"]
        y = datetime.fromtimestamp(s["ts"] / 1000, tz=timezone.utc).year
        pts.append((y, net))

    by_year = defaultdict(list)
    for y, v in pts:
        by_year[y].append(v)

    years = sorted(by_year.keys())
    out = []
    for i in range(1, len(years)):
        test_year = years[i]
        train_years = years[:i]
        train_vals = [x for yy in train_years for x in by_year[yy]]
        test_vals = by_year[test_year]

        st_train = rb.stats(train_vals)
        st_test = rb.stats(test_vals)

        out.append(
            {
                "scenario_id": scenario["id"],
                "test_year": test_year,
                "train_years": ",".join(str(y) for y in train_years),
                "train_n": st_train["n"],
                "train_avg": st_train["avg"],
                "test_n": st_test["n"],
                "test_avg": st_test["avg"],
                "test_win_rate": st_test["win_rate"],
            }
        )
    return out


def main():
    ensure_dirs()

    print("[adv 1/6] load data")
    rows = rb.load_rows(DATA_PATH)
    hourly = rb.aggregate_hourly(rows)

    # Scenarios selected from previous sweep + baseline
    scenarios = [
        {
            "id": "S0_baseline_raw_4h",
            "desc": "Baseline raw threshold=0.0988 hold=4h cost=0.08%",
            "threshold": 0.0988,
            "mode": "raw",
            "hold_hours": 4,
            "cost": 0.0008,
            "trend_filter": None,
        },
        {
            "id": "S1_alt_flat_48h_th007",
            "desc": "Alternating + flat filter(|24h mom|<=2%) threshold=0.07 hold=48h cost=0.08%",
            "threshold": 0.07,
            "mode": "alternating",
            "hold_hours": 48,
            "cost": 0.0008,
            "trend_filter": 0.02,
        },
        {
            "id": "S2_alt_flat_24h_th007",
            "desc": "Alternating + flat filter threshold=0.07 hold=24h cost=0.08%",
            "threshold": 0.07,
            "mode": "alternating",
            "hold_hours": 24,
            "cost": 0.0008,
            "trend_filter": 0.02,
        },
        {
            "id": "S3_alt_flat_4h_th009",
            "desc": "Alternating + flat filter threshold=0.09 hold=4h cost=0.08%",
            "threshold": 0.09,
            "mode": "alternating",
            "hold_hours": 4,
            "cost": 0.0008,
            "trend_filter": 0.02,
        },
        {
            "id": "S4_raw_flat_48h_th00988",
            "desc": "Raw + flat filter threshold=0.0988 hold=48h cost=0.08%",
            "threshold": 0.0988,
            "mode": "raw",
            "hold_hours": 48,
            "cost": 0.0008,
            "trend_filter": 0.02,
        },
        {
            "id": "S5_raw_24h_th014",
            "desc": "Raw threshold=0.14 hold=24h cost=0.08%",
            "threshold": 0.14,
            "mode": "raw",
            "hold_hours": 24,
            "cost": 0.0008,
            "trend_filter": None,
        },
        {
            "id": "S6_alt_4h_th018",
            "desc": "Alternating threshold=0.18 hold=4h cost=0.08%",
            "threshold": 0.18,
            "mode": "alternating",
            "hold_hours": 4,
            "cost": 0.0008,
            "trend_filter": None,
        },
    ]

    print("[adv 2/6] evaluate scenarios")
    eval_map = {}
    summary_rows = []
    yearly_rows = []
    walk_rows = []
    bootstrap_rows = []

    for s in scenarios:
        ev = evaluate_scenario(hourly, s)
        eval_map[s["id"]] = ev

        st = ev["stats"]
        summary_rows.append(
            {
                "scenario_id": s["id"],
                "desc": s["desc"],
                "threshold": s["threshold"],
                "mode": s["mode"],
                "hold_hours": s["hold_hours"],
                "cost": s["cost"],
                "trend_filter": "none" if s.get("trend_filter") is None else s["trend_filter"],
                "n": st["n"],
                "win_rate": st["win_rate"],
                "avg": st["avg"],
                "median": st["median"],
                "std": st["std"],
                "sum": st["sum"],
                "max_drawdown": ev["max_drawdown"],
            }
        )

        boot = ev["bootstrap"]
        bootstrap_rows.append(
            {
                "scenario_id": s["id"],
                "n": st["n"],
                "avg": st["avg"],
                "ci05": boot["ci05"],
                "ci95": boot["ci95"],
                "prob_mean_gt_0": boot["prob_gt_0"],
            }
        )

        yearly_rows.extend(yearly_for_scenario(hourly, s))
        walk_rows.extend(walkforward_fixed(hourly, s))

    summary_rows = sorted(summary_rows, key=lambda x: x["avg"], reverse=True)

    write_csv(
        os.path.join(OUT_DIR, "scenario_summary.csv"),
        summary_rows,
        [
            "scenario_id",
            "desc",
            "threshold",
            "mode",
            "hold_hours",
            "cost",
            "trend_filter",
            "n",
            "win_rate",
            "avg",
            "median",
            "std",
            "sum",
            "max_drawdown",
        ],
    )
    write_csv(
        os.path.join(OUT_DIR, "scenario_yearly.csv"),
        yearly_rows,
        ["scenario_id", "year", "n", "avg", "win_rate", "sum"],
    )
    write_csv(
        os.path.join(OUT_DIR, "scenario_walkforward_fixed.csv"),
        walk_rows,
        ["scenario_id", "test_year", "train_years", "train_n", "train_avg", "test_n", "test_avg", "test_win_rate"],
    )
    write_csv(
        os.path.join(OUT_DIR, "scenario_bootstrap.csv"),
        bootstrap_rows,
        ["scenario_id", "n", "avg", "ci05", "ci95", "prob_mean_gt_0"],
    )

    # also save a compact per-trade equity CSV for each scenario
    for s in scenarios:
        sid = s["id"]
        ev = eval_map[sid]
        rows_out = []
        c = 0.0
        for i, t in enumerate(ev["trades"], 1):
            c += t["net_return"]
            rows_out.append(
                {
                    "trade_no": i,
                    "timestamp": t["timestamp"],
                    "timestamp_kst": datetime.fromtimestamp(t["timestamp"] / 1000, tz=timezone.utc).astimezone(KST).strftime("%Y-%m-%d %H:%M"),
                    "direction": t["direction"],
                    "net_return": t["net_return"],
                    "cum_net_return": c,
                }
            )
        write_csv(
            os.path.join(OUT_DIR, f"trades_{sid}.csv"),
            rows_out,
            ["trade_no", "timestamp", "timestamp_kst", "direction", "net_return", "cum_net_return"],
        )

    print("[adv 3/6] charts")
    # Chart 1: summary bar (avg %)
    x = [r["scenario_id"] for r in summary_rows]
    y = [to_pct(r["avg"]) for r in summary_rows]
    txt = [f"n={r['n']} | win={r['win_rate']*100:.1f}%" for r in summary_rows]
    data1 = [{"type": "bar", "x": x, "y": y, "text": txt, "textposition": "auto"}]
    layout1 = {
        "title": "Advanced Scenarios - Avg Net Return per Trade",
        "xaxis": {"title": "Scenario"},
        "yaxis": {"title": "Avg Net % / trade"},
    }
    rb.write_plotly_html(
        os.path.join(CHART_DIR, "scenario_avg_bar.html"),
        "Scenario Avg Return",
        json.dumps(data1),
        json.dumps(layout1),
    )

    # Chart 2: equity compare (top 4 by avg)
    top4 = summary_rows[:4]
    data2 = []
    for r in top4:
        sid = r["scenario_id"]
        ev = eval_map[sid]
        yv = [to_pct(v) for v in ev["cum"]]
        xv = list(range(1, len(yv) + 1))
        data2.append({"type": "scatter", "mode": "lines", "name": sid, "x": xv, "y": yv})
    layout2 = {
        "title": "Equity Curve Compare (Top 4 scenarios)",
        "xaxis": {"title": "Trade #"},
        "yaxis": {"title": "Cumulative Return %"},
    }
    rb.write_plotly_html(
        os.path.join(CHART_DIR, "equity_compare_top4.html"),
        "Equity Compare Top4",
        json.dumps(data2),
        json.dumps(layout2),
    )

    # Chart 3: yearly average line by scenario
    yearly_by_sid = defaultdict(list)
    for r in yearly_rows:
        yearly_by_sid[r["scenario_id"]].append(r)
    data3 = []
    for sid, vals in yearly_by_sid.items():
        vals = sorted(vals, key=lambda x: x["year"])
        data3.append(
            {
                "type": "scatter",
                "mode": "lines+markers",
                "name": sid,
                "x": [v["year"] for v in vals],
                "y": [to_pct(v["avg"]) for v in vals],
            }
        )
    layout3 = {
        "title": "Yearly Avg Net Return per Trade by Scenario",
        "xaxis": {"title": "Year"},
        "yaxis": {"title": "Avg Net % / trade"},
    }
    rb.write_plotly_html(
        os.path.join(CHART_DIR, "yearly_compare.html"),
        "Yearly Compare",
        json.dumps(data3),
        json.dumps(layout3),
    )

    # Chart 4: bootstrap CI plot
    b_rows = sorted(bootstrap_rows, key=lambda x: x["avg"], reverse=True)
    xb = [r["scenario_id"] for r in b_rows]
    yb = [to_pct(r["avg"]) for r in b_rows]
    err_plus = [to_pct(r["ci95"] - r["avg"]) for r in b_rows]
    err_minus = [to_pct(r["avg"] - r["ci05"]) for r in b_rows]
    prob_text = [f"P(mean>0)={r['prob_mean_gt_0']:.2f}" for r in b_rows]

    data4 = [
        {
            "type": "bar",
            "x": xb,
            "y": yb,
            "text": prob_text,
            "textposition": "auto",
            "error_y": {
                "type": "data",
                "symmetric": False,
                "array": err_plus,
                "arrayminus": err_minus,
            },
        }
    ]
    layout4 = {
        "title": "Bootstrap 90% CI of Mean Return (per trade)",
        "xaxis": {"title": "Scenario"},
        "yaxis": {"title": "Mean Net % / trade"},
    }
    rb.write_plotly_html(
        os.path.join(CHART_DIR, "bootstrap_ci.html"),
        "Bootstrap CI",
        json.dumps(data4),
        json.dumps(layout4),
    )

    # Chart 5: fixed walk-forward comparison
    wf_by_sid = defaultdict(list)
    for r in walk_rows:
        wf_by_sid[r["scenario_id"]].append(r)
    data5 = []
    for sid, vals in wf_by_sid.items():
        vals = sorted(vals, key=lambda x: x["test_year"])
        data5.append(
            {
                "type": "scatter",
                "mode": "lines+markers",
                "name": sid,
                "x": [v["test_year"] for v in vals],
                "y": [to_pct(v["test_avg"]) for v in vals],
            }
        )
    layout5 = {
        "title": "Walk-forward (fixed params) test-year average by scenario",
        "xaxis": {"title": "Test Year"},
        "yaxis": {"title": "Test Avg Net % / trade"},
    }
    rb.write_plotly_html(
        os.path.join(CHART_DIR, "walkforward_fixed_compare.html"),
        "Walkforward Fixed Compare",
        json.dumps(data5),
        json.dumps(layout5),
    )

    # Index page
    idx_path = os.path.join(CHART_DIR, "index.html")
    with open(idx_path, "w", encoding="utf-8") as f:
        f.write("""<!doctype html>
<html lang=\"ko\"><head><meta charset=\"utf-8\"/><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"/>
<title>BTC LSR Advanced Charts</title>
<style>body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;margin:24px;line-height:1.6}</style></head>
<body>
<h1>BTC LSR Advanced Charts</h1>
<ul>
<li><a href=\"scenario_avg_bar.html\">Scenario Avg Return Bar</a></li>
<li><a href=\"equity_compare_top4.html\">Equity Compare Top4</a></li>
<li><a href=\"yearly_compare.html\">Yearly Compare</a></li>
<li><a href=\"bootstrap_ci.html\">Bootstrap CI</a></li>
<li><a href=\"walkforward_fixed_compare.html\">Walk-forward Fixed Compare</a></li>
</ul>
</body></html>""")

    print("[adv 4/6] markdown report")
    report = os.path.join(ADV_DIR, "README.md")
    with open(report, "w", encoding="utf-8") as f:
        f.write("# BTC LSR Advanced Scenario Report\n\n")
        f.write(f"Generated at: {datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S %Z')}\n\n")
        f.write("## Scenario Summary\n")
        f.write("|rank|scenario|n|win|avg|mdd|\n")
        f.write("|---:|---|---:|---:|---:|---:|\n")
        for i, r in enumerate(summary_rows, 1):
            f.write(
                f"|{i}|{r['scenario_id']}|{r['n']}|{r['win_rate']*100:.2f}%|{r['avg']*100:.4f}%|{r['max_drawdown']*100:.2f}%|\n"
            )

        f.write("\n## Bootstrap Confidence (90%)\n")
        f.write("|scenario|mean|ci05|ci95|P(mean>0)|\n")
        f.write("|---|---:|---:|---:|---:|\n")
        for r in sorted(bootstrap_rows, key=lambda x: x["avg"], reverse=True):
            f.write(
                f"|{r['scenario_id']}|{r['avg']*100:.4f}%|{r['ci05']*100:.4f}%|{r['ci95']*100:.4f}%|{r['prob_mean_gt_0']:.2f}|\n"
            )

        f.write("\n## Files\n")
        f.write("### Results\n")
        f.write("- `results/scenario_summary.csv`\n")
        f.write("- `results/scenario_yearly.csv`\n")
        f.write("- `results/scenario_walkforward_fixed.csv`\n")
        f.write("- `results/scenario_bootstrap.csv`\n")
        f.write("- `results/trades_<scenario>.csv`\n")
        f.write("\n### Charts\n")
        f.write("- `charts/index.html`\n")

    print("[adv 5/6] done")
    print(f"Report: {report}")
    print("[adv 6/6] complete")


if __name__ == "__main__":
    main()
