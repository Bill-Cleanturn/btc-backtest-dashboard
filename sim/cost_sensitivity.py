#!/usr/bin/env python3
import csv
import json
import os
import sys
from datetime import datetime, timezone, timedelta

SIM_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SIM_DIR)
import run_backtests as rb  # noqa: E402

DATA_PATH = os.path.abspath(os.path.join(SIM_DIR, "..", "data", "long-short-ratio-5m.cleaned.jsonl"))
OUT_DIR = os.path.join(SIM_DIR, "advanced", "results")
CHART_DIR = os.path.join(SIM_DIR, "advanced", "charts")
KST = timezone(timedelta(hours=9))

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(CHART_DIR, exist_ok=True)


def write_csv(path, rows, fields):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main():
    rows = rb.load_rows(DATA_PATH)
    hourly = rb.aggregate_hourly(rows)

    scenarios = [
        ("baseline_raw_4h", 0.0988, "raw", 4, None),
        ("alt_flat_4h_th009", 0.09, "alternating", 4, 0.02),
        ("alt_4h_th018", 0.18, "alternating", 4, None),
        ("alt_flat_48h_th007", 0.07, "alternating", 48, 0.02),
        ("raw_24h_th014", 0.14, "raw", 24, None),
    ]

    costs = [0.0002, 0.0004, 0.0006, 0.0008, 0.0010, 0.0012]
    out = []

    for sid, th, mode, hold, tf in scenarios:
        sigs = rb.build_signals(hourly, th)
        if mode == "alternating":
            sigs = rb.filter_alternating(sigs)
        for c in costs:
            st, _ = rb.evaluate_fixed_horizon(hourly, sigs, hold, c, tf)
            out.append(
                {
                    "scenario": sid,
                    "threshold": th,
                    "mode": mode,
                    "hold_hours": hold,
                    "trend_filter": "none" if tf is None else tf,
                    "cost": c,
                    "n": st["n"],
                    "win_rate": st["win_rate"],
                    "avg": st["avg"],
                }
            )

    write_csv(
        os.path.join(OUT_DIR, "scenario_cost_sensitivity.csv"),
        out,
        ["scenario", "threshold", "mode", "hold_hours", "trend_filter", "cost", "n", "win_rate", "avg"],
    )

    # chart
    by = {}
    for r in out:
        by.setdefault(r["scenario"], []).append(r)

    data = []
    for sid, vals in by.items():
        vals = sorted(vals, key=lambda x: x["cost"])
        data.append(
            {
                "type": "scatter",
                "mode": "lines+markers",
                "name": sid,
                "x": [v["cost"] * 100 for v in vals],
                "y": [v["avg"] * 100 for v in vals],
            }
        )

    layout = {
        "title": "Cost Sensitivity by Scenario",
        "xaxis": {"title": "Roundtrip Cost %"},
        "yaxis": {"title": "Avg Net % / trade"},
    }

    rb.write_plotly_html(
        os.path.join(CHART_DIR, "cost_sensitivity.html"),
        "Cost Sensitivity",
        json.dumps(data),
        json.dumps(layout),
    )

    # small markdown append/update file
    md = os.path.join(SIM_DIR, "advanced", "COST_SENSITIVITY.md")
    with open(md, "w", encoding="utf-8") as f:
        f.write("# Cost Sensitivity\n\n")
        f.write(f"Generated at: {datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S %Z')}\\n\\n")
        f.write("- CSV: `results/scenario_cost_sensitivity.csv`\\n")
        f.write("- Chart: `charts/cost_sensitivity.html`\\n")


if __name__ == "__main__":
    main()
