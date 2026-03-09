#!/usr/bin/env python3
import csv
import os
import sys

SIM_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SIM_DIR)
import run_backtests as rb  # noqa: E402
from v2_pipeline import build_filtered_signals  # noqa: E402
from v3_pipeline import eval_path  # noqa: E402

DATA_PATH = os.path.abspath(os.path.join(SIM_DIR, "..", "data", "long-short-ratio-5m.cleaned.jsonl"))
OUT_DIR = os.path.join(SIM_DIR, "v3", "results")
CHART_DIR = os.path.join(SIM_DIR, "v3", "charts")
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
    ts5 = [r[0] for r in rows]
    px5 = [r[2] for r in rows]

    sig_cfg = {
        "threshold": 0.10,
        "mode": "alternating",
        "trend_abs_max": 0.02,
        "vol24_min": 0.0008,
        "vol24_max": 0.025,
        "cooldown_hours": 8,
    }
    sigs = build_filtered_signals(hourly, sig_cfg)

    scenario_defs = [
        {
            "scenario": "V3C_like_fixed",
            "tp": 0.025,
            "sl": 0.012,
            "max_hold_hours": 24,
            "size_model": "fixed",
            "taker_roundtrip": 0.0008,
            "maker_roundtrip": 0.0003,
        },
        {
            "scenario": "V3D_like_volTarget",
            "tp": 0.025,
            "sl": 0.012,
            "max_hold_hours": 24,
            "size_model": "vol_target",
            "vol_target": 0.01,
            "min_size": 0.5,
            "max_size": 1.8,
            "taker_roundtrip": 0.0008,
            "maker_roundtrip": 0.0003,
        },
    ]

    maker_ratios = [0.5, 0.8]
    extra_slippages = [0.0, 0.0002, 0.0004]  # roundtrip extra

    out = []
    for sdef in scenario_defs:
        for mr in maker_ratios:
            for ex in extra_slippages:
                cfg = dict(sdef)
                cfg["maker_ratio"] = mr
                cfg["taker_roundtrip"] = sdef["taker_roundtrip"] + ex
                cfg["maker_roundtrip"] = sdef["maker_roundtrip"] + ex

                st, trades, vals, mdd, bt = eval_path(ts5, px5, hourly, sigs, cfg)
                out.append(
                    {
                        "scenario": sdef["scenario"],
                        "maker_ratio": mr,
                        "extra_slippage": ex,
                        "n": st["n"],
                        "win_rate": st["win_rate"],
                        "avg": st["avg"],
                        "mdd": mdd,
                        "prob_mean_gt_0": bt["prob_gt_0"],
                    }
                )

    out = sorted(out, key=lambda x: (x["scenario"], x["maker_ratio"], x["extra_slippage"]))
    write_csv(
        os.path.join(OUT_DIR, "v3_stress_cost_slippage.csv"),
        out,
        ["scenario", "maker_ratio", "extra_slippage", "n", "win_rate", "avg", "mdd", "prob_mean_gt_0"],
    )

    # line chart by extra slippage
    data = []
    grouped = {}
    for r in out:
        key = f"{r['scenario']}|maker{int(r['maker_ratio']*100)}"
        grouped.setdefault(key, []).append(r)

    for key, vals in grouped.items():
        vals = sorted(vals, key=lambda x: x["extra_slippage"])
        data.append(
            {
                "type": "scatter",
                "mode": "lines+markers",
                "name": key,
                "x": [v["extra_slippage"] * 100 for v in vals],
                "y": [v["avg"] * 100 for v in vals],
            }
        )

    layout = {
        "title": "V3 Stress: extra slippage impact",
        "xaxis": {"title": "Extra roundtrip slippage %"},
        "yaxis": {"title": "Avg Net % / trade"},
    }

    rb.write_plotly_html(
        os.path.join(CHART_DIR, "v3_stress_slippage.html"),
        "V3 Stress Slippage",
        __import__("json").dumps(data),
        __import__("json").dumps(layout),
    )


if __name__ == "__main__":
    main()
