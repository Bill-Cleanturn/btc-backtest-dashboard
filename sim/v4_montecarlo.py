#!/usr/bin/env python3
import csv
import os
import random
from statistics import mean

SIM_DIR = os.path.dirname(os.path.abspath(__file__))
V3_RESULTS = os.path.join(SIM_DIR, "v3", "results")
OUT_DIR = os.path.join(SIM_DIR, "v4", "results")
os.makedirs(OUT_DIR, exist_ok=True)


def load_returns(path):
    out = []
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            out.append(float(row["net_return"]))
    return out


def drawdown(eq_curve):
    peak = eq_curve[0]
    mdd = 0.0
    for v in eq_curve:
        if v > peak:
            peak = v
        dd = v / peak - 1.0
        if dd < mdd:
            mdd = dd
    return mdd


def run_mc(returns, years=1, trades_per_year=300, paths=5000, seed=42):
    rng = random.Random(seed)
    horizon = years * trades_per_year
    finals = []
    mdds = []
    ruin20 = 0
    ruin30 = 0
    neg = 0

    for _ in range(paths):
        eq = 1.0
        curve = [eq]
        for _ in range(horizon):
            r = returns[rng.randrange(len(returns))]
            eq *= (1.0 + max(-0.9999, r))
            curve.append(eq)
        final_ret = eq - 1.0
        finals.append(final_ret)
        mdd = drawdown(curve)
        mdds.append(mdd)
        if mdd <= -0.20:
            ruin20 += 1
        if mdd <= -0.30:
            ruin30 += 1
        if final_ret < 0:
            neg += 1

    finals_sorted = sorted(finals)
    mdd_sorted = sorted(mdds)
    return {
        "paths": paths,
        "years": years,
        "trades_per_year": trades_per_year,
        "exp_final": mean(finals),
        "p05_final": finals_sorted[int(0.05 * paths)],
        "p50_final": finals_sorted[int(0.50 * paths)],
        "p95_final": finals_sorted[int(0.95 * paths)],
        "p05_mdd": mdd_sorted[int(0.05 * paths)],
        "p50_mdd": mdd_sorted[int(0.50 * paths)],
        "p95_mdd": mdd_sorted[int(0.95 * paths)],
        "prob_mdd_le_20": ruin20 / paths,
        "prob_mdd_le_30": ruin30 / paths,
        "prob_final_negative": neg / paths,
    }


def main():
    candidates = {
        "V3C_blend80_fixed": os.path.join(V3_RESULTS, "trades_V3C_blend80_fixed.csv"),
        "V3D_blend80_volTarget": os.path.join(V3_RESULTS, "trades_V3D_blend80_volTarget.csv"),
    }

    rows = []
    for sid, p in candidates.items():
        if not os.path.exists(p):
            continue
        rets = load_returns(p)
        for years in [1, 2]:
            res = run_mc(rets, years=years, trades_per_year=300, paths=5000, seed=42 + years)
            res["scenario_id"] = sid
            rows.append(res)

    out = os.path.join(OUT_DIR, "v4_montecarlo.csv")
    fields = [
        "scenario_id",
        "years",
        "paths",
        "trades_per_year",
        "exp_final",
        "p05_final",
        "p50_final",
        "p95_final",
        "p05_mdd",
        "p50_mdd",
        "p95_mdd",
        "prob_mdd_le_20",
        "prob_mdd_le_30",
        "prob_final_negative",
    ]
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)


if __name__ == "__main__":
    main()
