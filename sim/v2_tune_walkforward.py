#!/usr/bin/env python3
import bisect
import csv
import itertools
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta

SIM_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SIM_DIR)
import run_backtests as rb  # noqa: E402
from v2_pipeline import build_filtered_signals  # noqa: E402

DATA_PATH = os.path.abspath(os.path.join(SIM_DIR, "..", "data", "long-short-ratio-5m.cleaned.jsonl"))
OUT_DIR = os.path.join(SIM_DIR, "v2", "results")
KST = timezone(timedelta(hours=9))

os.makedirs(OUT_DIR, exist_ok=True)


def eval_path(rows_ts, rows_px, signals, tp, sl, max_hold_h, cost):
    vals = []
    trades = []
    max_ms = max_hold_h * 3600_000

    for s in signals:
        ts0 = s["ts"]
        i0 = bisect.bisect_left(rows_ts, ts0)
        if i0 >= len(rows_ts):
            continue
        p0 = rows_px[i0]
        d = s["direction"]
        end_ts = ts0 + max_ms

        out = None
        i = i0 + 1
        while i < len(rows_ts) and rows_ts[i] <= end_ts:
            rr = d * (rows_px[i] / p0 - 1)
            if rr >= tp:
                out = rr
                break
            if rr <= -sl:
                out = rr
                break
            i += 1
        if out is None:
            j = min(i, len(rows_ts) - 1)
            while j > i0 and rows_ts[j] > end_ts:
                j -= 1
            if j <= i0:
                continue
            out = d * (rows_px[j] / p0 - 1)

        net = out - cost
        vals.append(net)
        y = datetime.fromtimestamp(ts0 / 1000, tz=timezone.utc).year
        trades.append((y, net))

    return vals, trades


def walkforward_score(trades):
    by = defaultdict(list)
    for y, v in trades:
        by[y].append(v)
    years = sorted(by)
    rows = []
    test_avgs = []
    weighted_sum = 0.0
    weighted_n = 0

    for i in range(1, len(years)):
        ty = years[i]
        train_years = years[:i]
        train_vals = [x for yy in train_years for x in by[yy]]
        test_vals = by[ty]

        st_tr = rb.stats(train_vals)
        st_te = rb.stats(test_vals)
        rows.append(
            {
                "test_year": ty,
                "train_n": st_tr["n"],
                "train_avg": st_tr["avg"],
                "test_n": st_te["n"],
                "test_avg": st_te["avg"],
                "test_win": st_te["win_rate"],
            }
        )
        if st_te["n"] >= 20:
            test_avgs.append(st_te["avg"])
            weighted_sum += st_te["avg"] * st_te["n"]
            weighted_n += st_te["n"]

    mean_test = sum(test_avgs) / len(test_avgs) if test_avgs else -999
    wmean_test = weighted_sum / weighted_n if weighted_n > 0 else -999
    return mean_test, wmean_test, rows


def main():
    print("[tune 1/4] load data")
    rows = rb.load_rows(DATA_PATH)
    hourly = rb.aggregate_hourly(rows)
    ts5 = [r[0] for r in rows]
    px5 = [r[2] for r in rows]

    thresholds = [0.085, 0.09, 0.095, 0.10]
    tps = [0.015, 0.02, 0.025]
    sls = [0.01, 0.012, 0.015]
    cooldowns = [4, 6, 8]
    max_hold_h = 24
    cost = 0.0008

    all_rows = []
    best = None
    best_wf_rows = None

    print("[tune 2/4] grid search")
    for th, tp, sl, cd in itertools.product(thresholds, tps, sls, cooldowns):
        cfg = {
            "threshold": th,
            "mode": "alternating",
            "trend_abs_max": 0.02,
            "vol24_min": 0.0008,
            "vol24_max": 0.025,
            "cooldown_hours": cd,
        }
        sigs = build_filtered_signals(hourly, cfg)
        vals, trades = eval_path(ts5, px5, sigs, tp, sl, max_hold_h, cost)
        st = rb.stats(vals)
        mean_test, wmean_test, wf_rows = walkforward_score(trades)

        row = {
            "threshold": th,
            "tp": tp,
            "sl": sl,
            "cooldown_h": cd,
            "n": st["n"],
            "win_rate": st["win_rate"],
            "avg": st["avg"],
            "wf_mean_test_avg": mean_test,
            "wf_weighted_test_avg": wmean_test,
        }
        all_rows.append(row)

        # selection priority: wf_weighted_test_avg, then avg, then n
        key = (wmean_test, st["avg"], st["n"])
        if best is None or key > best[0]:
            best = (key, row)
            best_wf_rows = wf_rows

    print("[tune 3/4] write files")
    all_rows = sorted(all_rows, key=lambda x: (x["wf_weighted_test_avg"], x["avg"], x["n"]), reverse=True)

    out_all = os.path.join(OUT_DIR, "v2a_tuning_grid.csv")
    with open(out_all, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["threshold", "tp", "sl", "cooldown_h", "n", "win_rate", "avg", "wf_mean_test_avg", "wf_weighted_test_avg"])
        w.writeheader()
        for r in all_rows:
            w.writerow(r)

    out_top = os.path.join(OUT_DIR, "v2a_tuning_top20.csv")
    with open(out_top, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["rank", "threshold", "tp", "sl", "cooldown_h", "n", "win_rate", "avg", "wf_mean_test_avg", "wf_weighted_test_avg"])
        w.writeheader()
        for i, r in enumerate(all_rows[:20], 1):
            x = dict(r)
            x["rank"] = i
            w.writerow(x)

    out_best_wf = os.path.join(OUT_DIR, "v2a_tuning_best_walkforward.csv")
    with open(out_best_wf, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["test_year", "train_n", "train_avg", "test_n", "test_avg", "test_win"])
        w.writeheader()
        for r in best_wf_rows:
            w.writerow(r)

    out_md = os.path.join(os.path.dirname(OUT_DIR), "V2A_TUNING.md")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("# V2A Tuning (walk-forward guided)\n\n")
        f.write(f"Generated at: {datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S %Z')}\n\n")
        f.write(f"Best params: threshold={best[1]['threshold']}, tp={best[1]['tp']}, sl={best[1]['sl']}, cooldown_h={best[1]['cooldown_h']}\n\n")
        f.write(f"- n={best[1]['n']}, win={best[1]['win_rate']*100:.2f}%, avg={best[1]['avg']*100:.4f}%\n")
        f.write(f"- wf_mean_test_avg={best[1]['wf_mean_test_avg']*100:.4f}%\n")
        f.write(f"- wf_weighted_test_avg={best[1]['wf_weighted_test_avg']*100:.4f}%\n")
        f.write("\nFiles:\n")
        f.write("- results/v2a_tuning_grid.csv\n")
        f.write("- results/v2a_tuning_top20.csv\n")
        f.write("- results/v2a_tuning_best_walkforward.csv\n")

    print("[tune 4/4] done")
    print("Best:", best[1])


if __name__ == "__main__":
    main()
