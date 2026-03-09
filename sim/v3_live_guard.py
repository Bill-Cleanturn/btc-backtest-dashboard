#!/usr/bin/env python3
import csv
import os
from collections import defaultdict
from datetime import datetime, timezone, timedelta

SIM_DIR = os.path.dirname(os.path.abspath(__file__))
V3_RESULTS = os.path.join(SIM_DIR, "v3", "results")
OUT_DIR = V3_RESULTS
KST = timezone(timedelta(hours=9))


def read_trades(path):
    out = []
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            ts = int(row["timestamp"])
            nr = float(row["net_return"])
            out.append((ts, nr))
    out.sort(key=lambda x: x[0])
    return out


def percentile(vals, p):
    if not vals:
        return 0.0
    vals = sorted(vals)
    idx = int((len(vals) - 1) * p)
    return vals[idx]


def max_losing_streak(returns):
    best = 0
    cur = 0
    for r in returns:
        if r < 0:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def yearly_stats(trades):
    by = defaultdict(list)
    for ts, r in trades:
        y = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).year
        by[y].append(r)
    rows = []
    for y in sorted(by):
        vals = by[y]
        avg = sum(vals) / len(vals)
        win = sum(1 for x in vals if x > 0) / len(vals)
        rows.append({"year": y, "n": len(vals), "avg": avg, "win_rate": win, "sum": sum(vals)})
    return rows


def guard_metrics(trades):
    rets = [r for _, r in trades]
    if not rets:
        return None

    # rolling 20-trade sum (proxy of short-term drawdown pressure)
    roll20 = []
    for i in range(len(rets)):
        s = max(0, i - 19)
        roll20.append(sum(rets[s : i + 1]))

    # monthly sum
    monthly = defaultdict(float)
    for ts, r in trades:
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).astimezone(KST)
        key = f"{dt.year}-{dt.month:02d}"
        monthly[key] += r

    mvals = list(monthly.values())

    return {
        "n": len(rets),
        "avg": sum(rets) / len(rets),
        "p05_trade": percentile(rets, 0.05),
        "p95_trade": percentile(rets, 0.95),
        "worst_trade": min(rets),
        "best_trade": max(rets),
        "max_losing_streak": max_losing_streak(rets),
        "roll20_p05": percentile(roll20, 0.05),
        "roll20_worst": min(roll20),
        "month_p05": percentile(mvals, 0.05) if mvals else 0.0,
        "month_worst": min(mvals) if mvals else 0.0,
    }


def write_csv(path, rows, fields):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main():
    scenarios = [
        "V3D_blend80_volTarget",
        "V3C_blend80_fixed",
        "V3A_taker_fixed",
    ]

    guard_rows = []
    y_rows = []

    for sid in scenarios:
        p = os.path.join(V3_RESULTS, f"trades_{sid}.csv")
        if not os.path.exists(p):
            continue
        tr = read_trades(p)
        g = guard_metrics(tr)
        if g:
            g["scenario_id"] = sid
            guard_rows.append(g)
        ys = yearly_stats(tr)
        for r in ys:
            r["scenario_id"] = sid
            y_rows.append(r)

    write_csv(
        os.path.join(OUT_DIR, "v3_guard_metrics.csv"),
        guard_rows,
        [
            "scenario_id",
            "n",
            "avg",
            "p05_trade",
            "p95_trade",
            "worst_trade",
            "best_trade",
            "max_losing_streak",
            "roll20_p05",
            "roll20_worst",
            "month_p05",
            "month_worst",
        ],
    )

    write_csv(
        os.path.join(OUT_DIR, "v3_guard_yearly.csv"),
        y_rows,
        ["scenario_id", "year", "n", "avg", "win_rate", "sum"],
    )

    md = os.path.join(SIM_DIR, "v3", "LIVE_GUARD.md")
    with open(md, "w", encoding="utf-8") as f:
        f.write("# V3 Live Guard Draft\n\n")
        f.write(f"Generated at: {datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S %Z')}\n\n")
        f.write("## Suggested initial guard template\n")
        f.write("- Pause strategy when **20-trade rolling PnL** drops below scenario `roll20_p05`\n")
        f.write("- Risk-off mode when **monthly PnL** drops below scenario `month_p05`\n")
        f.write("- Emergency stop when **losing streak** exceeds `max_losing_streak` (historical max)\n")
        f.write("\n## Files\n")
        f.write("- `results/v3_guard_metrics.csv`\n")
        f.write("- `results/v3_guard_yearly.csv`\n")


if __name__ == "__main__":
    main()
