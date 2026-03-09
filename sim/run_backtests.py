#!/usr/bin/env python3
import bisect
import csv
import json
import math
import os
import statistics
from collections import defaultdict
from datetime import datetime, timezone, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.abspath(os.path.join(BASE_DIR, "..", "data", "long-short-ratio-5m.cleaned.jsonl"))
OUT_DIR = os.path.join(BASE_DIR, "results")
CHART_DIR = os.path.join(BASE_DIR, "charts")

HOUR_MS = 3600_000
KST = timezone(timedelta(hours=9))


def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(CHART_DIR, exist_ok=True)


def load_rows(path):
    rows = []  # (timestamp, lsr, price)
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue

            ts = d.get("timestamp")
            lsr = d.get("long_short_ratio")
            mid = d.get("mid")

            if not isinstance(ts, int):
                continue
            if not isinstance(lsr, (int, float)) or not math.isfinite(lsr):
                continue

            # fallback if mid is missing/NaN
            if not (isinstance(mid, (int, float)) and math.isfinite(mid) and mid > 0):
                ask = d.get("ask")
                bid = d.get("bid")
                if (
                    isinstance(ask, (int, float))
                    and isinstance(bid, (int, float))
                    and math.isfinite(ask)
                    and math.isfinite(bid)
                    and ask > 0
                    and bid > 0
                ):
                    mid = (ask + bid) / 2
                else:
                    mid = None

            if not (isinstance(mid, (int, float)) and math.isfinite(mid) and mid > 0):
                continue

            rows.append((ts, float(lsr), float(mid)))

    rows.sort(key=lambda x: x[0])
    return rows


def aggregate_hourly(rows):
    by_hour = defaultdict(list)
    for ts, lsr, price in rows:
        h = (ts // HOUR_MS) * HOUR_MS
        by_hour[h].append((lsr, price))

    hourly = []  # (hour_ts, lsr_avg, close_price)
    for h in sorted(by_hour.keys()):
        vals = by_hour[h]
        if len(vals) < 7:  # same spirit as existing code
            continue
        lsrs = [v[0] for v in vals]
        close_price = vals[-1][1]
        hourly.append((h, sum(lsrs) / len(lsrs), close_price))

    return hourly


def build_signals(hourly, threshold):
    signals = []
    for i in range(2, len(hourly)):
        first = hourly[i - 2][1]
        second = hourly[i - 1][1]
        third = hourly[i][1]

        a = second - first
        b = third - second

        if a * b < 0 and abs(a - b) >= threshold:
            signals.append(
                {
                    "idx": i,
                    "ts": hourly[i][0],
                    "direction": 1 if a > 0 else -1,  # long=1 short=-1
                    "a": a,
                    "b": b,
                    "diff": abs(a - b),
                }
            )
    return signals


def filter_alternating(signals):
    out = []
    prev_dir = None
    for s in signals:
        if prev_dir is None or s["direction"] != prev_dir:
            out.append(s)
            prev_dir = s["direction"]
    return out


def forward_return(hourly, idx, hold_hours):
    j = idx + hold_hours
    if j >= len(hourly):
        return None
    p0 = hourly[idx][2]
    p1 = hourly[j][2]
    if p0 <= 0 or p1 <= 0:
        return None
    return p1 / p0 - 1


def prev_momentum(hourly, idx, lookback_hours=24):
    j = idx - lookback_hours
    if j < 0:
        return None
    p0 = hourly[j][2]
    p1 = hourly[idx][2]
    if p0 <= 0 or p1 <= 0:
        return None
    return p1 / p0 - 1


def stats(values):
    if not values:
        return {
            "n": 0,
            "win_rate": 0,
            "avg": 0,
            "median": 0,
            "std": 0,
            "sum": 0,
        }
    n = len(values)
    win_rate = sum(1 for v in values if v > 0) / n
    avg = sum(values) / n
    med = statistics.median(values)
    sd = statistics.pstdev(values) if n > 1 else 0
    return {
        "n": n,
        "win_rate": win_rate,
        "avg": avg,
        "median": med,
        "std": sd,
        "sum": sum(values),
    }


def evaluate_fixed_horizon(hourly, signals, hold_hours, cost, trend_filter=None, include_years=None):
    values = []
    trades = []
    for s in signals:
        idx = s["idx"]

        if include_years is not None:
            y = datetime.fromtimestamp(s["ts"] / 1000, tz=timezone.utc).year
            if y not in include_years:
                continue

        if trend_filter is not None:
            m = prev_momentum(hourly, idx, 24)
            if m is None or abs(m) > trend_filter:
                continue

        r = forward_return(hourly, idx, hold_hours)
        if r is None:
            continue
        gross = s["direction"] * r
        net = gross - cost
        values.append(net)
        trades.append(
            {
                "timestamp": s["ts"],
                "direction": "long" if s["direction"] == 1 else "short",
                "gross_return": gross,
                "net_return": net,
                "a": s["a"],
                "b": s["b"],
                "diff": s["diff"],
            }
        )
    return stats(values), trades


def evaluate_regime(hourly, signals, hold_hours, cost):
    buckets = {"down": [], "flat": [], "up": []}
    for s in signals:
        idx = s["idx"]
        mom24 = prev_momentum(hourly, idx, 24)
        if mom24 is None:
            continue
        if mom24 > 0.02:
            bucket = "up"
        elif mom24 < -0.02:
            bucket = "down"
        else:
            bucket = "flat"

        r = forward_return(hourly, idx, hold_hours)
        if r is None:
            continue
        net = s["direction"] * r - cost
        buckets[bucket].append(net)

    out = []
    for b in ["down", "flat", "up"]:
        st = stats(buckets[b])
        out.append(
            {
                "regime": b,
                "n": st["n"],
                "win_rate": st["win_rate"],
                "avg": st["avg"],
                "median": st["median"],
                "std": st["std"],
                "sum": st["sum"],
            }
        )
    return out


def evaluate_yearly(hourly, signals, hold_hours, cost):
    by_year = defaultdict(list)
    for s in signals:
        idx = s["idx"]
        r = forward_return(hourly, idx, hold_hours)
        if r is None:
            continue
        net = s["direction"] * r - cost
        year = datetime.fromtimestamp(hourly[idx][0] / 1000, tz=timezone.utc).year
        by_year[year].append(net)

    rows = []
    for year in sorted(by_year.keys()):
        st = stats(by_year[year])
        rows.append(
            {
                "year": year,
                "n": st["n"],
                "win_rate": st["win_rate"],
                "avg": st["avg"],
                "median": st["median"],
                "std": st["std"],
                "sum": st["sum"],
            }
        )
    return rows


def make_price_arrays_5m(rows):
    ts = [r[0] for r in rows]
    prices = [r[2] for r in rows]
    return ts, prices


def evaluate_tp_sl_sweep(rows_5m_ts, rows_5m_price, hourly, signals, tp_list, sl_list, cost=0.0008, max_hold_hours=48):
    # For each signal, we scan future 5m closes until barrier hit or timeout.
    max_ms = max_hold_hours * HOUR_MS

    # Pre-map signal -> path returns for fast replay
    signal_paths = []
    for s in signals:
        ts0 = s["ts"]
        direction = s["direction"]

        i0 = bisect.bisect_left(rows_5m_ts, ts0)
        if i0 >= len(rows_5m_ts):
            continue
        p0 = rows_5m_price[i0]
        end_ts = ts0 + max_ms

        path = []
        i = i0 + 1
        while i < len(rows_5m_ts) and rows_5m_ts[i] <= end_ts:
            rr = direction * (rows_5m_price[i] / p0 - 1)
            path.append(rr)
            i += 1
        if not path:
            continue
        signal_paths.append(path)

    results = []
    for tp in tp_list:
        for sl in sl_list:
            vals = []
            exit_type_cnt = {"tp": 0, "sl": 0, "timeout": 0}

            for path in signal_paths:
                out = None
                etype = "timeout"
                for rr in path:
                    if rr >= tp:
                        out = rr
                        etype = "tp"
                        break
                    if rr <= -sl:
                        out = rr
                        etype = "sl"
                        break
                if out is None:
                    out = path[-1]
                    etype = "timeout"

                net = out - cost
                vals.append(net)
                exit_type_cnt[etype] += 1

            st = stats(vals)
            results.append(
                {
                    "tp": tp,
                    "sl": sl,
                    "n": st["n"],
                    "win_rate": st["win_rate"],
                    "avg": st["avg"],
                    "median": st["median"],
                    "std": st["std"],
                    "sum": st["sum"],
                    "tp_hits": exit_type_cnt["tp"],
                    "sl_hits": exit_type_cnt["sl"],
                    "timeouts": exit_type_cnt["timeout"],
                }
            )

    return results


def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def pct(x):
    return f"{x * 100:.4f}%"


def kst_str(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone(KST).strftime("%Y-%m-%d %H:%M")


def write_plotly_html(path, title, data_js, layout_js):
    html = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{title}</title>
  <script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\"></script>
  <style>body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:24px;}}#chart{{width:100%;height:78vh;}}</style>
</head>
<body>
  <h2>{title}</h2>
  <div id=\"chart\"></div>
  <script>
    const data = {data_js};
    const layout = {layout_js};
    Plotly.newPlot('chart', data, layout, {{responsive:true}});
  </script>
</body>
</html>
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)


def main():
    ensure_dirs()

    print("[1/7] Loading rows...")
    rows = load_rows(DATA_PATH)
    print(f"rows={len(rows)}")

    print("[2/7] Aggregating hourly...")
    hourly = aggregate_hourly(rows)
    print(f"hourly={len(hourly)}")

    # Scenario sets
    thresholds = [0.05, 0.07, 0.09, 0.0988, 0.12, 0.14, 0.16, 0.18, 0.20, 0.22]
    holds = [1, 4, 24, 48]
    costs = [0.0004, 0.0006, 0.0008]
    trend_filters = [None, 0.02]  # None=all, 2%=flat-ish only

    print("[3/7] Running fixed horizon grid...")
    grid_rows = []

    baseline_threshold = 0.0988
    baseline_hold = 4
    baseline_cost = 0.0008

    baseline_signals_raw = build_signals(hourly, baseline_threshold)
    baseline_signals_alt = filter_alternating(baseline_signals_raw)

    for th in thresholds:
        raw = build_signals(hourly, th)
        alt = filter_alternating(raw)

        for mode_name, sigs in [("raw", raw), ("alternating", alt)]:
            for h in holds:
                for c in costs:
                    for tf in trend_filters:
                        st, _ = evaluate_fixed_horizon(hourly, sigs, h, c, tf)
                        grid_rows.append(
                            {
                                "threshold": th,
                                "mode": mode_name,
                                "hold_hours": h,
                                "cost": c,
                                "trend_filter_abs_24h": "none" if tf is None else tf,
                                "n": st["n"],
                                "win_rate": st["win_rate"],
                                "avg": st["avg"],
                                "median": st["median"],
                                "std": st["std"],
                                "sum": st["sum"],
                            }
                        )

    write_csv(
        os.path.join(OUT_DIR, "grid_results.csv"),
        grid_rows,
        [
            "threshold",
            "mode",
            "hold_hours",
            "cost",
            "trend_filter_abs_24h",
            "n",
            "win_rate",
            "avg",
            "median",
            "std",
            "sum",
        ],
    )

    print("[4/7] Baseline trade logs / yearly / regime...")
    baseline_st_raw_4h, baseline_trades_raw_4h = evaluate_fixed_horizon(
        hourly, baseline_signals_raw, baseline_hold, baseline_cost, trend_filter=None
    )
    baseline_st_raw_48h, baseline_trades_raw_48h = evaluate_fixed_horizon(
        hourly, baseline_signals_raw, 48, baseline_cost, trend_filter=None
    )
    baseline_st_flat_4h, baseline_trades_flat_4h = evaluate_fixed_horizon(
        hourly, baseline_signals_raw, baseline_hold, baseline_cost, trend_filter=0.02
    )

    # Save trade ledger (baseline 4h raw)
    ledger_rows = []
    cumsum = 0.0
    for t in baseline_trades_raw_4h:
        cumsum += t["net_return"]
        ledger_rows.append(
            {
                "timestamp": t["timestamp"],
                "timestamp_kst": kst_str(t["timestamp"]),
                "direction": t["direction"],
                "gross_return": t["gross_return"],
                "net_return": t["net_return"],
                "cum_net_return": cumsum,
                "a": t["a"],
                "b": t["b"],
                "diff": t["diff"],
            }
        )
    write_csv(
        os.path.join(OUT_DIR, "trades_baseline_raw_4h.csv"),
        ledger_rows,
        [
            "timestamp",
            "timestamp_kst",
            "direction",
            "gross_return",
            "net_return",
            "cum_net_return",
            "a",
            "b",
            "diff",
        ],
    )

    # Baseline summary
    summary_rows = [
        {
            "scenario": "baseline_raw_4h",
            "threshold": baseline_threshold,
            "hold_hours": 4,
            "cost": baseline_cost,
            "trend_filter": "none",
            "n": baseline_st_raw_4h["n"],
            "win_rate": baseline_st_raw_4h["win_rate"],
            "avg": baseline_st_raw_4h["avg"],
            "median": baseline_st_raw_4h["median"],
            "std": baseline_st_raw_4h["std"],
            "sum": baseline_st_raw_4h["sum"],
        },
        {
            "scenario": "baseline_raw_48h",
            "threshold": baseline_threshold,
            "hold_hours": 48,
            "cost": baseline_cost,
            "trend_filter": "none",
            "n": baseline_st_raw_48h["n"],
            "win_rate": baseline_st_raw_48h["win_rate"],
            "avg": baseline_st_raw_48h["avg"],
            "median": baseline_st_raw_48h["median"],
            "std": baseline_st_raw_48h["std"],
            "sum": baseline_st_raw_48h["sum"],
        },
        {
            "scenario": "baseline_flat_only_4h",
            "threshold": baseline_threshold,
            "hold_hours": 4,
            "cost": baseline_cost,
            "trend_filter": "abs(24h_mom)<=2%",
            "n": baseline_st_flat_4h["n"],
            "win_rate": baseline_st_flat_4h["win_rate"],
            "avg": baseline_st_flat_4h["avg"],
            "median": baseline_st_flat_4h["median"],
            "std": baseline_st_flat_4h["std"],
            "sum": baseline_st_flat_4h["sum"],
        },
    ]
    write_csv(
        os.path.join(OUT_DIR, "baseline_summary.csv"),
        summary_rows,
        [
            "scenario",
            "threshold",
            "hold_hours",
            "cost",
            "trend_filter",
            "n",
            "win_rate",
            "avg",
            "median",
            "std",
            "sum",
        ],
    )

    # Regime analysis (baseline)
    regime_rows = evaluate_regime(hourly, baseline_signals_raw, 4, baseline_cost)
    write_csv(
        os.path.join(OUT_DIR, "regime_results_baseline_4h.csv"),
        regime_rows,
        ["regime", "n", "win_rate", "avg", "median", "std", "sum"],
    )

    # Yearly analysis (baseline)
    yearly_4h = evaluate_yearly(hourly, baseline_signals_raw, 4, baseline_cost)
    yearly_48h = evaluate_yearly(hourly, baseline_signals_raw, 48, baseline_cost)
    yearly_rows = []
    by_year_map = {r["year"]: {"4h": r} for r in yearly_4h}
    for r in yearly_48h:
        by_year_map.setdefault(r["year"], {})["48h"] = r
    for y in sorted(by_year_map.keys()):
        r4 = by_year_map[y].get("4h")
        r48 = by_year_map[y].get("48h")
        yearly_rows.append(
            {
                "year": y,
                "n_4h": r4["n"] if r4 else 0,
                "avg_4h": r4["avg"] if r4 else 0,
                "win_rate_4h": r4["win_rate"] if r4 else 0,
                "n_48h": r48["n"] if r48 else 0,
                "avg_48h": r48["avg"] if r48 else 0,
                "win_rate_48h": r48["win_rate"] if r48 else 0,
            }
        )
    write_csv(
        os.path.join(OUT_DIR, "yearly_results_baseline.csv"),
        yearly_rows,
        ["year", "n_4h", "avg_4h", "win_rate_4h", "n_48h", "avg_48h", "win_rate_48h"],
    )

    # Walk-forward (train on past years, test on next year)
    all_signal_years = sorted(
        set(datetime.fromtimestamp(s["ts"] / 1000, tz=timezone.utc).year for s in baseline_signals_raw)
    )
    walk_rows = []

    for test_year in all_signal_years[1:]:
        train_years = set(y for y in all_signal_years if y < test_year)
        if not train_years:
            continue

        # pick best threshold on train for 4h and 48h independently
        best_train_4h = None
        best_train_48h = None

        for th in thresholds:
            sigs = build_signals(hourly, th)

            st_tr4, _ = evaluate_fixed_horizon(
                hourly, sigs, 4, baseline_cost, trend_filter=None, include_years=train_years
            )
            if st_tr4["n"] >= 100:
                cand = (th, st_tr4["avg"], st_tr4["n"], st_tr4["win_rate"])
                if best_train_4h is None or cand[1] > best_train_4h[1]:
                    best_train_4h = cand

            st_tr48, _ = evaluate_fixed_horizon(
                hourly, sigs, 48, baseline_cost, trend_filter=None, include_years=train_years
            )
            if st_tr48["n"] >= 100:
                cand48 = (th, st_tr48["avg"], st_tr48["n"], st_tr48["win_rate"])
                if best_train_48h is None or cand48[1] > best_train_48h[1]:
                    best_train_48h = cand48

        if best_train_4h is not None:
            th = best_train_4h[0]
            sigs = build_signals(hourly, th)
            st_te4, _ = evaluate_fixed_horizon(
                hourly, sigs, 4, baseline_cost, trend_filter=None, include_years={test_year}
            )
        else:
            st_te4 = {"n": 0, "avg": 0, "win_rate": 0}

        if best_train_48h is not None:
            th48 = best_train_48h[0]
            sigs48 = build_signals(hourly, th48)
            st_te48, _ = evaluate_fixed_horizon(
                hourly, sigs48, 48, baseline_cost, trend_filter=None, include_years={test_year}
            )
        else:
            st_te48 = {"n": 0, "avg": 0, "win_rate": 0}

        walk_rows.append(
            {
                "test_year": test_year,
                "train_years": ",".join(str(y) for y in sorted(train_years)),
                "best_th_train_4h": best_train_4h[0] if best_train_4h else "",
                "train_avg_4h": best_train_4h[1] if best_train_4h else "",
                "train_n_4h": best_train_4h[2] if best_train_4h else "",
                "test_n_4h": st_te4["n"],
                "test_avg_4h": st_te4["avg"],
                "test_win_4h": st_te4["win_rate"],
                "best_th_train_48h": best_train_48h[0] if best_train_48h else "",
                "train_avg_48h": best_train_48h[1] if best_train_48h else "",
                "train_n_48h": best_train_48h[2] if best_train_48h else "",
                "test_n_48h": st_te48["n"],
                "test_avg_48h": st_te48["avg"],
                "test_win_48h": st_te48["win_rate"],
            }
        )

    write_csv(
        os.path.join(OUT_DIR, "walkforward_results.csv"),
        walk_rows,
        [
            "test_year",
            "train_years",
            "best_th_train_4h",
            "train_avg_4h",
            "train_n_4h",
            "test_n_4h",
            "test_avg_4h",
            "test_win_4h",
            "best_th_train_48h",
            "train_avg_48h",
            "train_n_48h",
            "test_n_48h",
            "test_avg_48h",
            "test_win_48h",
        ],
    )

    print("[5/7] TP/SL sweep (5m path, max hold 48h)...")
    ts5, px5 = make_price_arrays_5m(rows)
    tp_list = [0.006, 0.008, 0.010, 0.015, 0.020, 0.030]
    sl_list = [0.006, 0.008, 0.010, 0.015, 0.020, 0.030]
    tpsl_rows = evaluate_tp_sl_sweep(ts5, px5, hourly, baseline_signals_raw, tp_list, sl_list, cost=baseline_cost, max_hold_hours=48)
    write_csv(
        os.path.join(OUT_DIR, "tp_sl_sweep_baseline.csv"),
        tpsl_rows,
        [
            "tp",
            "sl",
            "n",
            "win_rate",
            "avg",
            "median",
            "std",
            "sum",
            "tp_hits",
            "sl_hits",
            "timeouts",
        ],
    )

    print("[6/7] Building charts...")
    # 1) Heatmap: raw mode, cost=0.0008, trend=none
    heat_rows = [
        r
        for r in grid_rows
        if r["mode"] == "raw"
        and r["cost"] == 0.0008
        and r["trend_filter_abs_24h"] == "none"
        and r["hold_hours"] in [1, 4, 24, 48]
    ]
    ths = sorted(set(r["threshold"] for r in heat_rows))
    hhs = sorted(set(r["hold_hours"] for r in heat_rows))
    z = []
    for th in ths:
        row = []
        for hh in hhs:
            vals = [r["avg"] * 100 for r in heat_rows if r["threshold"] == th and r["hold_hours"] == hh]
            row.append(vals[0] if vals else None)
        z.append(row)

    heat_data = [
        {
            "type": "heatmap",
            "x": hhs,
            "y": ths,
            "z": z,
            "colorscale": "RdYlGn",
            "reversescale": False,
            "colorbar": {"title": "Avg Net % / trade"},
        }
    ]
    heat_layout = {
        "title": "LSR Backtest Heatmap (Raw, Cost=0.08%, No Trend Filter)",
        "xaxis": {"title": "Hold Hours"},
        "yaxis": {"title": "Threshold"},
    }
    write_plotly_html(
        os.path.join(CHART_DIR, "heatmap_raw_cost8bp.html"),
        "LSR Backtest Heatmap (Raw)",
        json.dumps(heat_data),
        json.dumps(heat_layout),
    )

    # 2) Equity curve baseline raw 4h
    x = [r["timestamp_kst"] for r in ledger_rows]
    y = [r["cum_net_return"] * 100 for r in ledger_rows]
    eq_data = [
        {
            "type": "scatter",
            "mode": "lines",
            "name": "Cumulative Net Return",
            "x": x,
            "y": y,
            "line": {"width": 1.6},
        }
    ]
    eq_layout = {
        "title": "Baseline Equity Curve (threshold=0.0988, hold=4h, cost=0.08%)",
        "xaxis": {"title": "Trade Time (KST)"},
        "yaxis": {"title": "Cumulative Return %"},
    }
    write_plotly_html(
        os.path.join(CHART_DIR, "equity_baseline_raw_4h.html"),
        "Baseline Equity Curve",
        json.dumps(eq_data),
        json.dumps(eq_layout),
    )

    # 3) Regime bar
    reg_x = [r["regime"] for r in regime_rows]
    reg_y = [r["avg"] * 100 for r in regime_rows]
    reg_n = [r["n"] for r in regime_rows]
    reg_data = [
        {
            "type": "bar",
            "x": reg_x,
            "y": reg_y,
            "text": [f"n={n}" for n in reg_n],
            "textposition": "auto",
        }
    ]
    reg_layout = {
        "title": "Baseline 4h Avg Net Return by 24h Regime (cost=0.08%)",
        "xaxis": {"title": "Regime"},
        "yaxis": {"title": "Avg Net % / trade"},
    }
    write_plotly_html(
        os.path.join(CHART_DIR, "regime_baseline_4h.html"),
        "Regime Analysis (Baseline 4h)",
        json.dumps(reg_data),
        json.dumps(reg_layout),
    )

    # 4) Yearly line (4h vs 48h)
    years = [r["year"] for r in yearly_rows]
    avg4 = [r["avg_4h"] * 100 for r in yearly_rows]
    avg48 = [r["avg_48h"] * 100 for r in yearly_rows]
    yr_data = [
        {"type": "scatter", "mode": "lines+markers", "name": "4h avg %", "x": years, "y": avg4},
        {"type": "scatter", "mode": "lines+markers", "name": "48h avg %", "x": years, "y": avg48},
    ]
    yr_layout = {
        "title": "Yearly Avg Net Return per Trade (Baseline)",
        "xaxis": {"title": "Year"},
        "yaxis": {"title": "Avg Net % / trade"},
    }
    write_plotly_html(
        os.path.join(CHART_DIR, "yearly_baseline.html"),
        "Yearly Baseline Performance",
        json.dumps(yr_data),
        json.dumps(yr_layout),
    )

    # 5) Walk-forward test chart
    wf_years = [r["test_year"] for r in walk_rows]
    wf_4h = [float(r["test_avg_4h"]) * 100 if r["test_avg_4h"] != "" else None for r in walk_rows]
    wf_48h = [float(r["test_avg_48h"]) * 100 if r["test_avg_48h"] != "" else None for r in walk_rows]
    wf_data = [
        {"type": "scatter", "mode": "lines+markers", "name": "Walk-forward test avg 4h %", "x": wf_years, "y": wf_4h},
        {"type": "scatter", "mode": "lines+markers", "name": "Walk-forward test avg 48h %", "x": wf_years, "y": wf_48h},
    ]
    wf_layout = {
        "title": "Walk-forward Test Performance by Year (threshold optimized on prior years)",
        "xaxis": {"title": "Test Year"},
        "yaxis": {"title": "Avg Net % / trade"},
    }
    write_plotly_html(
        os.path.join(CHART_DIR, "walkforward_yearly.html"),
        "Walk-forward Yearly Performance",
        json.dumps(wf_data),
        json.dumps(wf_layout),
    )

    # 6) TP/SL heatmap
    tp_vals = sorted(set(r["tp"] for r in tpsl_rows))
    sl_vals = sorted(set(r["sl"] for r in tpsl_rows))
    z2 = []
    for tp in tp_vals:
        row = []
        for sl in sl_vals:
            v = [r["avg"] * 100 for r in tpsl_rows if r["tp"] == tp and r["sl"] == sl]
            row.append(v[0] if v else None)
        z2.append(row)

    tpsl_data = [
        {
            "type": "heatmap",
            "x": sl_vals,
            "y": tp_vals,
            "z": z2,
            "colorscale": "RdYlGn",
            "colorbar": {"title": "Avg Net % / trade"},
        }
    ]
    tpsl_layout = {
        "title": "TP/SL Sweep Heatmap (Baseline Signals, 48h max hold, cost=0.08%)",
        "xaxis": {"title": "SL"},
        "yaxis": {"title": "TP"},
    }
    write_plotly_html(
        os.path.join(CHART_DIR, "tpsl_heatmap_baseline.html"),
        "TP/SL Sweep Heatmap",
        json.dumps(tpsl_data),
        json.dumps(tpsl_layout),
    )

    print("[7/7] Writing markdown report...")
    # Top scenarios
    valid = [r for r in grid_rows if r["n"] >= 100]
    top = sorted(valid, key=lambda x: x["avg"], reverse=True)[:15]

    best_tpsl = sorted([r for r in tpsl_rows if r["n"] >= 100], key=lambda x: x["avg"], reverse=True)[:10]

    report_path = os.path.join(BASE_DIR, "README.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("# BTC LSR Backtest Report\n\n")
        f.write(f"Generated at: {datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S %Z')}\n\n")
        f.write("## Dataset\n")
        f.write(f"- Source: `../data/long-short-ratio-5m.cleaned.jsonl`\n")
        f.write(f"- Parsed 5m rows: **{len(rows):,}**\n")
        f.write(f"- Hourly aggregated rows: **{len(hourly):,}**\n")
        f.write(f"- Baseline threshold: **{baseline_threshold}**\n")
        f.write(f"- Baseline cost (roundtrip): **{baseline_cost*100:.2f}%**\n\n")

        f.write("## Baseline Snapshots\n")
        f.write(f"- Raw 4h: n={baseline_st_raw_4h['n']}, win={pct(baseline_st_raw_4h['win_rate'])}, avg={pct(baseline_st_raw_4h['avg'])}\n")
        f.write(f"- Raw 48h: n={baseline_st_raw_48h['n']}, win={pct(baseline_st_raw_48h['win_rate'])}, avg={pct(baseline_st_raw_48h['avg'])}\n")
        f.write(f"- Flat-only 4h (|24h mom|<=2%): n={baseline_st_flat_4h['n']}, win={pct(baseline_st_flat_4h['win_rate'])}, avg={pct(baseline_st_flat_4h['avg'])}\n\n")

        wf4_vals = [float(r["test_avg_4h"]) for r in walk_rows if r["test_avg_4h"] != ""]
        wf48_vals = [float(r["test_avg_48h"]) for r in walk_rows if r["test_avg_48h"] != ""]
        if wf4_vals or wf48_vals:
            f.write("## Walk-forward Snapshot\n")
            if wf4_vals:
                f.write(f"- 4h test-year avg of averages: {pct(sum(wf4_vals)/len(wf4_vals))} over {len(wf4_vals)} test years\n")
            if wf48_vals:
                f.write(f"- 48h test-year avg of averages: {pct(sum(wf48_vals)/len(wf48_vals))} over {len(wf48_vals)} test years\n")
            f.write("\n")

        f.write("## Top Scenarios (n >= 100)\n")
        f.write("|rank|threshold|mode|hold|cost|trend_filter|n|win|avg|\n")
        f.write("|---:|---:|---|---:|---:|---|---:|---:|---:|\n")
        for i, r in enumerate(top, 1):
            f.write(
                f"|{i}|{r['threshold']:.4f}|{r['mode']}|{r['hold_hours']}|{r['cost']*100:.2f}%|{r['trend_filter_abs_24h']}|{r['n']}|{r['win_rate']*100:.2f}%|{r['avg']*100:.4f}%|\n"
            )
        f.write("\n")

        f.write("## Best TP/SL (Baseline signals, n >= 100)\n")
        f.write("|rank|TP|SL|n|win|avg|tp_hits|sl_hits|timeouts|\n")
        f.write("|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for i, r in enumerate(best_tpsl, 1):
            f.write(
                f"|{i}|{r['tp']*100:.2f}%|{r['sl']*100:.2f}%|{r['n']}|{r['win_rate']*100:.2f}%|{r['avg']*100:.4f}%|{r['tp_hits']}|{r['sl_hits']}|{r['timeouts']}|\n"
            )

        f.write("\n## Files\n")
        f.write("### CSV\n")
        f.write("- `results/baseline_summary.csv`\n")
        f.write("- `results/grid_results.csv`\n")
        f.write("- `results/regime_results_baseline_4h.csv`\n")
        f.write("- `results/yearly_results_baseline.csv`\n")
        f.write("- `results/walkforward_results.csv`\n")
        f.write("- `results/tp_sl_sweep_baseline.csv`\n")
        f.write("- `results/trades_baseline_raw_4h.csv`\n")
        f.write("\n### Charts (open in browser)\n")
        f.write("- `charts/heatmap_raw_cost8bp.html`\n")
        f.write("- `charts/equity_baseline_raw_4h.html`\n")
        f.write("- `charts/regime_baseline_4h.html`\n")
        f.write("- `charts/yearly_baseline.html`\n")
        f.write("- `charts/walkforward_yearly.html`\n")
        f.write("- `charts/tpsl_heatmap_baseline.html`\n")

    print("Done.")
    print(f"Report: {report_path}")


if __name__ == "__main__":
    main()
