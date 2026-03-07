#!/usr/bin/env python3
import glob
import json
import os
from datetime import datetime, timezone, timedelta

import run_backtests as rb

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA = os.path.join(BASE, "data", "long-short-ratio-5m.cleaned.jsonl")
SIM_LIVE = os.path.join(BASE, "sim", "live")
SIM_LIVE_RESULTS = os.path.join(SIM_LIVE, "results")
SIM_LIVE_TRADES = os.path.join(SIM_LIVE_RESULTS, "trades")
PAGES_DASH = os.path.join(BASE, "pages", "dashboard")
PAGES_DATA = os.path.join(PAGES_DASH, "data")
PAGES_STRATEGIES = os.path.join(PAGES_DATA, "strategies")
KST = timezone(timedelta(hours=9))
INITIAL_CAPITAL = 10000.0


def ensure():
    os.makedirs(SIM_LIVE_RESULTS, exist_ok=True)
    os.makedirs(SIM_LIVE_TRADES, exist_ok=True)
    os.makedirs(PAGES_DATA, exist_ok=True)
    os.makedirs(PAGES_STRATEGIES, exist_ok=True)


def strategy_id(row):
    rv = "rev" if row["reverse"] else "orig"
    tf = "tf002" if row["trend_filter"] is not None else "nof"
    th = int(round(row["threshold"] * 1000))
    return f"{rv}_{row['mode']}_th{th}_h{row['hold_hours']}_{tf}"


def simulate_capital(trades, initial_capital=INITIAL_CAPITAL):
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


def evaluate(hourly, threshold, mode, hold_hours, trend_filter, reverse, cost=0.0008):
    sigs = rb.build_signals(hourly, threshold)
    if mode == "alternating":
        sigs = rb.filter_alternating(sigs)

    trades = []
    values = []

    for s in sigs:
        idx = s["idx"]
        if trend_filter is not None:
            m = rb.prev_momentum(hourly, idx, 24)
            if m is None or abs(m) > trend_filter:
                continue
        r = rb.forward_return(hourly, idx, hold_hours)
        if r is None:
            continue

        direction = s["direction"] * (-1 if reverse else 1)
        gross = direction * r
        net = gross - cost

        ts = s["ts"]
        trades.append(
            {
                "timestamp": ts,
                "timestamp_kst": datetime.fromtimestamp(ts / 1000, tz=timezone.utc).astimezone(KST).strftime("%Y-%m-%d %H:%M"),
                "direction": "long" if direction == 1 else "short",
                "gross_return": gross,
                "net_return": net,
            }
        )
        values.append(net)

    st = rb.stats(values)
    trades, final_capital = simulate_capital(trades, INITIAL_CAPITAL)

    return {
        "n": st["n"],
        "win_rate": st["win_rate"],
        "avg": st["avg"],
        "sum": st["sum"],
        "values": values,
        "final_capital": final_capital,
        "trades": trades,
    }


def label(row):
    dir_text = "반전진입" if row["reverse"] else "원전략"
    trend = "추세완화필터" if row["trend_filter"] is not None else "필터없음"
    return f"{dir_text} · {row['mode']} · th {row['threshold']:.3f} · {row['hold_hours']}h · {trend}"


def detect_turning_points(trades, window=20, confirm=3, dd_threshold=0.05):
    # turning point: rolling avg crosses + -> <=0, with meaningful drawdown
    if len(trades) < window + confirm + 2:
        return []

    rets = [t["net_return"] for t in trades]
    caps = [t["capital_after"] for t in trades]

    roll = [None] * len(trades)
    for i in range(window - 1, len(trades)):
        seg = rets[i - window + 1 : i + 1]
        roll[i] = sum(seg) / len(seg)

    peaks = []
    m = caps[0]
    for c in caps:
        if c > m:
            m = c
        peaks.append(m)

    out = []
    for i in range(window, len(trades) - confirm):
        prev = roll[i - 1]
        cur = roll[i]
        if prev is None or cur is None:
            continue
        if prev > 0 and cur <= 0:
            # confirm persistence
            ok = True
            for k in range(1, confirm + 1):
                if roll[i + k] is None or roll[i + k] > 0:
                    ok = False
                    break
            if not ok:
                continue

            dd = (caps[i] / peaks[i]) - 1.0 if peaks[i] > 0 else 0.0
            if dd > -abs(dd_threshold):
                continue

            out.append(
                {
                    "trade_no": trades[i]["trade_no"],
                    "timestamp": trades[i]["timestamp"],
                    "timestamp_kst": trades[i]["timestamp_kst"],
                    "rolling_avg": cur,
                    "drawdown": dd,
                    "capital_after": caps[i],
                    "reason": "rolling_avg_cross_down_and_drawdown",
                }
            )

    return out


def write_trades_csv(path, trades):
    with open(path, "w", encoding="utf-8") as f:
        f.write(
            "trade_no,timestamp,timestamp_kst,direction,gross_return,net_return,pnl_amount,capital_before,capital_after\n"
        )
        for t in trades:
            f.write(
                f"{t['trade_no']},{t['timestamp']},{t['timestamp_kst']},{t['direction']},{t['gross_return']},{t['net_return']},{t['pnl_amount']},{t['capital_before']},{t['capital_after']}\n"
            )


def write_strategy_json(path, row, turning_points, generated_at):
    payload = {
        "generated_at": generated_at,
        "strategy_id": row["strategy_id"],
        "name": label(row),
        "initial_capital": INITIAL_CAPITAL,
        "final_capital": round(row["final_capital"], 2),
        "total_return_pct": round(((row["final_capital"] / INITIAL_CAPITAL) - 1.0) * 100, 2),
        "avg_pct": round(row["avg"] * 100, 4),
        "win_rate_pct": round(row["win_rate"] * 100, 2),
        "trades_count": row["n"],
        "params": {
            "reverse": row["reverse"],
            "mode": row["mode"],
            "threshold": row["threshold"],
            "hold_hours": row["hold_hours"],
            "trend_filter": row["trend_filter"],
        },
        "turning_points": turning_points,
        "trades": row["trades"],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def run():
    ensure()
    rows = rb.load_rows(DATA)
    hourly = rb.aggregate_hourly(rows)

    thresholds = [0.07, 0.085, 0.09, 0.095, 0.10, 0.12, 0.14, 0.16, 0.18, 0.20]
    modes = ["raw", "alternating"]
    holds = [4, 24, 48]
    trend_filters = [None, 0.02]
    reverses = [False, True]

    all_rows = []
    for th in thresholds:
        for md in modes:
            for hh in holds:
                for tf in trend_filters:
                    for rv in reverses:
                        st = evaluate(hourly, th, md, hh, tf, rv, cost=0.0008)
                        row = {
                            "threshold": th,
                            "mode": md,
                            "hold_hours": hh,
                            "trend_filter": tf,
                            "reverse": rv,
                            "n": st["n"],
                            "win_rate": st["win_rate"],
                            "avg": st["avg"],
                            "sum": st["sum"],
                            "final_capital": st["final_capital"],
                            "trades": st["trades"],
                        }
                        row["strategy_id"] = strategy_id(row)
                        all_rows.append(row)

    # 폐기 규칙: avg <= 0 또는 샘플 너무 적은 전략은 보여주지 않음
    profitable = [r for r in all_rows if r["avg"] > 0 and r["n"] >= 120]
    profitable.sort(key=lambda x: (x["avg"], x["n"]), reverse=True)
    top = profitable[:12]

    now = datetime.now(tz=KST)

    # cleanup old per-strategy artifacts
    for fp in glob.glob(os.path.join(SIM_LIVE_TRADES, "*.csv")):
        try:
            os.remove(fp)
        except OSError:
            pass
    for fp in glob.glob(os.path.join(PAGES_STRATEGIES, "*.json")):
        try:
            os.remove(fp)
        except OSError:
            pass

    # save all trade details + strategy viewer JSON for current top strategies
    strategy_turning = {}
    for r in top:
        sid = r["strategy_id"]
        p = os.path.join(SIM_LIVE_TRADES, f"{sid}.csv")
        write_trades_csv(p, r["trades"])

        tps = detect_turning_points(r["trades"], window=20, confirm=3, dd_threshold=0.05)
        strategy_turning[sid] = tps
        write_strategy_json(
            os.path.join(PAGES_STRATEGIES, f"{sid}.json"),
            r,
            tps,
            now.isoformat(),
        )

    # turning points for best strategy
    turning_points = []
    if top:
        turning_points = strategy_turning.get(top[0]["strategy_id"], [])

    with open(os.path.join(PAGES_DATA, "turning_points.json"), "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at": now.isoformat(),
                "strategy_id": top[0]["strategy_id"] if top else None,
                "strategy_name": label(top[0]) if top else None,
                "count": len(turning_points),
                "items": turning_points,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    # save machine-readable (dashboard)
    out_json = {
        "generated_at": now.isoformat(),
        "note": "수익(avg>0) + 충분한 샘플(n>=120)만 노출",
        "top_strategies": [
            {
                "rank": i + 1,
                "strategy_id": r["strategy_id"],
                "name": label(r),
                "avg_pct": round(r["avg"] * 100, 4),
                "win_rate_pct": round(r["win_rate"] * 100, 2),
                "trades": r["n"],
                "reverse": r["reverse"],
                "mode": r["mode"],
                "threshold": r["threshold"],
                "hold_hours": r["hold_hours"],
                "trend_filter": r["trend_filter"],
                "initial_capital": INITIAL_CAPITAL,
                "final_capital": round(r["final_capital"], 2),
                "total_return_pct": round(((r["final_capital"] / INITIAL_CAPITAL) - 1.0) * 100, 2),
                "turning_points_count": len(strategy_turning.get(r["strategy_id"], [])),
                "viewer_url": f"./strategy_viewer.html?sid={r['strategy_id']}",
            }
            for i, r in enumerate(top)
        ],
    }

    with open(os.path.join(PAGES_DATA, "top_strategies.json"), "w", encoding="utf-8") as f:
        json.dump(out_json, f, ensure_ascii=False, indent=2)

    # best strategy trade file alias
    if top:
        best_trade_csv = os.path.join(SIM_LIVE_RESULTS, "best_strategy_trades_10000.csv")
        write_trades_csv(best_trade_csv, top[0]["trades"])

    # charts (best equity + top final capital)
    if top:
        best = top[0]
        x = [t["timestamp_kst"] for t in best["trades"]]
        y = [t["capital_after"] for t in best["trades"]]
        rb.write_plotly_html(
            os.path.join(PAGES_DASH, "best_strategy_equity.html"),
            "Best Strategy Equity (Initial 10,000)",
            json.dumps([
                {"type": "scatter", "mode": "lines", "name": label(best), "x": x, "y": y}
            ]),
            json.dumps(
                {
                    "title": "Best Strategy Equity Curve (Initial 10,000)",
                    "xaxis": {"title": "Trade Time (KST)"},
                    "yaxis": {"title": "Capital"},
                }
            ),
        )

    rb.write_plotly_html(
        os.path.join(PAGES_DASH, "top12_final_capital.html"),
        "Top12 Final Capital",
        json.dumps(
            [
                {
                    "type": "bar",
                    "x": [label(r) for r in top],
                    "y": [round(r["final_capital"], 2) for r in top],
                }
            ]
        ),
        json.dumps(
            {
                "title": "Top12 Final Capital (Initial 10,000)",
                "xaxis": {"title": "Strategy"},
                "yaxis": {"title": "Final Capital"},
            }
        ),
    )

    # concise summary for autopilot reports
    bestj = out_json["top_strategies"][0] if out_json["top_strategies"] else None
    summary = {
        "generated_at": now.isoformat(),
        "kept_count": len(top),
        "discarded_count": len(all_rows) - len(profitable),
        "best": bestj,
        "turning_points_count": len(turning_points),
        "last_turning_point": turning_points[-1] if turning_points else None,
    }
    with open(os.path.join(SIM_LIVE_RESULTS, "latest_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # append run history
    hist_path = os.path.join(SIM_LIVE_RESULTS, "run_history.jsonl")
    with open(hist_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(summary, ensure_ascii=False) + "\n")

    # human-friendly markdown
    md = os.path.join(SIM_LIVE, "README.md")
    with open(md, "w", encoding="utf-8") as f:
        f.write("# BTC Live Strategy Board (Auto)\n\n")
        f.write(f"Updated: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}\n\n")
        f.write("- 손해 전략은 자동 폐기(avg<=0 또는 n<120)\n")
        f.write("- 현재는 수익 전략만 표시\n")
        f.write("- 모든 상위 전략은 거래 전체 내역 CSV + 차트 링크 제공\n\n")
        if bestj:
            f.write("## 현재 1순위\n")
            f.write(f"- {bestj['name']}\n")
            f.write(f"- 평균: **{bestj['avg_pct']:.4f}% / trade**\n")
            f.write(f"- 승률: **{bestj['win_rate_pct']:.2f}%**\n")
            f.write(f"- 샘플: **{bestj['trades']} trades**\n")
            f.write(
                f"- 초기금액 10,000 기준 최종금액: **{bestj['final_capital']:.2f}** (총 {bestj['total_return_pct']:.2f}%)\n"
            )
            f.write(f"- 변곡점 감지 횟수: **{len(turning_points)}**\n\n")
        f.write("## 파일\n")
        f.write("- `results/latest_summary.json`\n")
        f.write("- `results/run_history.jsonl`\n")
        f.write("- `results/best_strategy_trades_10000.csv`\n")
        f.write("- `results/trades/*.csv`\n")
        f.write("- `../pages/dashboard/data/top_strategies.json`\n")
        f.write("- `../pages/dashboard/data/turning_points.json`\n")
        f.write("- `../pages/dashboard/best_strategy_equity.html`\n")
        f.write("- `../pages/dashboard/top12_final_capital.html`\n")


if __name__ == "__main__":
    run()
