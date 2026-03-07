#!/usr/bin/env python3
import csv
import os
from datetime import datetime, timezone, timedelta

SIM_DIR = os.path.dirname(os.path.abspath(__file__))
V3_DIR = os.path.join(SIM_DIR, "v3")
RES = os.path.join(V3_DIR, "results")
CHART = os.path.join(V3_DIR, "charts")
KST = timezone(timedelta(hours=9))


def read_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def f_pct(x):
    return f"{float(x)*100:.4f}%"


def main():
    summary = read_csv(os.path.join(RES, "v3_summary.csv"))
    guards = read_csv(os.path.join(RES, "v3_guard_metrics.csv"))

    # sort by avg desc
    summary = sorted(summary, key=lambda r: float(r["avg"]), reverse=True)
    top = summary[0]

    guard_map = {g["scenario_id"]: g for g in guards}
    top_guard = guard_map.get(top["scenario_id"])

    # live profile yaml (conservative vs aggressive)
    profiles = os.path.join(V3_DIR, "live_profiles.yaml")
    with open(profiles, "w", encoding="utf-8") as f:
        f.write("# Generated from v3 backtests\n")
        f.write("profiles:\n")
        f.write("  conservative:\n")
        f.write("    strategy: V3C_blend80_fixed\n")
        f.write("    maker_ratio: 0.8\n")
        f.write("    sizing: fixed\n")
        f.write("    tp: 0.025\n")
        f.write("    sl: 0.012\n")
        f.write("    max_hold_hours: 24\n")
        f.write("    guard:\n")
        f.write("      max_losing_streak: 9\n")
        f.write("      pause_roll20_below: -0.060\n")
        f.write("      riskoff_month_below: -0.061\n")
        f.write("  aggressive:\n")
        f.write("    strategy: V3D_blend80_volTarget\n")
        f.write("    maker_ratio: 0.8\n")
        f.write("    sizing: vol_target\n")
        f.write("    vol_target: 0.01\n")
        f.write("    min_size: 0.5\n")
        f.write("    max_size: 1.8\n")
        f.write("    tp: 0.025\n")
        f.write("    sl: 0.012\n")
        f.write("    max_hold_hours: 24\n")
        f.write("    guard:\n")
        f.write("      max_losing_streak: 9\n")
        f.write("      pause_roll20_below: -0.107\n")
        f.write("      riskoff_month_below: -0.095\n")

    # status markdown
    status = os.path.join(V3_DIR, "STATUS.md")
    with open(status, "w", encoding="utf-8") as f:
        f.write("# V3 Status Snapshot\n\n")
        f.write(f"Generated at: {datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S %Z')}\n\n")
        f.write("## Top by avg\n")
        f.write(f"- Top scenario: **{top['scenario_id']}**\n")
        f.write(f"- avg: {f_pct(top['avg'])}, win: {f_pct(top['win_rate'])}, n: {top['n']}\n")
        f.write(f"- mdd: {f_pct(top['mdd'])}, P(mean>0): {float(top['prob_mean_gt_0']):.2f}\n\n")

        if top_guard:
            f.write("## Suggested live guard (top scenario)\n")
            f.write(f"- max losing streak: {top_guard['max_losing_streak']}\n")
            f.write(f"- pause if rolling20 <= {f_pct(top_guard['roll20_p05'])}\n")
            f.write(f"- risk-off if month <= {f_pct(top_guard['month_p05'])}\n\n")

        f.write("## Files\n")
        f.write("- `live_profiles.yaml`\n")
        f.write("- `results/v3_summary.csv`\n")
        f.write("- `results/v3_guard_metrics.csv`\n")

    # tiny html dashboard
    html = os.path.join(CHART, "v3_live_dashboard.html")
    with open(html, "w", encoding="utf-8") as f:
        f.write("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>")
        f.write("<title>V3 Live Dashboard</title><style>body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;max-width:900px;margin:24px auto;line-height:1.6;padding:0 12px}table{border-collapse:collapse;width:100%}th,td{border:1px solid #ddd;padding:8px;font-size:14px}th{background:#f6f8fa}</style></head><body>")
        f.write("<h1>V3 Live Dashboard</h1>")
        f.write(f"<p>Generated at: {datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S %Z')}</p>")
        f.write("<h3>Scenario ranking</h3><table><tr><th>Scenario</th><th>n</th><th>win</th><th>avg</th><th>mdd</th><th>P(mean&gt;0)</th></tr>")
        for r in summary:
            f.write(f"<tr><td>{r['scenario_id']}</td><td>{r['n']}</td><td>{f_pct(r['win_rate'])}</td><td>{f_pct(r['avg'])}</td><td>{f_pct(r['mdd'])}</td><td>{float(r['prob_mean_gt_0']):.2f}</td></tr>")
        f.write("</table></body></html>")


if __name__ == "__main__":
    main()
