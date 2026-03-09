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


def pick(row_map, sid):
    r = row_map[sid]
    roll20_p05 = float(r["roll20_p05"])
    month_p05 = float(r["month_p05"])
    roll20_worst = float(r["roll20_worst"])
    month_worst = float(r["month_worst"])
    streak = int(r["max_losing_streak"])

    # three-state guard thresholds
    riskoff_roll20 = round(roll20_p05, 4)
    riskoff_month = round(month_p05, 4)

    # stop line between p05 and historical worst (conservative)
    stop_roll20 = round((roll20_p05 + roll20_worst) / 2, 4)
    stop_month = round((month_p05 + month_worst) / 2, 4)

    resume_roll20 = round(riskoff_roll20 * 0.5, 4)
    resume_month = round(riskoff_month * 0.5, 4)

    return {
        "scenario_id": sid,
        "riskoff_roll20": riskoff_roll20,
        "riskoff_month": riskoff_month,
        "stop_roll20": stop_roll20,
        "stop_month": stop_month,
        "resume_roll20": resume_roll20,
        "resume_month": resume_month,
        "max_losing_streak": streak,
        "stop_losing_streak": streak + 1,
    }


def pct(x):
    return f"{x*100:.2f}%"


def main():
    guards = read_csv(os.path.join(RES, "v3_guard_metrics.csv"))
    summary = read_csv(os.path.join(RES, "v3_summary.csv"))

    gm = {r["scenario_id"]: r for r in guards}
    top_scenarios = ["V3C_blend80_fixed", "V3D_blend80_volTarget"]

    rows = [pick(gm, sid) for sid in top_scenarios if sid in gm]

    out_csv = os.path.join(RES, "v3_guard_final.csv")
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "scenario_id",
                "riskoff_roll20",
                "riskoff_month",
                "stop_roll20",
                "stop_month",
                "resume_roll20",
                "resume_month",
                "max_losing_streak",
                "stop_losing_streak",
            ],
        )
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # Markdown playbook
    md = os.path.join(V3_DIR, "LIVE_GUARD_FINAL.md")
    with open(md, "w", encoding="utf-8") as f:
        f.write("# LIVE GUARD FINAL (V3)\n\n")
        f.write(f"Generated at: {datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S %Z')}\n\n")
        f.write("대상 전략: `V3C_blend80_fixed`(안정형), `V3D_blend80_volTarget`(공격형)\n\n")
        f.write("## 운영 상태 머신\n")
        f.write("- **NORMAL**: full size\n")
        f.write("- **RISK_OFF**: 50% size\n")
        f.write("- **STOP**: 신규 진입 중지\n\n")

        for r in rows:
            f.write(f"### {r['scenario_id']}\n")
            f.write(f"- RISK_OFF 진입: rolling20 <= {pct(r['riskoff_roll20'])} 또는 month <= {pct(r['riskoff_month'])}\n")
            f.write(f"- STOP 진입: rolling20 <= {pct(r['stop_roll20'])} 또는 month <= {pct(r['stop_month'])} 또는 연속손실 >= {r['stop_losing_streak']}\n")
            f.write(f"- NORMAL 복귀: rolling20 > {pct(r['resume_roll20'])} 그리고 month > {pct(r['resume_month'])} (최소 24h 관찰)\n\n")

        f.write("## 파일\n")
        f.write("- `results/v3_guard_final.csv`\n")

    # Live dashboard refresh
    s_by_id = {r["scenario_id"]: r for r in summary}
    html = os.path.join(CHART, "v3_live_dashboard.html")
    with open(html, "w", encoding="utf-8") as f:
        f.write("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>")
        f.write("<title>V3 Live Dashboard</title><style>body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;max-width:960px;margin:24px auto;line-height:1.6;padding:0 12px}.card{border:1px solid #e1e4e8;border-radius:10px;padding:14px;margin:10px 0}table{border-collapse:collapse;width:100%}th,td{border:1px solid #ddd;padding:8px;font-size:14px}th{background:#f6f8fa}</style></head><body>")
        f.write("<h1>V3 Live Dashboard</h1>")
        f.write(f"<p>Updated: {datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S %Z')}</p>")

        f.write("<h2>Execution Summary</h2><table><tr><th>Scenario</th><th>n</th><th>win</th><th>avg</th><th>mdd</th><th>P(mean>0)</th></tr>")
        for sid in ["V3C_blend80_fixed", "V3D_blend80_volTarget", "V3A_taker_fixed"]:
            if sid not in s_by_id:
                continue
            r = s_by_id[sid]
            f.write(
                f"<tr><td>{sid}</td><td>{r['n']}</td><td>{float(r['win_rate'])*100:.2f}%</td><td>{float(r['avg'])*100:.4f}%</td><td>{float(r['mdd'])*100:.2f}%</td><td>{float(r['prob_mean_gt_0']):.2f}</td></tr>"
            )
        f.write("</table>")

        f.write("<h2>Live Guard Thresholds</h2>")
        for r in rows:
            f.write("<div class='card'>")
            f.write(f"<h3>{r['scenario_id']}</h3>")
            f.write(f"<p>RISK_OFF: roll20 ≤ {pct(r['riskoff_roll20'])} or month ≤ {pct(r['riskoff_month'])}</p>")
            f.write(f"<p>STOP: roll20 ≤ {pct(r['stop_roll20'])} or month ≤ {pct(r['stop_month'])} or losing streak ≥ {r['stop_losing_streak']}</p>")
            f.write(f"<p>RESUME: roll20 > {pct(r['resume_roll20'])} and month > {pct(r['resume_month'])} (24h confirm)</p>")
            f.write("</div>")

        f.write("<p><small>Details: LIVE_GUARD_FINAL.md / results/v3_guard_final.csv</small></p>")
        f.write("</body></html>")


if __name__ == "__main__":
    main()
