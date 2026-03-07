# BTC LSR Backtest Report

Generated at: 2026-03-04 23:13:34 UTC+09:00

## Dataset
- Source: `../data/long-short-ratio-5m.cleaned.jsonl`
- Parsed 5m rows: **468,964**
- Hourly aggregated rows: **39,493**
- Baseline threshold: **0.0988**
- Baseline cost (roundtrip): **0.08%**

## Baseline Snapshots
- Raw 4h: n=1386, win=46.6811%, avg=-0.0859%
- Raw 48h: n=1385, win=49.0975%, avg=-0.1035%
- Flat-only 4h (|24h mom|<=2%): n=663, win=46.3047%, avg=-0.0430%

## Walk-forward Snapshot
- 4h test-year avg of averages: -0.0242% over 5 test years
- 48h test-year avg of averages: -0.3851% over 5 test years

## Top Scenarios (n >= 100)
|rank|threshold|mode|hold|cost|trend_filter|n|win|avg|
|---:|---:|---|---:|---:|---|---:|---:|---:|
|1|0.2200|alternating|24|0.04%|none|137|53.28%|0.1524%|
|2|0.2200|alternating|24|0.06%|none|137|52.55%|0.1324%|
|3|0.2200|alternating|4|0.04%|none|137|50.36%|0.1252%|
|4|0.2200|alternating|24|0.08%|none|137|52.55%|0.1124%|
|5|0.2000|alternating|4|0.04%|none|174|51.72%|0.1104%|
|6|0.2200|alternating|4|0.06%|none|137|50.36%|0.1052%|
|7|0.1800|alternating|4|0.04%|none|213|52.11%|0.0997%|
|8|0.0700|alternating|48|0.04%|0.02|812|49.63%|0.0945%|
|9|0.2000|alternating|4|0.06%|none|174|51.15%|0.0904%|
|10|0.2200|alternating|4|0.08%|none|137|49.64%|0.0852%|
|11|0.1400|alternating|48|0.04%|0.02|189|48.68%|0.0850%|
|12|0.1800|alternating|4|0.06%|none|213|51.64%|0.0797%|
|13|0.2200|raw|4|0.04%|none|225|50.67%|0.0768%|
|14|0.0700|alternating|48|0.06%|0.02|812|49.26%|0.0745%|
|15|0.0900|alternating|4|0.04%|0.02|485|48.45%|0.0707%|

## Best TP/SL (Baseline signals, n >= 100)
|rank|TP|SL|n|win|avg|tp_hits|sl_hits|timeouts|
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
|1|3.00%|3.00%|1386|50.87%|-0.0020%|554|525|307|
|2|1.50%|3.00%|1386|63.56%|-0.0534%|857|414|115|
|3|3.00%|2.00%|1386|42.78%|-0.0556%|484|721|181|
|4|2.00%|3.00%|1386|57.50%|-0.0559%|737|467|182|
|5|3.00%|1.50%|1386|37.23%|-0.0579%|431|832|123|
|6|3.00%|1.00%|1386|29.87%|-0.0598%|354|955|77|
|7|1.50%|2.00%|1386|56.28%|-0.0623%|762|567|57|
|8|3.00%|0.60%|1386|21.79%|-0.0627%|260|1077|49|
|9|1.50%|1.50%|1386|50.58%|-0.0667%|689|668|29|
|10|1.50%|0.60%|1386|32.25%|-0.0713%|439|935|12|

## Files
### CSV
- `results/baseline_summary.csv`
- `results/grid_results.csv`
- `results/regime_results_baseline_4h.csv`
- `results/yearly_results_baseline.csv`
- `results/walkforward_results.csv`
- `results/tp_sl_sweep_baseline.csv`
- `results/trades_baseline_raw_4h.csv`

### Charts (open in browser)
- `charts/heatmap_raw_cost8bp.html`
- `charts/equity_baseline_raw_4h.html`
- `charts/regime_baseline_4h.html`
- `charts/yearly_baseline.html`
- `charts/walkforward_yearly.html`
- `charts/tpsl_heatmap_baseline.html`
