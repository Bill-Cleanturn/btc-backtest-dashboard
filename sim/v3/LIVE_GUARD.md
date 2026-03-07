# V3 Live Guard Draft

Generated at: 2026-03-05 23:30:04 UTC+09:00

## Suggested initial guard template
- Pause strategy when **20-trade rolling PnL** drops below scenario `roll20_p05`
- Risk-off mode when **monthly PnL** drops below scenario `month_p05`
- Emergency stop when **losing streak** exceeds `max_losing_streak` (historical max)

## Files
- `results/v3_guard_metrics.csv`
- `results/v3_guard_yearly.csv`
