# BTC LSR V4 Monte Carlo Snapshot

- File: `results/v4_montecarlo.csv`
- Method: bootstrap-style random resampling from historical per-trade returns
- Paths: 5,000 per scenario/year-horizon
- Horizons: 1y (300 trades), 2y (600 trades)

Quick read:
- `V3C_blend80_fixed`는 기대수익은 낮지만 MDD tail이 상대적으로 안정적
- `V3D_blend80_volTarget`는 기대수익이 더 높지만 대폭 더 큰 MDD tail 리스크를 보임
