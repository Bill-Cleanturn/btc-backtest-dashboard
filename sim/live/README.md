# BTC Live Strategy Board (Auto)

Updated: 2026-03-09 23:07:16 UTC+09:00

- 손해 전략은 자동 폐기(avg<=0 또는 n<120)
- 현재는 수익 전략만 표시
- 모든 상위 전략은 거래 전체 내역 CSV + 차트 링크 제공

## 현재 1순위
- 반전진입 · alternating · th 0.160 · 24h · 추세완화필터
- 평균: **0.4747% / trade**
- 승률: **52.90%**
- 샘플: **138 trades**
- 초기금액 10,000 기준 최종금액: **16972.82** (총 69.73%)
- 변곡점 감지 횟수: **0**

## 파일
- `results/latest_summary.json`
- `results/run_history.jsonl`
- `results/best_strategy_trades_10000.csv`
- `results/trades/*.csv`
- `../pages/dashboard/data/top_strategies.json`
- `../pages/dashboard/data/turning_points.json`
- `../pages/dashboard/best_strategy_equity.html`
- `../pages/dashboard/top12_final_capital.html`
