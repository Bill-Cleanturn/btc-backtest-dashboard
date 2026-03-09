# LIVE GUARD FINAL (V3)

Generated at: 2026-03-05 23:30:04 UTC+09:00

대상 전략: `V3C_blend80_fixed`(안정형), `V3D_blend80_volTarget`(공격형)

## 운영 상태 머신
- **NORMAL**: full size
- **RISK_OFF**: 50% size
- **STOP**: 신규 진입 중지

### V3C_blend80_fixed
- RISK_OFF 진입: rolling20 <= -5.99% 또는 month <= -6.13%
- STOP 진입: rolling20 <= -8.94% 또는 month <= -7.25% 또는 연속손실 >= 10
- NORMAL 복귀: rolling20 > -3.00% 그리고 month > -3.07% (최소 24h 관찰)

### V3D_blend80_volTarget
- RISK_OFF 진입: rolling20 <= -10.74% 또는 month <= -9.45%
- STOP 진입: rolling20 <= -14.69% 또는 month <= -11.58% 또는 연속손실 >= 10
- NORMAL 복귀: rolling20 > -5.37% 그리고 month > -4.73% (최소 24h 관찰)

## 파일
- `results/v3_guard_final.csv`
