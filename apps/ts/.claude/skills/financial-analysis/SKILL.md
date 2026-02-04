---
name: financial-analysis
description: Financial analysis algorithms including ROE, screening, and factor regression.
globs: "packages/shared/**/*.ts, packages/api/**/*.ts, packages/cli/**/*.ts"
alwaysApply: false
---

# Financial Analysis

## Fundamental Metrics

| Metric | Formula |
|--------|---------|
| ROE | Net profit / Shareholders' equity |
| ROA | Net profit / Total assets |
| EPS | Net profit / Shares outstanding |
| Diluted EPS | Net profit / Diluted shares outstanding |
| BPS | Shareholders' equity / Shares outstanding |
| PER | Stock price at disclosure / EPS |
| PBR | Stock price at disclosure / BPS |
| Operating Margin | Operating profit / Net sales |
| Net Margin | Net profit / Net sales |

```typescript
import { calculateMetrics } from '@trading25/shared/fa/metrics';

const metrics = calculateMetrics({
  netProfit: 2500000,
  equity: 15000000,
  totalAssets: 50000000,
  sharesOutstanding: 1000000000,
  stockPrice: 2500,
  netSales: 30000000,
  operatingProfit: 3000000,
});
```

## Screening Algorithms

### Range Break Fast
- **Period**: 100 days (long-term), 10 days (recent)
- **Condition**: `recentMaxHigh >= periodMaxHigh`
- **Volume**: 30-day EMA > 120-day EMA × 1.7

```typescript
const rangeBreakFastParams = {
  period: 100,
  lookbackDays: 10,
  volumeRatioThreshold: 1.7,
  volumeShortPeriod: 30,
  volumeLongPeriod: 120,
  volumeType: 'ema'
};
```

### Range Break Slow
- **Period**: 150 days (long-term), 10 days (recent)
- **Condition**: `recentMaxHigh >= periodMaxHigh`
- **Volume**: 50-day SMA > 150-day SMA × 1.7

```typescript
const rangeBreakSlowParams = {
  period: 150,
  lookbackDays: 10,
  volumeRatioThreshold: 1.7,
  volumeShortPeriod: 50,
  volumeLongPeriod: 150,
  volumeType: 'sma'
};
```

```typescript
import { detectRangeBreak } from '@trading25/shared/screening';

const result = detectRangeBreak(stockData, rangeBreakFastParams, 10);
if (result.found && result.details) {
  console.log(`Break on ${result.details.breakDate}`);
  console.log(`Break %: ${result.details.breakPercentage}`);
}
```

## Factor Regression (Two-Stage OLS)

### Stage 1: Market Regression
```
r_stock = α + βm × r_TOPIX + residual
```
Calculates market beta (βm) and R² (market exposure).

### Stage 2: Residual Factor Matching
```
residual ~ r_index → R² for ranking
```
Regresses residuals against each index category, returns top 3 matches:
- **TOPIX-17 Sectors**: 17 industry groups (0080-0090)
- **33 Sectors**: Detailed sector indices (0040-0060)
- **Size + Style**: TOPIX Core30/Large70/Mid400/Small, Value/Growth

**Module Structure**:
```
packages/shared/src/factor-analysis/
├── types.ts        # DailyReturn, OLSResult, IndexMatch
├── returns.ts      # Daily return calculation
├── regression.ts   # OLS implementation (beta, alpha, R², residuals)
└── factor-regression.ts  # Two-stage orchestration
```

## CLI Commands

```bash
# ROE analysis
bun cli analysis roe 7203 --format table --sort-by roe_desc

# Screening
bun cli analysis screening --no-range-break-fast  # Slow only
bun cli analysis screening --no-range-break-slow  # Fast only

# Factor regression
bun cli analysis factor-regression 7203
bun cli analysis factor-regression 7203 --lookback-days 126 --format json

# Rankings
bun cli analysis ranking --limit 20 --markets prime
bun cli analysis ranking --lookback-days 5  # 5-day average
```

## Margin Pressure Indicators

信用取引データを用いた需給分析指標。

| Indicator | Formula | Description |
|-----------|---------|-------------|
| Margin Long Pressure | (LongVol - ShrtVol) / N-day avg volume | Net long position relative to trading activity |
| Margin Flow Pressure | Δ(LongVol - ShrtVol) / N-day avg volume | Acceleration of margin positions |
| Margin Turnover Days | LongVol / N-day avg volume | Days to absorb long margin positions |

```typescript
// Margin pressure indicators are now served via apps/bt/ API
// apps/ts/api proxies to: POST http://localhost:3002/api/indicators/margin
// API endpoint: GET /api/analytics/stocks/{symbol}/margin-pressure?period=15
```

## API Endpoints

- `GET /api/analytics/roe` - ROE calculation
- `GET /api/analytics/screening` - Stock screening
- `GET /api/analytics/factor-regression/{symbol}` - Factor regression
- `GET /api/analytics/fundamentals/{symbol}` - Fundamental metrics time-series
- `GET /api/analytics/ranking` - Market rankings
- `GET /api/analytics/margin-pressure/{symbol}` - Margin pressure indicators
