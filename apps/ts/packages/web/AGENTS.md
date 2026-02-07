# Web Package - React Application

React 19 financial trading application with Vite, Tailwind CSS v4, and lightweight-charts.

## Architecture

- **Frontend**: React 19 + TypeScript + Vite
- **Logger**: Browser-specific (separate from Node.js)
- **Styling**: Tailwind CSS v4 + Radix UI + shadcn/ui
- **Charts**: lightweight-charts for OHLC/margin
- **Editor**: Monaco Editor for YAML strategy editing
- **State**: Zustand + TanStack Query
- **API**: HTTP requests to `/api/*` (proxied to FastAPI port 3002)

## Tailwind CSS v4 Rules

```css
/* V4 Syntax (required) */
@import "tailwindcss";
@custom-variant dark (&:where(.dark, .dark *));
@theme { --color-background: oklch(100% 0 0); }

/* V3 Syntax (NEVER USE) */
@tailwind base;  /* INVALID in v4 */
```

## Structure

```
src/
├── components/Chart/       # OHLC charts, Fundamentals, FactorRegression panels
├── components/Backtest/    # Backtest runner, strategies, results, optimization
├── components/Lab/         # Lab: generate, evolve, optimize, improve (SSE progress)
├── components/Editor/      # Monaco YAML editor
├── components/ui/          # shadcn/ui components
├── pages/                  # ChartsPage, AnalysisPage, BacktestPage
├── stores/                 # Zustand state (chartStore, backtestStore)
├── hooks/                  # useStockData, useBtIndicators, useBtMarginIndicators, useBtSignals, useFundamentals, useBacktest, useLab
├── constants/              # Signal reference data
└── types/                  # Re-exports from @trading25/shared, backtest types
```

## Chart Data & Indicator Calculation

全て FastAPI (:3002) に統合:
- **OHLCVデータ**: `POST /api/ohlcv/resample` (`useBtOHLCV`)
  - daily/weekly/monthly timeframe変換
  - relativeMode (TOPIX相対) は `benchmark_code: 'topix'` パラメータ
- **インジケータ**: `POST /api/indicators/compute` (`useBtIndicators`)
- **Margin指標**: `POST /api/indicators/margin` (`useBtMarginIndicators`)
- **Signal Overlay**: `POST /api/signals/compute` (`useBtSignals`)

仕様: `apps/bt/docs/spec-timeframe-resample.md`

## Development

```bash
bun run dev                # Vite (port 5173)
bun run build              # Production build
bun run typecheck          # TypeScript validation
bun run test               # Run tests
```
