# Phase 6 Production-Scale Smoke Report

更新日: 2026-03-03

## 固定条件

- 実データ SoT: `~/.local/share/trading25/market.db`
  - `stocks=4,455`
  - `stock_data=1,660,958`
  - `topix_data=2,443`
  - `indices_data=176,750`
  - `statements=70,556`
- screening:
  - `markets=prime`
  - `strategies=range_break_v15`
  - `recentDays=20`
  - `sortBy=matchedDate`
  - `order=desc`
  - `limit=200`
- backtest:
  - `strategy=production/range_break_v15`
- dataset build throughput:
  - `build_stock_data_row` synthetic benchmark
  - `rowsPerRun=50,000`
- run count:
  - warmup なし、連続 3 run（同一条件）
- runtime artifact:
  - `/tmp/trading25-phase6-runtime`

## 計測結果

計測 JSON: [`docs/phase6-production-smoke-baseline.json`](/Users/shinjiroaso/.codex/worktrees/0804/trading25/docs/phase6-production-smoke-baseline.json)

| workload | median | p95 |
|---|---:|---:|
| screening | 7.3466 sec | 7.7609 sec |
| backtest | 30.0962 sec | 32.0844 sec |
| dataset_build_throughput | 83,935,944.4265 rows/min | 86,427,831.3661 rows/min |

## 再現コマンド

```bash
UV_CACHE_DIR=/tmp/uv-cache \
uv run --project apps/bt python scripts/collect-production-smoke-baseline.py \
  --runs 3 \
  --output docs/phase6-production-smoke-baseline.json
```

注記:
- backtest は marimo export 実行時にローカル socket を利用する。
- 制限環境では実行権限が必要な場合がある。
