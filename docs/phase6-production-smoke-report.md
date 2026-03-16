# Phase 6 Production-Scale Smoke Report

更新日: 2026-03-16

## 固定条件

- 実データ SoT: `~/.local/share/trading25/market-timeseries/market.duckdb`
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
- dataset build:
  - current collector は end-to-end `POST /api/dataset` を計測する
  - historical JSON / table below is the pre-change synthetic throughput baseline (`build_stock_data_row`, `rowsPerRun=50,000`)
- run count:
  - warmup なし、連続 3 run（同一条件）
- runtime artifact:
  - `$TMPDIR/trading25-phase6-runtime`

## 計測結果

計測 JSON: [`docs/phase6-production-smoke-baseline.json`](./phase6-production-smoke-baseline.json)

| workload | median | p95 |
|---|---:|---:|
| screening | 7.3466 sec | 7.7609 sec |
| backtest | 30.0962 sec | 32.0844 sec |
| dataset_build_throughput | 83,935,944.4265 rows/min | 86,427,831.3661 rows/min |

## 再現コマンド

```bash
UV_CACHE_DIR="${TMPDIR:-/tmp}/uv-cache" \
uv run --project apps/bt python scripts/collect-production-smoke-baseline.py \
  --runs 3 \
  --output docs/phase6-production-smoke-baseline.json
```

注記:
- backtest は marimo export 実行時にローカル socket を利用する。
- 制限環境では実行権限が必要な場合がある。
- `scripts/collect-production-smoke-baseline.py` は現在 `--dataset-preset topix100` を使った end-to-end dataset build を出力する。下記の throughput 数値を current baseline として使う場合は JSON を再生成すること。
