# Phase 6 Release Gate Report

更新日: 2026-03-03

## 1. 実装サマリ

Phase 6 のリリースゲートとして、以下を実装した。

- Contract tests を CI 必須ジョブ化
  - `.github/workflows/ci.yml`: `contract-tests`
  - `scripts/check-contract-sync.sh` を追加
    - `apps/bt` ソースから OpenAPI 再生成
    - snapshot (`apps/ts/packages/contracts/openapi/bt-openapi.json`) との厳密比較
    - `scripts/verify-openapi-compat.py` による互換性検証
    - 生成型 (`bt-api-types.ts`) の差分検知
- Golden dataset 回帰テストを CI 必須ジョブ化
  - `.github/workflows/ci.yml`: `golden-dataset-regression`
  - `scripts/test-golden-regression.sh` を追加
  - `test_indicator_golden.py` / `test_resample_compatibility.py` を専用実行
- Coverage gate を CI 必須ジョブ化
  - `.github/workflows/ci.yml`: `coverage-gate`
  - `scripts/coverage-gate.sh` を追加
  - `bt`: `coverage report --fail-under=70`
  - `ts`: `workspace:test:coverage` + `coverage:check`

## 2. Performance Baseline

計測データ: [`docs/phase6-performance-baseline.json`](./phase6-performance-baseline.json)
本番相当 smoke baseline: [`docs/phase6-production-smoke-baseline.json`](./phase6-production-smoke-baseline.json)

測定方法:
- warmup 1 回 + 本計測 3 回
- screening/backtest/build は deterministic な unit-test workload
- build throughput は `build_stock_data_row` の synthetic benchmark（50,000 rows/run）

結果:

| workload | median | p95 |
|---|---:|---:|
| screening | 2.8104 sec | 2.8662 sec |
| backtest | 4.8002 sec | 4.8533 sec |
| dataset_build | 2.7708 sec | 2.9228 sec |
| dataset_build_throughput | 60,007,600.5689 rows/min | 64,568,430.8582 rows/min |

本番相当 smoke（market.db 実データ + production strategy）:

| workload | median | p95 |
|---|---:|---:|
| screening (prime, range_break_v15, recentDays=20) | 7.3466 sec | 7.7609 sec |
| backtest (production/range_break_v15) | 30.0962 sec | 32.0844 sec |
| dataset_build_throughput | 83,935,944.4265 rows/min | 86,427,831.3661 rows/min |

備考:
- 上記は「回帰比較用 baseline」であり、本番相当データ量の SLA 判定値とは区別する。

## 3. Migration Completion

差分要約:
- CI に release gate（contract/golden/coverage）を追加し、Phase 0-5 の成果を保護する検査線を固定。
- baseline 計測手順を `scripts/collect-performance-baseline.py` と JSON成果物で再現可能化。
- 残制約は `issues/` に登録し、次フェーズに引き継ぐ。

## 4. 既知制約と次アクション

- 本番相当データ量での smoke/perf baseline は bt-035 で実施済み
  - [`issues/done/bt-035-phase6-production-scale-smoke-and-baseline.md`](../issues/done/bt-035-phase6-production-scale-smoke-and-baseline.md)
- job duration メトリクスは screening 以外（backtest/optimize）に未展開
  - [`issues/bt-036-phase6-extend-job-duration-metrics.md`](../issues/bt-036-phase6-extend-job-duration-metrics.md)
