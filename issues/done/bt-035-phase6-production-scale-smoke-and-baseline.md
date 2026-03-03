---
id: bt-035
title: "Phase 6: 本番相当データ量 smoke/perf baseline の定常運用化"
status: done
priority: high
labels: [phase6, release-gate, performance, smoke, bt]
project: bt
created: 2026-03-02
updated: 2026-03-03
closed: 2026-03-03
depends_on: []
blocks: []
parent: null
---

# bt-035 Phase 6: 本番相当データ量 smoke/perf baseline の定常運用化

## 目的
- Phase 6 で導入した CI ゲートに加え、本番相当データ量での smoke run と性能 baseline 計測を定常実行できる状態にする。

## 背景
- 現在の `docs/phase6-performance-baseline.json` は deterministic unit-test workload 基準。
- リリース判定では、代表 universe / 実データ量での screening/backtest/build 実測値が必要。

## 受け入れ条件
- [x] 本番相当データ量の smoke run を少なくとも 1 サイクル実行し、結果を docs に保存する。
- [x] screening p95 / backtest median / dataset build throughput を同一条件で再計測する。
- [x] 失敗時の再実行手順と閾値判定手順を runbook に追記する。

## 実施内容
- [x] smoke 実行用の固定入力条件（dataset/strategy/date range）を定義
- [x] 実測コマンドをスクリプト化
- [x] 結果 JSON/Markdown を artifacts + docs に保存

## 結果
- `scripts/collect-production-smoke-baseline.py` を追加し、production-scale smoke baseline の計測を再現可能化。
- 固定条件（`markets=prime`, `strategy=production/range_break_v15`, `recentDays=20`, `runs=3`）で 3run 実測を完了。
- 成果物を以下へ保存:
  - `docs/phase6-production-smoke-baseline.json`
  - `docs/phase6-production-smoke-report.md`
  - `docs/phase6-release-gate-report.md`（Phase 6 report へ反映）
- 測定値:
  - screening: median `7.3466s`, p95 `7.7609s`
  - backtest: median `30.0962s`, p95 `32.0844s`
  - dataset build throughput: median `83,935,944.4265 rows/min`, p95 `86,427,831.3661 rows/min`

## 補足
- Phase 6 の CI contract/golden/coverage gate は導入済み。ここでは「実データ運用ベースライン」の不足を補完する。
