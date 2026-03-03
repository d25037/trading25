---
id: bt-036
title: "Phase 6: job duration metrics を backtest/optimize へ拡張"
status: open
priority: medium
labels: [phase6, observability, metrics, bt]
project: bt
created: 2026-03-02
updated: 2026-03-02
depends_on: []
blocks: []
parent: null
---

# bt-036 Phase 6: job duration metrics を backtest/optimize へ拡張

## 目的
- `metrics_recorder.record_job_duration` と `job_lifecycle` structured log を screening だけでなく backtest/optimize にも統一適用する。

## 背景
- Phase 5 で request/jquants/screening の観測性は整備済み。
- 主要ジョブのうち backtest/optimize は同等粒度の duration 計測が不足している。

## 受け入れ条件
- [ ] backtest/optimize の completed/failed/cancelled で `record_job_duration` が記録される。
- [ ] `job_lifecycle` ログに `jobType/jobId/status/durationMs/correlationId` が揃う。
- [ ] 単体テストで metrics/log 呼び出しを検証する。

## 実施内容
- [ ] backtest job service / optimize job service に計測フック追加
- [ ] runbook と observability docs を更新

## 結果
- 未着手

## 補足
- release gate そのもの（contract/golden/coverage）は Phase 6 で導入済み。
- 本 issue は観測性の適用範囲を主要ジョブ全体へ拡張する追補。
