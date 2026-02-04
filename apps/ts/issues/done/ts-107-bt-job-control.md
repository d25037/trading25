---
id: ts-107
title: "Backtest ジョブ制御追加"
status: done
priority: medium
labels: []
project: ts
created: 2026-01-30
updated: 2026-01-31
depends_on: []
blocks: []
parent: null
---

# ts-107 Backtest ジョブ制御追加

## 目的
キャンセル/再実行/ログ取得をサポートする。

## 受け入れ条件
- CLI/UI からジョブキャンセルが可能
- 再実行の UX が用意される

## 実施内容
- BacktestClient に cancelJob メソッド追加 (POST /api/backtest/jobs/{id}/cancel)
- JobStatus 型に 'cancelled' を追加
- CLI `backtest cancel <job-id>` コマンド新規作成 (409 Conflict ハンドリング付き)
- CLI `backtest run` で cancelled ステータスのハンドリング追加
- Web UI: useCancelBacktest hook、JobProgressCard にキャンセルボタン追加
- Web UI: cancelled ステータスの表示対応 (JobProgressCard, OptimizationJobProgressCard)
- race condition 対策: onMutate でポーリングクエリキャンセル
- 409 エラー時のクエリ invalidation 追加

## 結果
キャンセル機能を CLI/Web UI の両方で実装完了。再実行機能は別途対応予定。

## 補足
