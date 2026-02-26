---
id: bt-034
title: "Screening: evaluator 分解（stock/strategy 評価）"
status: open
priority: medium
labels: [screening, domains, refactor, bt]
project: bt
created: 2026-02-26
updated: 2026-02-26
depends_on: [bt-032, bt-033]
blocks: []
parent: null
---

# bt-034 Screening: evaluator 分解（stock/strategy 評価）

## 目的
`screening_service` 内の stock/strategy 評価ループを domain evaluator に分離し、並列実行と判定ロジックの責務を分割する。

## 受け入れ条件
- `_evaluate_stock` が domain evaluator に移管される
- `_evaluate_strategy_input` が domain evaluator に移管される
- `_evaluate_strategy` が domain evaluator に移管される
- `_apply_stock_outcome` が domain evaluator に移管される
- service 側はジョブ進行管理と I/O のみに限定される

## 実施内容
- `src/domains/analytics/screening_evaluator.py`（仮）を追加
- 入力 DTO / 出力 DTO を domain モデル化
- 既存 ThreadPoolExecutor 前提の並列評価パスを維持

## 結果
（完了後に記載）

## 補足
- 分解後も warning 集約仕様と cache token の互換を維持する
