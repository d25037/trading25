---
id: bt-034
title: "Screening: evaluator 分解（stock/strategy 評価）"
status: done
priority: medium
labels: [screening, domains, refactor, bt]
project: bt
created: 2026-02-26
updated: 2026-03-02
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
- `apps/bt/src/domains/analytics/screening_evaluator.py` を追加し、`evaluate_stock` / `evaluate_strategy_input` / `evaluate_strategy` / `apply_stock_outcome` を domain へ移管。
- `screening_service` 側では同名 helper を薄い委譲に変更し、ジョブ進行管理・I/O orchestration へ責務を集約。
- warning 文言、cache token/キー構築、ThreadPoolExecutor 前提の並列実行パス互換を維持。
- `apps/bt/tests/unit/domains/analytics/test_screening_evaluator.py` を追加し、異常系・境界系を含む分岐をテスト化。`coverage --branch` で `screening_evaluator.py` は line 97.48% / branch 100% を確認。

## 補足
- 分解後も warning 集約仕様と cache token の互換を維持する
