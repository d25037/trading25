---
id: bt-033
title: "Screening: result selection/sort 判定を domains へ抽出"
status: open
priority: high
labels: [screening, domains, refactor, bt]
project: bt
created: 2026-02-26
updated: 2026-02-26
depends_on: [bt-032]
blocks: [bt-034]
parent: null
---

# bt-033 Screening: result selection/sort 判定を domains へ抽出

## 目的
スクリーニング結果の選別・並び替えロジックを service から domain に移し、再利用可能な結果判定モジュールに統一する。

## 受け入れ条件
- `_find_recent_match_date` が domain モジュールへ移管される
- `_pick_best_strategy` が domain モジュールへ移管される
- `_sort_results` が domain モジュールへ移管される
- `_build_result_item` が domain モジュールへ移管される
- API レスポンス形式は変更しない

## 実施内容
- `src/domains/analytics/screening_results.py`（仮）を追加
- sort key / order / tie-break を domain 側で明示化
- 既存 route/service の契約互換を維持した置換を行う

## 結果
（完了後に記載）

## 補足
- `sortBy=matchedDate` / `order=desc` の既定値互換を維持する
