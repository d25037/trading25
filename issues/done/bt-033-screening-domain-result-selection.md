---
id: bt-033
title: "Screening: result selection/sort 判定を domains へ抽出"
status: done
priority: high
labels: [screening, domains, refactor, bt]
project: bt
created: 2026-02-26
updated: 2026-03-02
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
- `apps/bt/src/domains/analytics/screening_results.py` を追加し、`find_recent_match_date` / `pick_best_strategy` / `build_result_item` / `sort_results` を domain へ抽出。
- `screening_service` の `_find_recent_match_date` / `_pick_best_strategy` / `_sort_results` / `_build_result_item` は domain 関数呼び出しに統一。
- `apps/bt/tests/unit/domains/analytics/test_screening_results.py` で sort key/order/tie-break と null score 末尾維持を明示テスト化。
- 既存の service/helper テストと route 側の fundamental/ranking 系テストを再実行し、レスポンス互換を確認。

## 補足
- `sortBy=matchedDate` / `order=desc` の既定値互換を維持する
