---
id: bt-032
title: "Screening: data requirements 判定を domains へ抽出"
status: open
priority: high
labels: [screening, domains, refactor, bt]
project: bt
created: 2026-02-26
updated: 2026-02-26
depends_on: []
blocks: [bt-033, bt-034]
parent: null
---

# bt-032 Screening: data requirements 判定を domains へ抽出

## 目的
`application/services/screening_service.py` にあるデータ要求判定ロジックを `domains/analytics` へ移し、service を orchestration 専用にする。

## 受け入れ条件
- `_build_data_requirements` が domain モジュールへ移管される
- `_needs_data_requirement` が domain モジュールへ移管される
- `_resolve_period_type` が domain モジュールへ移管される
- `_should_include_forecast_revision` が domain モジュールへ移管される
- `screening_service` は domain API 呼び出しのみに整理される

## 実施内容
- `src/domains/analytics/screening_requirements.py`（仮）を追加
- 判定関数を pure function 化し単体テストを追加
- `screening_service` から重複ロジックを削除

## 結果
（完了後に記載）

## 補足
- SoT は `market.db` 前提（dataset フォールバック禁止）を維持する
