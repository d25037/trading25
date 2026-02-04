---
id: bt-022
title: MarketAPIClient.perform_screening() デッドコード削除
status: done
priority: low
labels: [refactor, dead-code, api-integration]
project: bt
created: 2026-02-02
updated: 2026-02-02
depends_on: []
blocks: []
parent: null
---

# bt-022 MarketAPIClient.perform_screening() デッドコード削除

## 目的
apps/bt/内で未使用のデッドコードを削除し、API結合の責務を明確にする。

## 受け入れ条件
- `MarketAPIClient.perform_screening()` メソッドが削除されていること
- apps/bt/の既存テスト・機能に影響がないこと
- `market_analysis.perform_screening()` は内部の `signal_screening.py` を使用しており、影響を受けないこと

## 実施内容
- ファイル: `src/api/market_client.py`
- `perform_screening()` は apps/ts/ の `/api/analytics/screening` を呼ぶが、apps/bt/内で一度も使用されていない
- apps/bt/は独自の `signal_screening.py` でシグナルベーススクリーニングを実装済み
- このメソッドを削除する

## 結果
- `MarketAPIClient.perform_screening()` メソッドを削除
- 対応するテスト `test_perform_screening` を削除
- docstringから `/api/analytics/screening` エンドポイント記述を削除
- 全テスト通過確認済み

## 補足
- apps/ts/ の `/api/analytics/screening` はレンジブレイク検出
- apps/bt/ の `signal_screening.py` はシグナルベースの独自ロジック
- 両者は目的が異なるため、apps/ts/ API側の変更は不要
