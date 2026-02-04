---
id: bt-021
title: TOPIX二重ロードパスの整理
status: done
priority: medium
labels: [design, data-loader, api-integration]
project: bt
created: 2026-02-02
updated: 2026-02-02
depends_on: []
blocks: []
parent: null
---

# bt-021 TOPIX二重ロードパスの整理

## 目的
MarketAPIClient と DatasetAPIClient の両方から TOPIX をロード可能な現状を整理し、データソースの使い分けを明確にする。

## 受け入れ条件
- バックテスト実行時は DatasetAPIClient 経由の TOPIX のみを使用すること
- market.db 経由の TOPIX ロードの用途が明確にドキュメント化されていること
- loader 関数の命名・配置が用途に沿って整理されていること

## 実施内容
- ファイル: `src/data/loaders/index_loaders.py`
- 現状:
  - `load_topix_data()` → DatasetAPIClient (dataset.db)
  - `load_topix_data_from_market_db()` → MarketAPIClient (market.db)
- バックテストでは dataset.db の TOPIX が正（バックテスト期間と整合するため）
- market.db 経由は以下の用途に限定すべき:
  - ポートフォリオ分析（最新データが必要）
  - cli_market のランキング/スクリーニング（bt-020で削除検討中）
- bt-020 で cli_market が削除された場合、market.db 経由の TOPIX ロードの用途はポートフォリオ分析のみとなる
- `load_topix_data_from_market_db()` をポートフォリオ専用ローダーに移動するか、docstringで用途を明記

## 結果
- `index_loaders.py` のモジュールdocstringに二重パスのアーキテクチャ説明を追加
- `load_topix_data()` のdocstringに「バックテスト専用（dataset.db）」を明記
- `load_topix_data_from_market_db()` のdocstringを更新:
  - bt-020完了後の利用箇所を正確に列挙: signal_screening β値計算 + cli_portfolio PCA分析
  - market.db TOPIXの用途は「ポートフォリオ分析 + signal_screening」
- 全テスト通過確認済み

## 補足
- depends_on に bt-020 を追加する可能性あり（cli_market 削除の影響範囲次第）
- dataset.db と market.db で TOPIX データの期間・粒度が異なる場合がある
