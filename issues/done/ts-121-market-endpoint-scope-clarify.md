---
id: ts-121
title: /api/market/* エンドポイントのスコープ明確化
status: closed
priority: low
labels: [design, api, api-integration]
project: ts
created: 2026-02-02
updated: 2026-02-02
depends_on: []
blocks: []
parent: null
---

# ts-121 /api/market/* エンドポイントのスコープ明確化

## 目的
`/api/market/*` エンドポイントの消費者と用途を明確にし、不要であれば廃止を検討する。

## 受け入れ条件
- `/api/market/*` の全エンドポイントについて、実際のコンシューマーが特定されていること
- OpenAPIドキュメントに用途・想定コンシューマーが記載されていること
- 不要なエンドポイントがあれば deprecation or 削除されていること

## 実施内容
- 現状の `/api/market/*` エンドポイント:
  - `GET /api/market/stocks/{code}/ohlcv` — apps/bt/ MarketAPIClient が使用（ポートフォリオ分析）
  - `GET /api/market/stocks` — apps/bt/ MarketAPIClient が使用（スクリーニング用全銘柄データ）
  - `GET /api/market/topix` — apps/bt/ MarketAPIClient が使用（TOPIXベンチマーク）
- apps/ts/web, apps/ts/cli からは直接使用されていない（chart/ や analytics/ を使用）
- apps/bt/ の cli_market 削除（bt-020）が実施された場合:
  - `/api/market/stocks` のスクリーニング用途がなくなる可能性
  - `/api/market/topix` のポートフォリオ分析以外の用途がなくなる可能性
- bt-020 の結果に応じて、エンドポイントの縮小・統合を検討

## 結果

### bt-020 完了注記 (2026-02-02)
apps/bt/ の cli_market/ が削除された。apps/bt/ が引き続き使用する market API エンドポイント:
- `GET /api/market/stocks/{code}/ohlcv` — signal_screening / ポートフォリオ分析
- `GET /api/market/stocks` — signal_screening（全銘柄データ取得）
- `GET /api/market/topix` — signal_screening β値計算 / cli_portfolio PCA分析

apps/bt/ から `/api/analytics/ranking` と `/api/analytics/screening` への呼び出しは完全に削除された。

## 補足
- `/api/market/*` と `/api/chart/*` は同じ market.db を読むが、レスポンスフォーマットが異なる
- chart/ は camelCase + フロントエンド最適化、market/ は snake_case + Python最適化
- 統合する場合はレスポンスフォーマットの互換性に注意
- bt-020 (apps/bt/ cli_market削除) の影響を受けるため、先にbt-020を解決すべき
