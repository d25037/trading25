---
id: ts-012
title: "ログ/メトリクス標準化"
status: done
priority: medium
labels: []
project: ts
created: 2026-01-30
updated: 2026-02-10
closed: 2026-02-10
depends_on: []
blocks: []
parent: null
---

# ts-012 ログ/メトリクス標準化

## 目的
API/CLI のエラートレースと性能観測を強化する。

## 受け入れ条件
- 主要処理に統一フォーマットのログ
- 失敗時に相関 ID が追跡可能

## 実施内容
- FastAPI 側で J-Quants 実 fetch と proxy cache 状態を構造化ログで出力するよう更新
  - `event="jquants_fetch"`（外部 fetch 実行）
  - `event="jquants_proxy_cache"`（hit/miss/wait）
- リクエストログを `event="request"` / `event="request_error"` で統一
- 内部 API クライアント呼び出しで `x-correlation-id` を伝播

## 結果
- 相関 ID を軸に 1 リクエスト内の内部呼び出し・外部 J-Quants 呼び出しを追跡可能にした
- 主要ログを構造化フォーマットへ統一し、観測性を改善
- 変更対象:
  - `apps/bt/src/server/clients/jquants_client.py`
  - `apps/bt/src/server/services/jquants_proxy_service.py`
  - `apps/bt/src/server/middleware/request_logger.py`
  - `apps/bt/src/api/client.py`
  - 関連 unit test 一式を追加・更新

## 補足
