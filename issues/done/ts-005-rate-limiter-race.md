---
id: ts-005
title: "BaseJQuantsClient のレートリミッタ競合解消"
status: done
priority: medium
labels: []
project: ts
created: 2026-01-30
updated: 2026-01-30
depends_on: []
blocks: []
parent: null
---

# ts-005 BaseJQuantsClient のレートリミッタ競合解消

## 目的
並列リクエスト時の競合・不正な待機時間を排除する。

## 受け入れ条件
- 共有状態にロック/キュー制御がある
- 複数インスタンス利用時も期待通りのレート制御

## 実施内容

### 問題分析
`BaseJQuantsClient.waitForRateLimit()` は Promise チェーンパターンで並列リクエストを直列化していたが、以下の問題があった:
1. `.catch(() => {})` によるエラーの暗黙的な握りつぶし — エラー情報が失われる
2. Promise チェーン参照の脆弱性 — チェーンが壊れた場合にリクエストがキューをバイパスする可能性
3. 明示的なキュー構造がない — 保留中リクエストの管理・検査ができない

### 修正内容
`RateLimitQueue` クラスを新規実装し、`globalRateLimiter` を置換:
- **FIFO キュー**: `Array<{resolve, reject}>` で厳密な順序保証
- **Mutex (`processing` フラグ)**: 単一の `processQueue` ループで排他制御
- **エラー伝播**: 各リクエストに個別の resolve/reject で適切なエラーハンドリング
- **テスト対応**: `reset()` / `disabled` で既存テストとの互換性維持

### テスト追加
- 並列リクエストの FIFO 順序保証テスト
- リクエスト間最小インターバル強制テスト
- 複数クライアントインスタンス間の共有レート制御テスト
- disabled 時のスキップテスト

## 結果
- 全 60 クライアントテスト通過 (新規 4 テスト含む)
- TypeScript typecheck 通過 (root, web)
- 既存 API (`resetRateLimiter`) の後方互換性維持

## 補足
API typecheck の `type-compatibility-check.ts` エラーは既存の問題（本修正と無関係）。
