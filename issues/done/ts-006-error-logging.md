---
id: ts-006
title: "サイレントエラーの可観測性改善"
status: done
priority: medium
labels: []
project: ts
created: 2026-01-30
updated: 2026-02-13
depends_on: []
blocks: []
parent: null
---

# ts-006 サイレントエラーの可観測性改善

## 目的
catch で握り潰している箇所を可視化する。

## 受け入れ条件
- 主要 catch にログまたはメトリクス出力
- エラー追跡が可能

## 実施内容
- silent/no-op catch の可観測性を改善
  - `shared/src/utils/browser-token-storage.ts`: `isAvailable()` の catch で localStorage 判定失敗を warn 出力（1回のみ）
  - `shared/src/utils/find-project-root.ts`: 壊れた `package.json` の parse 失敗を warn 出力
  - `shared/src/utils/secure-env-manager.ts`: 暗号化トークン判定の parse 失敗を warn 出力（平文扱いへフォールバック）
  - `shared/src/utils/dataset-paths.ts`: 書き込み権限エラーで原因メッセージを保持
  - `shared/src/utils/logger.ts` / `clients-ts/src/utils/logger.ts`: 環境判定 catch を warn 出力（1回のみ）
- HTTP エラーハンドリングの追跡性を改善
  - `clients-ts/src/base/http-client.ts`: 非JSONエラーレスポンス時の観測ログ追加、text fallback を保持
  - `clients-ts/src/backtest/BacktestClient.ts`: JSON parse 失敗時に原因文字列をエラーメッセージへ付与
- テストを追加し、可観測性改善の分岐を検証
  - `shared/src/utils/browser-token-storage.test.ts`
  - `shared/src/utils/logger.test.ts`
  - `clients-ts/src/utils/logger.test.ts`
  - `clients-ts/src/base/http-client.test.ts`（分岐拡張）

## 結果
- 主要 catch で silent になっていた箇所にログを追加し、原因追跡可能な形へ改善
- 非JSON応答・JSON parse失敗の文脈情報（status/parse失敗理由）が取得可能になった
- 検証:
  - `bun run --filter @trading25/clients-ts test:coverage` 61 pass
    - `src/base/http-client.ts`: funcs 90.00 / lines 100.00
    - `src/utils/logger.ts`: funcs 96.88 / lines 100.00
  - `bun run --filter @trading25/shared test:coverage` 427 pass
    - `src/utils/browser-token-storage.ts`: funcs 100.00 / lines 100.00
    - `src/utils/logger.ts`: funcs 96.88 / lines 100.00
    - `src/utils/dataset-paths.ts`: funcs 100.00 / lines 80.49
    - `src/utils/find-project-root.ts`: funcs 100.00 / lines 88.89
    - `src/utils/secure-env-manager.ts`: funcs 81.25 / lines 80.41
  - `bun run --filter @trading25/shared typecheck` pass
  - `bun run typecheck:all` (apps/ts) pass
  - `bun run lint` (apps/ts) pass

## 補足
