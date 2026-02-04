---
id: ts-109
title: "trading25-bt との契約整合"
status: done
priority: medium
labels: [backtest, types]
project: ts
created: 2026-01-30
updated: 2026-02-01
depends_on: []
blocks: []
parent: null
---

# ts-109 trading25-bt との契約整合

## 目的
Backtest API 仕様と TS 型を自動で同期する。

## 受け入れ条件
- OpenAPI などから型生成が行われる
- 変更差分が検知される

## 実施内容
- bt サーバーの OpenAPI スキーマを取得・スナップショット化するスクリプト作成 (`fetch-bt-openapi.ts`)
- `openapi-typescript` による型自動生成パイプライン構築 (`bt:generate-types`)
- `Normalize<T>` + `AssertExtends` パターンによるコンパイル時型互換チェック (`type-compatibility-check.ts`)
- CI に bt 型生成ステップを追加し `tsc --noEmit` で自動検証
- bt サーバー未起動でも CI が動くスナップショット戦略

## 結果
- 手動型と OpenAPI 生成型の構造互換が tsc で自動検証される
- 手動型を故意に変更すると typecheck でコンパイルエラーが発生することを確認
- 残存課題は ts-115 (SignalFieldDefinition.type enum 化), ts-116 (OptimizationHtmlFile* スキーマ追加) として起票済み

## 補足
- `bt-api-types.ts` は gitignore 対象（CI で毎回生成）
- `openapi/bt-openapi.json` はコミット対象（オフライン型生成用スナップショット）
