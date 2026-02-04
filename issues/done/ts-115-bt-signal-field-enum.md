---
id: ts-115
title: "bt契約: SignalFieldDefinition.type を enum 化して型互換チェック対象にする"
status: done
priority: low
labels: [backtest, types]
project: ts
created: 2026-02-01
updated: 2026-02-01
depends_on: [ts-109]
blocks: []
---

# ts-115 bt契約: SignalFieldDefinition.type を enum 化して型互換チェック対象にする

## 現状
- 手動型 `SignalFieldDefinition.type` は union literal (`'boolean' | 'number' | 'string' | 'select'`)
- bt (FastAPI) の OpenAPI スキーマでは plain `string` として生成される
- この差異により `type-compatibility-check.ts` で `SignalFieldDefinition` / `SignalDefinition` の型互換チェックが除外されている

## 対応方針
- bt 側で `type` フィールドを Python `Enum` に変更し、OpenAPI スキーマに enum 制約を反映させる
- スキーマ更新後、`type-compatibility-check.ts` に `SignalFieldDefinition` と `SignalDefinition` のチェックを追加する

## 受け入れ条件
- `SignalFieldDefinition` と `SignalDefinition` が `type-compatibility-check.ts` で互換チェック対象になる
- `bun run typecheck:all` が通る

## 結果
- bt 側で `type` フィールドが Python Enum 化され、生成型と手動型の union literal が完全一致
- bt 側で `data_requirements`, `constraints` も OpenAPI スキーマに追加済み
- `SignalFieldDefinition`: 5/6 フィールド (`name`, `type`, `description`, `default`, `options`) をチェック対象化
  - Omit: `constraints`（nullable ミスマッチ: 手動型 `FieldConstraints` vs 生成型 `FieldConstraints | null`、両側から除外）
- `SignalDefinition`: 8/9 フィールド (`key`, `name`, `category`, `description`, `usage_hint`, `yaml_snippet`, `exit_disabled`, `data_requirements`) をチェック対象化
  - Omit: `fields`（ネストした `SignalFieldDefinition[]` は `_SignalFieldDefinition` で個別チェック済み）
