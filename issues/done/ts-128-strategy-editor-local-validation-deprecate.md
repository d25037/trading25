---
id: ts-128
title: Strategy Editor local validation deprecate
status: done
priority: medium
labels: [web, validation, backtest]
project: ts
created: 2026-02-13
updated: 2026-02-13
depends_on: []
blocks: []
parent: null
---

# ts-128 Strategy Editor local validation deprecate

## 目的
- Strategy Editor で backend strict validation が導入されたため、frontend の重複ローカル検証を整理し、検証ロジックの単一責務を backend に寄せる。

## 受け入れ条件
- `StrategyEditor` の保存/検証時判定が backend `/api/strategies/{name}/validate` を正として動作する。
- frontend 側 `strategyValidation.ts` は削除または `@deprecated` 明示のどちらかに統一される。
- UX 劣化（エラーメッセージ遅延、保存可否判定の不整合）がないことをテストで確認する。
- ドキュメントまたはコードコメントで「検証の責務は backend」を明示する。

## 実施内容
- [x] 現状の `validateStrategyConfigLocally` 呼び出し箇所を整理し、削除/縮退方針を決める。
- [x] `StrategyEditor` のバリデーションフローを backend 優先へ一本化する。
- [x] 必要に応じてローカル側は YAML parse と最小限の型ガードのみに限定する。
- [x] `strategyValidation` のテストを更新し、不要テストを削除する。
- [x] E2E or component test で typo（例: `foward_eps_growth`）の検出が backend 経由で維持されることを確認する。

## 結果
- `StrategyEditor` から `validateStrategyConfigLocally` / `mergeValidationResults` の依存を削除し、保存/検証の判定源を backend `/api/strategies/{name}/validate` に一本化。
- ローカル側は YAML parse（オブジェクト判定）だけを継続。
- backend 検証リクエスト失敗時は `Validation request failed: ...` を UI 表示し、保存を停止。
- `strategyValidation.ts` に `@deprecated` を追加し、backend が source of truth である旨を明記。
- 追加テスト:
  - `apps/ts/packages/web/src/components/Backtest/StrategyEditor.test.tsx`
  - backend validation NG時/通信失敗時に保存されないことを検証。

## 補足
- backend strict validation は実装済み（`/api/strategies/{name}/validate` と保存経路で適用）。
- 本Issueは frontend 側の重複検証整理に限定する。
