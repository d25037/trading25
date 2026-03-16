---
name: ts-web-design-guidelines
description: ts/web の UI 実装を Web Interface Guidelines 観点で監査するスキル。アクセシビリティ、視覚設計、操作性、情報設計のレビューを求められたときに使用する。
---

# ts-web-design-guidelines

## When to use

- ts/web の UI 実装を guideline 観点でレビューするとき。
- アクセシビリティ、視覚設計、操作性、情報設計の監査を求められたとき。

## Source of Truth

- `apps/ts/packages/web/src`
- `apps/ts/packages/web/AGENTS.md`
- `apps/ts/AGENTS.md`
- `https://raw.githubusercontent.com/vercel-labs/web-interface-guidelines/main/command.md`

## Workflow

1. guideline を確認し、対象 UI のファイル範囲を決める。
2. 指定された UI ファイルを読み、必要なら関連 hook と type も確認する。
3. guideline 観点で監査し、`file:line` 形式で指摘を出す。
4. Tailwind v4 と既存デザインシステムとの整合を保つ。

## Guardrails

- guideline を機械的に押し付けず、既存パターンとの整合を優先する。
- file:line 付きの具体的指摘を優先し、抽象的な感想で埋めない。
- ファイル指定がない場合は対象パスを特定してから監査する。

## Verification

- `bun run quality:typecheck`
- `bun run workspace:test`
