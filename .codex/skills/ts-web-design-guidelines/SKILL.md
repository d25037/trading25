---
name: ts-web-design-guidelines
description: ts/web の UI 実装を Web Interface Guidelines 観点で監査するスキル。アクセシビリティ、視覚設計、操作性、情報設計のレビューを求められたときに使用する。
---

# ts-web-design-guidelines

## Scope

- Primary target: `apps/ts/packages/web/src/**`
- Secondary target: UI を呼び出す hooks/types (`apps/ts/packages/web/src/hooks/**`, `src/types/**`)

## Guidelines Source

- `https://raw.githubusercontent.com/vercel-labs/web-interface-guidelines/main/command.md`

## Workflow

1. 最新 guideline を取得する。
2. 指定された UI ファイルを読む（指定なしなら `packages/web/src/components/**` を優先）。
3. guideline 全項目で監査し、`file:line` 形式で指摘を出す。
4. プロジェクト既存のデザインシステム（Tailwind v4 + shadcn/ui）との整合を保つ。

ファイル指定がない場合は、対象パス（例: `packages/web/src/components/**`）を確認してから監査する。
