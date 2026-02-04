---
id: ts-009
title: "CI 標準化"
status: done
priority: medium
labels: []
project: ts
created: 2026-01-30
updated: 2026-01-31
depends_on: []
blocks: []
parent: null
---

# ts-009 CI 標準化

## 目的
lint/typecheck/test を PR で必須化する。

## 受け入れ条件
- CI で標準ジョブが動作
- ドキュメント化済み

## 実施内容
- `.github/workflows/ci.yml` は既に全ブランチ push + PR で Lint → OpenAPI型生成 → Typecheck → Test with coverage → Coverage threshold 検証を実行
- `CLAUDE.md` に CI セクションを追加し、トリガー条件・ステップ一覧・ランタイム情報をドキュメント化

## 結果
- CI ジョブ: Lint, OpenAPI型生成, Typecheck, Test with coverage, Coverage threshold 検証（ubuntu-latest, Bun 1.3.8）
- ドキュメント化: CLAUDE.md の `## CI` セクションに記載完了
- 受け入れ条件をすべて達成

## 補足
