---
id: ts-133
title: "Secret key file hardening (.trading25.key ignore + handling rule)"
status: open
priority: high
labels: [security, secret-management]
project: ts
created: 2026-02-21
updated: 2026-02-21
depends_on: [ts-013]
blocks: []
parent: null
---

# ts-133 Secret key file hardening (.trading25.key ignore + handling rule)

## 目的
`SecureEnvManager` が生成する暗号鍵ファイルの誤コミットを防止し、鍵運用ルールを明文化する。

## 受け入れ条件
- ルート `.gitignore` に `.trading25.key`（必要なら `**/.trading25.key`）を追加する。
- `apps/ts/packages/shared` のドキュメントに鍵ファイルの配置・権限・ローテーション方針を追記する。
- 既存トラッキング確認手順（`git ls-files | rg trading25.key` 等）を runbook に追記する。

## 実施内容
- [ ] `.gitignore` 更新
- [ ] 鍵ファイル運用（生成場所/権限/削除）の文書化
- [ ] pre-commit/CI での検知方法を検討

## 結果
- 未着手

## 補足
- 背景: `docs/reports/public-repo-readiness-audit-2026-02-20.md` の High 対応。
