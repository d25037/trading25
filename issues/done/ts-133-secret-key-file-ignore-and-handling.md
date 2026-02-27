---
id: ts-133
title: "Secret key file hardening (.trading25.key ignore + handling rule)"
status: done
priority: high
labels: [security, secret-management]
project: ts
created: 2026-02-21
updated: 2026-02-24
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
- [x] `.gitignore` 更新
- [x] 鍵ファイル運用（生成場所/権限/削除）の文書化
- [x] pre-commit/CI での検知方法を検討（CI常時検知は `ts-132` で実施）

## 結果
- ルート `.gitignore` に `.trading25.key` / `**/.trading25.key` を追加し、誤コミットを防止。
- `apps/ts/packages/shared/AGENTS.md` に鍵ファイル運用ルール（配置・権限・ローテーション・削除）を追記。
- runbook `docs/security/secret-key-runbook.md` を追加し、`git ls-files | rg 'trading25\\.key'` を含む追跡確認手順を明文化。
- CI への secret scan 常時組み込みは `ts-132` へ継続。

## 補足
- 背景: `docs/archive/reports/public-repo-readiness-audit-2026-02-20.md` の High 対応。
