---
id: ts-132
title: "Public repository security automation (Dependabot + secret scan + vuln audit)"
status: open
priority: high
labels: [security, ci, dependency]
project: ts
created: 2026-02-21
updated: 2026-02-21
depends_on: []
blocks: []
parent: null
---

# ts-132 Public repository security automation (Dependabot + secret scan + vuln audit)

## 目的
公開後の継続運用で、依存関係脆弱性と秘密情報混入をCIで早期検知できる状態を作る。

## 受け入れ条件
- `.github/dependabot.yml` を追加し、bun/uv/github-actions の更新監視を有効化する。
- CI に secret scan（例: gitleaks or detect-secrets）を追加する。
- CI に依存関係脆弱性チェック（Python/Bun 両系）を追加する。
- 失敗時の triage 手順を runbook に明記する。

## 実施内容
- [ ] Dependabot 設定追加（更新頻度、PR上限、ラベル）
- [ ] secret scan ジョブ追加
- [ ] Python/Bun の vuln audit ジョブ追加
- [ ] false positive の運用ルール定義
- [ ] `issues/ts-011-ops-runbook.md` か docs に手順追記

## 結果
- 未着手

## 補足
- 背景: `docs/reports/public-repo-readiness-audit-2026-02-20.md` の Medium 対応。
