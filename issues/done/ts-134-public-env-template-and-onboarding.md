---
id: ts-134
title: "Public onboarding env template (.env.example)"
status: done
priority: medium
labels: [docs, onboarding, security]
project: ts
created: 2026-02-21
updated: 2026-02-24
depends_on: []
blocks: []
parent: null
---

# ts-134 Public onboarding env template (.env.example)

## 目的
公開リポジトリでの初期セットアップを安全かつ一貫化するため、秘密情報を含まない env テンプレートを提供する。

## 受け入れ条件
- リポジトリルート `.env.example`（必要なら `apps/bt` 用補足）を追加する。
- 必須変数（`JQUANTS_API_KEY`, `JQUANTS_PLAN`, `API_BASE_URL`, `BT_API_URL`）をコメント付きで定義する。
- README / Quick Start にテンプレートコピー手順を追記する。
- テンプレートに実鍵・実URL（本番）を含めない。

## 実施内容
- [x] `.env.example` 作成
- [x] README 追記
- [x] 開発/CI での読み込み差異を確認

## 結果
- ルート `.env.example` を追加し、`JQUANTS_API_KEY` / `JQUANTS_PLAN` / `API_BASE_URL` / `BT_API_URL` をコメント付きで定義。
- ルート `.gitignore` に `!**/.env.example` / `!**/.env.template` を追加し、テンプレートを追跡可能にした。
- `README.md` と `apps/ts/README.md` の Quick Start に `cp .env.example .env` 手順を追記。
- テンプレート値はローカル開発向けのプレースホルダ/localhost のみを使用し、本番 URL・実鍵は未記載。

## 補足
- 背景: `docs/archive/reports/public-repo-readiness-audit-2026-02-20.md` の Low 対応。
