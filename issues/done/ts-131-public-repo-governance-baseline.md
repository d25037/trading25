---
id: ts-131
title: "Public repository governance baseline (SECURITY / LICENSE / CODEOWNERS)"
status: done
priority: high
labels: [security, governance, oss]
project: ts
created: 2026-02-21
updated: 2026-02-24
depends_on: []
blocks: []
parent: null
---

# ts-131 Public repository governance baseline (SECURITY / LICENSE / CODEOWNERS)

## 目的
private -> public 移行前に、公開リポジトリとして必須のガバナンス文書を整備する。

## 受け入れ条件
- ルートに `SECURITY.md` を追加し、脆弱性報告窓口・初回応答目標・公開方針を明記する。
- ルートに `LICENSE` を追加し、採用ライセンスを明示する。
- `.github/CODEOWNERS` を追加し、`apps/bt` `apps/ts` `contracts` `docs` のレビュー責務を定義する。
- README から上記文書への導線を追加する。

## 実施内容
- [x] `SECURITY.md` のドラフト作成（報告経路、SLA、対象バージョン）
- [x] `LICENSE` の選定・追加（MIT）
- [x] `.github/CODEOWNERS` の作成
- [x] `README.md` へ参照リンク追加

## 結果
- ルートに `SECURITY.md` を追加し、脆弱性報告経路（GitHub Security Advisory private report）・初回応答目標・公開方針を明記。
- ルートに `LICENSE`（MIT）を追加し、ライセンスを明示。
- `.github/CODEOWNERS` を追加し、`apps/bt` `apps/ts` `contracts` `docs` のレビュー責務を定義。
- `README.md` に Governance セクションを追加し、上記文書への導線を整備。

## 補足
- 背景: `docs/archive/reports/public-repo-readiness-audit-2026-02-20.md` の High/Medium 対応。
