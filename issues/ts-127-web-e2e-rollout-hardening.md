---
id: ts-127
title: "Web E2E rollout hardening (post-smoke)"
status: open
priority: medium
labels: [test, e2e]
project: ts
created: 2026-02-12
updated: 2026-02-12
depends_on: []
blocks: []
---

# ts-127 Web E2E rollout hardening (post-smoke)

## 目的
- Playwright smoke 導入後の運用安定化を進め、required check 化と対象シナリオ拡張を安全に実施する。

## 受け入れ条件
- CI の web e2e smoke 実行結果を一定期間（目安: 1 週間）観測し、flake 原因を潰した上で required check に昇格する。
- Nightly などで実行する拡張E2E（最低 2 シナリオ）を追加する。
- E2E 運用手順（失敗時の切り分け・再実行・artifact確認）を `apps/ts/TESTING.md` か runbook に追記する。

## 実施内容
- [ ] CI 実行結果を観測し、失敗パターンを分類（app起動/プロキシ/API遅延/テスト実装由来）
- [ ] `@smoke` 以外の拡張シナリオを追加
  - [ ] Backtest attribution のポーリング遷移（pending/running/completed）
  - [ ] Lab SSE の接続・再接続・終了状態
- [ ] required check 化のPRを作成し、運用ルールを文書化

## 結果
- 未着手

## 補足
- 初期 smoke はこのworktreeで導入済み（navigation/backtest-health/portfolio-crud）。
