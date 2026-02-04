---
id: ts-008
title: "CLI の process.exit 削減"
status: done
priority: medium
labels: []
project: ts
created: 2026-01-30
updated: 2026-01-30
depends_on: []
blocks: []
parent: null
---

# ts-008 CLI の process.exit 削減

## 目的
CLI のテスト容易性と一貫したエラーハンドリングを確保する。

## 受け入れ条件
- `process.exit()` の大量利用を解消
- 例外/戻り値に基づく終了コード制御

## 実施内容
- CLIError に silent フラグと cause オプションを追加
- CLICancelError (exitCode=0, silent=true) を新設
- index.ts で process.exitCode による自然終了に変更
- handleCommandError に CLIError ガード追加（spinner.stop() + re-throw）
- 全 ~75 箇所の process.exit() を CLIError throw に置換
- DB_TIPS 定数追加、冗長な catch block を handleCommandError に統合
- spinner 停止漏れによるプロセスハング問題を修正

## 結果
- process.exit() は index.ts の process.exitCode のみに集約
- 52 files changed, +529/-458
- 全 923 テストパス、typecheck・lint クリーン
- コミット: f3f1e91

## 補足
- Phase 2 レビュー（code-reviewer + Codex）で spinner 停止漏れを発見し Phase 3 で修正
- Phase 4 で catch block を handleCommandError に統合しコード簡潔化
