---
id: ts-101
title: "Backtest CLI の --bt-url 統一"
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

# ts-101 Backtest CLI の --bt-url 統一

## 目的
`backtest run` でも `--bt-url` が使えるように統一する。

## 受け入れ条件
- `run` コマンドに `btUrl` 引数が追加される
- 他サブコマンドと挙動が一致する

## 実施内容

`run.ts` に `btUrl` arg を追加し、`BacktestClient({ baseUrl: btUrl })` で渡すよう変更。
他サブコマンド(list, status, results, validate)と同一パターン。

## 結果

`backtest run` で `--bt-url` オプションが使用可能になり、全サブコマンドで統一された。

## 補足
