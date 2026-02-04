---
id: ts-004
title: "JSON.parse の例外ガード"
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

# ts-004 JSON.parse の例外ガード

## 目的
破損データによるクラッシュを防止する。

## 受け入れ条件
- 該当箇所に try/catch を追加
- 失敗時はログと安全なフォールバック

## 実施内容

コードベース内の全 `JSON.parse` 呼び出し（5箇所）を調査し、全て try/catch で保護済みであることを確認。
ts-103 で新たに追加した `BacktestClient.request()` 内の `JSON.parse` も try/catch 内で保護。

## 結果

対応済み。全 `JSON.parse` が例外ガード付きであることを確認しclose。

## 補足
