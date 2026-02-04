---
id: ts-102
title: "Backtest 進捗 0% 表示修正"
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

# ts-102 Backtest 進捗 0% 表示修正

## 目的
progress=0 のとき空表示になる問題を修正する。

## 受け入れ条件
- CLI/UI で 0% が正しく表示される

## 実施内容

`run.ts` の falsy check `j.progress ? ...` を `j.progress != null ? ...` に変更。
`0` は正当な進捗値として `0%` と表示されるようになった。

## 結果

progress=0 時に空文字ではなく `0%` が正しく表示される。

## 補足
