---
id: ts-103
title: "BacktestClient の非 JSON レスポンス耐性"
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

# ts-103 BacktestClient の非 JSON レスポンス耐性

## 目的
バックテスト API が非 JSON を返した場合でも安全にエラー処理する。

## 受け入れ条件
- `response.json()` 例外を捕捉し `BacktestApiError` へ変換

## 実施内容

`BacktestClient.request()` で `response.json()` を `response.text()` + `JSON.parse` に置換。
空ボディ・非JSONレスポンスに対してそれぞれ明確な `BacktestApiError` をスロー。
テスト5件を新規作成（正常JSON, 非JSON, 空ボディ, HTTPエラー, truncation）。

## 結果

非JSONレスポンス（HTMLエラーページ等）を受けても `SyntaxError` ではなく `BacktestApiError` として適切にハンドリングされる。

## 補足
