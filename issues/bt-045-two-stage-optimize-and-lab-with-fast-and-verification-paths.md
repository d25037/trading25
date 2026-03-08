---
id: bt-045
title: "Optimize / Lab を fast path と verification path の二段実行へ移行"
status: open
priority: medium
labels: [optimize, lab, vectorbt, nautilus, bt]
project: bt
created: 2026-03-08
updated: 2026-03-08
depends_on: [bt-041, bt-044]
blocks: []
parent: bt-037
---

# bt-045 Optimize / Lab を fast path と verification path の二段実行へ移行

## 目的
- optimize / lab を単一 engine 前提から外し、`vectorbt` 一次探索と `Nautilus` 再検証を組み合わせられるようにする。
- 高速探索と高忠実度検証の trade-off を product として明示する。

## 受け入れ条件
- [ ] optimize trial が engine policy を選択できる。
- [ ] lab candidate の上位候補を verification queue に回せる。
- [ ] result UI / API が fast path と verification path の差分を表示できる。
- [ ] verification 不一致時の扱いが定義される。

## 実施内容
- [ ] optimize/lab orchestration を engine-aware にする。
- [ ] ranking / candidate selection に verification 状態を追加する。
- [ ] best/worst だけでなく verification 結果との差分を保存する。
- [ ] web/API 表示項目を更新する。

## 結果
- 未着手

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 8, 10

