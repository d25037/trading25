---
id: bt-062
title: "high-coupling research family を段階分解する"
status: open
priority: high
labels: [bt, research, refactor, analytics]
project: bt
created: 2026-04-18
updated: 2026-04-18
depends_on: [bt-061]
blocks: [bt-063]
parent: bt-060
---

# bt-062 high-coupling research family を段階分解する

## 目的
- `topix100_streak_*` と event-conditioned analytics の責務を family 単位で分割し、巨大 module の変更コストを下げる。
- data access / feature panel / model / walkforward / sampling / report shaping の境界を family 内で明確にする。

## 受け入れ条件
- [ ] `topix100_streak_*` の分割先が family 内で合意され、最初の extraction が入っている。
- [ ] event-conditioned analytics の `event filter / outcome builder / sampling / report shaping` 分離が少なくとも 1 family で実施されている。
- [ ] family ごとの shared helper 依存が concrete study より上位へ寄っている。

## 実施内容
- [ ] TOPIX100 streak / LightGBM chain を `data access / feature panel / model / walkforward / publication` に分ける。
- [ ] event-conditioned analytics を `event filter / outcome builder / sampling / report shaping` に分ける。
- [ ] family ごとの最小 public entrypoint を runner / bundle writer / bundle loader に揃える。

## 結果
- 未着手

## 補足
- 親 issue: `bt-060`
- 依存: `bt-061`
