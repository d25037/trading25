---
id: bt-053
title: "decile / bucket naming と research wrapper interface を整理"
status: done
priority: medium
labels: [bt, analytics, refactor, naming]
project: bt
created: 2026-03-27
updated: 2026-03-30
depends_on: [bt-050, bt-051]
blocks: []
parent: bt-049
---

# bt-053 decile / bucket naming と research wrapper interface を整理

## 目的
- 10分割なのに `quartile` 名が残っている命名の歪みを解消する。
- wrapper / notebook / result dataclass の public field を、実態に合う名前へ揃える。

## 背景
- [topix100_sma_ratio_rank_future_close.py](/Users/shinjiroaso/dev/trading25/apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close.py) では `QUARTILE_ORDER`、`feature_quartile` などの名称が 10 decile 実装とずれている。
- 派生 module や notebook がこの命名を引きずっており、読み手に不要な認知負荷を与えている。

## 受け入れ条件
- [x] internal naming が `decile` ベースへ整理される。
- [x] public result field を変える場合は互換 alias または段階移行方針が明示される。
- [x] notebook の表示文言と table column 名も実態に揃う。

## 実施内容
- [x] `quartile` 系内部名を `decile` へ段階的に置換する。
- [x] label map / order constant / helper 名を見直す。
- [x] wrapper module と notebook の文言・列名を同期する。
- [x] 必要なら deprecation comment を追加する。

## 結果
- domain / test / notebook / wrapper callsite から `quartile` 命名を除去し、`decile` / `feature_decile` / `decile_future_summary_df` へ統一した。
- 互換 alias は残さず、既存 notebook / callsite 側もまとめて更新する方針で移行した。
- `analytics`, `tests/unit/domains/analytics`, `notebooks/playground` 配下で `quartile` / `QUARTILE` の残存がないことを確認した。
- 実装は commit `ec61197` で反映した。

## 補足
- OpenAPI / API 契約に露出していない domain 専用 field から先に整理する。
