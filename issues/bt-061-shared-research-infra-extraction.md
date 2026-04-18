---
id: bt-061
title: "shared research infra extraction を進める"
status: in-progress
priority: high
labels: [bt, research, refactor, analytics]
project: bt
created: 2026-04-18
updated: 2026-04-18
depends_on: []
blocks: [bt-062, bt-063]
parent: bt-060
---

# bt-061 shared research infra extraction を進める

## 目的
- concrete study module に埋まっている shared infra を抽出し、research family ごとの本題ロジックと周辺インフラを分離する。
- snapshot fallback / code normalize / result codec / publication helper の SoT を family 横断で揃える。

## 受け入れ条件
- [x] event-conditioned analytics の read-only DuckDB helper が shared module に抽出されている。
- [x] 既存の lock fallback test seam を壊さずに concrete study 側が thin wrapper 化されている。
- [x] result codec / publication helper の重複候補が inventory 化されている。
- [x] 追加抽出対象が次の slice として issue / code コメントなしで追える。

## 実施内容
- [x] `readonly_duckdb_support.py` を追加し、live read / snapshot fallback / date range / code normalize を共通化した。
- [x] `topix_close_stock_overnight_distribution.py` / `topix_gap_intraday_distribution.py` / `stock_intraday_overnight_share.py` / `topix100_open_relative_intraday_path.py` の重複 helper を thin wrapper 化した。
- [x] shared helper 自体の unit test を追加した。
- [x] payload-based bundle writer / loader helper を `research_bundle.py` に追加し、event-conditioned analytics 4 module の bundle wrapper 重複を削った。
- [ ] latest bundle resolution helper と plot publication hook の共通化を次の slice で検討する。

## 結果
- 2026-04-18: shared read-only DuckDB helper を追加し、event-conditioned analytics 4 module の duplicated helper を除去した。
- 2026-04-18: 既存の monkeypatch ベース lock fallback test を壊さないよう、concrete module 側には `_connect_duckdb` と `_open_analysis_connection` の互換 wrapper を残した。
- 2026-04-18: `write_payload_research_bundle` / `load_payload_research_bundle` を追加し、event-conditioned analytics 4 module の bundle write/load boilerplate を共通化した。

## 補足
- 親 issue: `bt-060`
- 次の候補: latest bundle resolution helper / plot publication hook
