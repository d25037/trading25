# Daily Ranking SMA5 Score-Ring Rotation Evidence Design

## Purpose

Value × Long Hybrid の既存 score ring 内で、保有銘柄が X2 / X3 / X4 に
到達したとき、cash exit や保有継続よりも、同日中に健全な同一 ring 銘柄へ
持ち替える方が良いかを検証する。

これは個人研究の exploratory follow-up とし、共通 validator、Market DB、
strategy、API、UI は変更しない。

## Frozen Comparison

- Source event:
  - `E0_no_sma5_filter` / 60-session cap の score ring baseline position を保有中
  - 当日 Close 時点で次のいずれかが初めて exit trigger になる日
    - X2: `sma5_above_count_5d <= 1`
    - X3: `Close < SMA5` が3営業日連続
    - X4: `sma5_atr20_deviation <= -1`
- Target basket:
  - source と同じ日・同じ score ring
  - X2 / X3 / X4 のすべてに非該当
  - source 銘柄自身を除外
- Outcome:
  - source: 当日 Close から翌営業日 Close まで保有継続
  - rotation: target basket の同区間等ウェイト return
  - paired delta: `rotation return - source return`
- Execution cost:
  - `0 / 10 / 20 bps` を rotation 側だけに控除

## Scope

- Primary: `core_high_high`
- Robustness: `near_high_high_1`, `near_high_high_2`
- X2 / X3 / X4 は別々に集計する
- 期間は既存 canonical run と同じ `2018-01-01..2026-07-21`
- 既に holdout を観測済みのため、全結果を exploratory と明記する

## Outputs

各 ring × trigger について次だけを出す。

- source event 数
- target availability
- target basket size
- source / rotation return の mean・median
- paired delta の mean・median
- rotation outperform rate
- 0 / 10 / 20bps 後の paired delta
- 暦年別の paired delta

結果は小さな research table と日本語 Published Readout に保存する。新しい
汎用 framework、optimization、bootstrap gate、production promotion は追加しない。

## Decision Rule

次を満たす trigger だけを「rotation 候補」とする。

- Core の 10bps 後 paired median delta が正
- rotation outperform rate が 50% 超
- 暦年の過半数で paired median delta が正
- `near_high_high_1` / `near_high_high_2` の10bps後 paired median delta も
  0以上
- 20bps 後も paired median delta が負へ反転しない

条件を満たしても production rule にはせず、個人運用上の持ち替え候補として
記録する。

## Verification

- source event と target basket の小さな unit test
- source 自身と X2 / X3 / X4 該当 target が除外されること
- rotation cost が rotation 側だけに一度控除されること
- canonical Market v5 run と結果表の read-only 照合
