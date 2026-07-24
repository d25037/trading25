# Daily Ranking SMA5 Score-Ring Stateful Rotation Evidence Design

## Purpose

Value × Long Hybrid score ring の保有銘柄が X2 / X3 / X4 に到達したとき、
同日healthy basketへ一度だけ持ち替え、各targetの次triggerまで保有した累積
returnが、同期間sourceを持ち続けるより良いかをpaired比較する。

前回の1日rotation研究を複数日へ延長するexploratory follow-upであり、
recursive portfolio、共通validator、Market DB、strategy、API、UIは変更しない。

## Source Event

- Baseline position: `E0_no_sma5_filter`、60-session cap
- Score rings:
  - Primary: `core_high_high`
  - Robustness: `near_high_high_1`、`near_high_high_2`
- 保有中にX2 / X3 / X4のいずれかが初めて成立した当日Closeを起点にする。
- 同時成立時の分類優先順位は既存研究と同じX4 → X3 → X2とする。

Trigger定義:

- X2: `sma5_above_count_5d <= 1`
- X3: `Close < SMA5`が3営業日連続
- X4: `sma5_atr20_deviation <= -1`

## Target and Holding State

Targetはsource eventと同じ日・同じscore ringに存在し、X2 / X3 / X4の
すべてに非該当する銘柄とする。source自身を除外し、結果を見た追加rankingや
銘柄選択は行わない。

各target sleeveは当日Closeで取得し、次の最初の日のCloseまで保有する。

1. X2 / X3 / X4のいずれかが成立
2. 当該score ringから離脱
3. 60営業日に到達
4. データ終端

持ち替えは一度だけとし、target exit後の再rotationは行わない。

## Paired Counterfactual

各targetについて、sourceもtargetと同じ終了日まで継続保有したcounterfactualを
作る。

```text
gross paired delta
= target cumulative Close-to-Close return
 - source cumulative Close-to-Close return

net paired delta
= gross paired delta
 - rotation cost
```

Costはrotation当日に一度だけ`0 / 10 / 20 bps`を控除する。sourceとtargetの
両方に同じ開始日・終了日を使い、期間差を混ぜない。途中のsource triggerは
counterfactualでは無視する。

## Aggregation

同じsource eventのhealthy targetsを等ウェイトsleeveとして扱い、
targetごとのpaired deltaを平均してevent-level paired deltaを作る。

X2 / X3 / X4 × ring × costについて次を出す。

- source event数とtarget availability
- target数と保有日数
- source / target累積returnのmean・median
- event-level paired deltaのmean・median
- paired deltaが正のevent比率
- 暦年別event-level paired delta
- target exit reason構成

## Decision Rule

次をすべて満たすtriggerだけを`stateful_rotation_candidate`とする。

- Core・10bps後のevent-level paired median deltaが正
- Core・10bps後のpositive event rateが50%超
- Core・10bps後のpositive yearが過半数
- `near_high_high_1` / `near_high_high_2`の10bps後medianが0以上
- Core・20bps後medianが0以上

候補になってもproduction ruleにはせず、recursive portfolio研究へ進める
価値があるという判断に限定する。

## Outputs

小さなrunner-first bundleに次の表だけを保存する。

- `stateful_rotation_summary_df`
- `stateful_rotation_annual_df`
- `stateful_rotation_exit_reason_df`
- `stateful_rotation_decision_df`
- `stateful_rotation_event_df`
- `coverage_diagnostics_df`

日本語Published ReadoutにはX2 / X3 / X4のCore 10bps、Near robustness、
20bps stress、保有日数、判定を簡潔に記載する。

## Caveats

- Signal、target選択、rotationは同じCloseを使うoptimistic proxy。
- Source eventやtarget episodeは重複しうるため、実portfolio NAVではない。
- 既に2025年以降を観測済みで、独立holdoutではない。
- 等ウェイト、流動性、capacity、market impact、既存保有との重複は未評価。

## Verification

- X4 → X3 → X2 precedence
- healthy target除外条件
- targetの最初のtrigger / ring exit / 60-session / terminal exit
- sourceとtargetの開始日・終了日一致
- costがeventに一度だけ適用されること
- event-level等ウェイト集計
- canonical bundle値のread-only再計算

