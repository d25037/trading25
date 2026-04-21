# TOPIX100 Streak Point-in-Time Audit

Date: `2026-04-10`

## Summary

TOPIX100 streak family には、tradeable な stock-level signal research としては無効になる
future-dependent contamination が 2 系統あった。

1. Historical snapshot runtime bug
2. Research feature panel design bug

特に 2 は refit cadence に依存しない。`cadence=1` でも `5` でも `20` でも `63` でも
`126` でも、**入力される daily stock panel 自体が future-conditioned** だったため、
同じ leak を含んでいた。

この整理後、残す canonical study は次の 1 本だけとする。

- `TOPIX100 Streak 3/53 Next-Session Intraday LightGBM Walk-Forward`

派生研究は削除し、before/after の比較はこの canonical study に集約する。

## Before / After

同じ `train 756 / test 126 / step 126` の `LightGBM / pair_50_50` で比較すると、
leak 除去前後の差は以下の通りだった。

| Book | Leak あり Before | Leak 除去 After |
|---|---:|---:|
| `Top1 / Bottom1` avg/day | `+1.08%` | `-0.02%` |
| `Top1 / Bottom1` positive day rate | `88.82%` | `49.54%` |
| `Top1 / Bottom1` max DD | `-2.39%` | `-54.24%` |
| `Top1 / Bottom1` Sharpe | `16.19` | `-0.20` |
| `Top3 / Bottom3` avg/day | `+0.95%` | `+0.02%` |
| `Top3 / Bottom3` positive day rate | `95.17%` | `52.05%` |
| `Top3 / Bottom3` max DD | `-1.81%` | `-28.64%` |
| `Top3 / Bottom3` Sharpe | `22.11` | `0.47` |
| `Top5 / Bottom5` avg/day | `+0.85%` | `+0.03%` |
| `Top5 / Bottom5` positive day rate | `96.16%` | `52.38%` |
| `Top5 / Bottom5` max DD | `-1.56%` | `-23.06%` |
| `Top5 / Bottom5` Sharpe | `24.33` | `0.83` |

要するに、contaminated bundle が示していた

- `+0.95%/day`
- `95% positive-day rate`
- `-1.8% max drawdown`

のような異常な強さは、point-in-time 化すると消えた。

参照 bundle:

- Before: `20260407_143906_acbf9bc0`
- After: `20260410_ptit_wf126`

## Root Cause 1: Historical Snapshot Runtime Bug

Historical date `X` の ranking / snapshot 計算で、

- stock history query が `history <= X` に切られていなかった
- feature enrichment も `analysis_end_date=X` で固定されていなかった

そのため `X+1` の OHLCV が DB に入ったあとに同じ `X` を再計算すると、
`Top/Bottom` ranking が変わった。

これは「過去日の replay が、当時知らないはずの将来データに依存する」純粋な
future leak だった。

### Runtime symptom

- `2026-04-09` を `2026-04-10` の OHLCV が無い状態で計算した ranking
- `2026-04-10` の OHLCV 取得後に同じ `2026-04-09` を再計算した ranking

が一致しなかった。

### Runtime fix

Historical snapshot runtime は、

- `history_df` query を `end_date=target_date`
- feature/state enrichment も `analysis_end_date=target_date`

に固定した。

対象の主経路:

- [topix100_streak_353_next_session_intraday_lightgbm.py](../apps/bt/src/domains/analytics/topix100_streak_353_next_session_intraday_lightgbm.py)
- [ranking_service.py](../apps/bt/src/application/services/ranking_service.py)

## Root Cause 2: Research Feature Panel Design Bug

旧 streak research は、daily stock panel を作るときに
`state_event_df` / `state_horizon_event_df` を `segment_end_date` で join していた。

これは tradeable な daily signal SoT としては不正だった。

### Why it leaked

`segment_end_date=t` という row は、翌日 `t+1` も同じ streak が続いた瞬間に
`segment_end_date=t+1` へ伸びる。

つまり、`date=t` の stock row が

- ある日には存在する
- 将来日を足すと消える

という future-dependent な挙動になる。

その結果、`date=t` の cross-sectional ranking が `t+1` の存在で変わる。

### Concrete failure mode

旧 intraday research では、ある signal date の daily universe が

- 平均 `~51` 銘柄
- 最小 `2` 銘柄
- 最大 `96` 銘柄

しかなく、TOPIX100 daily ranking になっていなかった。

point-in-time snapshot では `100` 銘柄ある日でも、
旧 batch research では future streak extension の影響で行が消え、
Bottom3 / Top3 が別物になっていた。

## Why Cadence Did Not Save It

今回の contamination は **model refit schedule より前段** にある。

問題は

- `cadence=1` で毎日再学習したか
- `cadence=20` や `126` で block 固定したか

ではない。

問題は、そのどの cadence でも共通に使う
`date x code` feature panel が future-conditioned だったこと。

したがって、

- `cadence=1`
- `cadence=5`
- `cadence=20`
- `cadence=63`
- `cadence=126`

の全てで leak していた。

cadence は model fitting cadence の違いに過ぎず、入力 universe / state row が
将来依存なら、すべて contaminated になる。

## Research That Became Invalid

少なくとも、stock-level daily signal として `state_event_df` / `state_horizon_event_df`
を join していた研究は invalid 扱いにする。

主な対象:

- `topix100_streak_353_signal_score_lightgbm`
- `topix100_streak_353_signal_score_lightgbm_walkforward`
- `topix100_streak_353_next_session_intraday_lightgbm`
- `topix100_streak_353_next_session_intraday_lightgbm_walkforward`
- `topix100_streak_353_next_session_intraday_refit_cadence_ablation`
- `topix100_streak_353_next_session_intraday_train_window_ablation`
- `topix100_streak_353_next_session_intraday_discrete_ablation_walkforward`
- `topix100_streak_353_next_session_intraday_portfolio_construction_walkforward`
- `topix100_strongest_setup_q10_threshold`
- `topix100_streak_353_multivariate_priority`
- `topix100_short_side_streak_353_scan`
- `topix100_q10_bounce_streak_353_conditioning`

この cleanup では、上記の派生研究 surface は削除対象とし、
canonical な original intraday walk-forward だけを残す。

## Transfer Research Status

`topix100_streak_353_transfer` 自体は retrospective event study としては残せる。
ただし、その `state_event_df` / `state_horizon_event_df` を
tradeable daily signal panel の SoT に流用してはいけない。

要するに、

- retrospective explanation tool としては可
- stock-level daily selection research の SoT としては不可

である。

## Implemented Fix

新しい SoT は daily point-in-time state panel:

- [topix100_streak_353_transfer.py](../apps/bt/src/domains/analytics/topix100_streak_353_transfer.py)
  - `build_topix100_streak_daily_state_panel_df(...)`
  - `build_topix100_streak_state_snapshot_df(...)`

この panel は各 `date, code` について `history <= date` だけを使い、
その日までに観測可能な current streak state を日次で生成する。

これに載せ替えた研究群:

- `signal_score_lightgbm`
- `next_session_intraday_lightgbm`
- `walkforward`
- `refit cadence ablation`
- `train window ablation`
- `strongest setup`
- `multivariate priority`
- `short-side scan`
- `q10 conditioning`
- `intraday discrete ablation`
- `intraday portfolio construction`

## Required Follow-Up

1. 旧 bundle は SoT として使わない
2. before/after の比較は canonical walk-forward だけで行う
3. runtime と research で同じ daily point-in-time SoT を使い続ける

## Decision Rule Going Forward

streak 系で stock-level daily signal を作るときは、

- `segment_end_date` based event row を daily ranking に join しない
- `date=t` の row は `history <= t` だけで再現できなければならない
- 同じ `target_date` は将来 OHLCV が追加されても変わってはいけない

この 3 条件を満たさない研究は、tradeable signal research として採用しない。
