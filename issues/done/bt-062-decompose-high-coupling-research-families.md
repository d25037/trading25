---
id: bt-062
title: "high-coupling research family を段階分解する"
status: done
priority: high
labels: [bt, research, refactor, analytics]
project: bt
created: 2026-04-18
updated: 2026-04-19
depends_on: [bt-061]
blocks: [bt-063]
parent: bt-060
---

# bt-062 high-coupling research family を段階分解する

## 目的
- `topix100_streak_*` と event-conditioned analytics の責務を family 単位で分割し、巨大 module の変更コストを下げる。
- data access / feature panel / model / walkforward / sampling / report shaping の境界を family 内で明確にする。

## 受け入れ条件
- [x] `topix100_streak_*` の分割先が family 内で合意され、最初の extraction が入っている。
- [x] event-conditioned analytics の `event filter / outcome builder / sampling / report shaping` 分離が少なくとも 1 family で実施されている。
- [x] family ごとの shared helper 依存が concrete study より上位へ寄っている。

## 実施内容
- [x] TOPIX100 streak / LightGBM chain を `data access / feature panel / model / walkforward / publication` に分ける。
- [x] event-conditioned analytics を `event filter / outcome builder / sampling / report shaping` に分ける。
- [x] family ごとの最小 public entrypoint を runner / bundle writer / bundle loader に揃える。

## 結果
- `apps/bt/src/domains/analytics/topix100_streak_lightgbm_feature_panel.py` を追加し、TOPIX100 streak LightGBM family の shared feature-panel helper を新設した。
- `topix100_streak_353_signal_score_lightgbm.py` と `topix100_streak_353_next_session_intraday_lightgbm.py` の price feature build / state panel coercion / price-state join を shared helper 経由へ移した。
- 既存の `_build_price_feature_frame` / `_coerce_*_state_panel_df` は wrapper として残し、family 内の他 study へ段階展開しやすい形にした。
- `topix100_streak_353_next_session_open_to_open_5d_lightgbm.py` / `topix100_streak_353_next_session_open_to_close_5d_lightgbm.py` / `topix100_streak_353_next_session_open_to_close_10d_lightgbm.py` の swing target builder も shared helper 直結へ移し、signal score module への内部依存を浅くした。
- `topix100_streak_lightgbm_feature_panel.py` に snapshot feature builder と recent-date slicer も集約し、`signal_score` / `intraday` の wrapper を残したまま swing runtime snapshot modules の concrete-module 依存をさらに減らした。
- `topix100_streak_lightgbm_validation_support.py` を追加し、`DEFAULT_TOP_K_VALUES` / baseline selector key / score decile shaping を shared support へ移した。walkforward modules は intraday concrete module ではなく shared support と `topix_streak_extreme_mode` の formatter を直接参照するように整理した。
- `topix100_1330_entry_next_1045_exit_conditioning_support.py` を追加し、1330 conditioning family の `event/outcome builder` と `segment aggregation` を family-local support へ切り出した。main research module は orchestration / bundle / plot を残しつつ support helper を読む形に寄せた。
- `topix100_1330_entry_next_1045_exit_conditioning_support.py` に published summary / bundle summary markdown builder も移し、1330 conditioning family の report shaping を concrete study module から外した。bundle writer は payload (`metadata` / `tables`) を support helper へ渡すだけの形に整理した。
- 1330 conditioning の bundle roundtrip test は `summary.md` / `summary.json` まで検証するよう広げ、publication helper の抽出後も artifact 契約が保たれることを固定した。
- 検証: `UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt ruff check ...`, `UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt pyright ...`, `UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt pytest apps/bt/tests/unit/domains/analytics/test_topix100_streak_353_signal_score_lightgbm.py apps/bt/tests/unit/domains/analytics/test_topix100_streak_353_next_session_intraday_lightgbm.py`
- 追加検証: `UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt pytest apps/bt/tests/unit/domains/analytics/test_topix100_streak_353_next_session_open_to_close_swing_targets.py`
- walkforward 追加検証: `UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt pytest apps/bt/tests/unit/domains/analytics/test_topix100_streak_353_next_session_intraday_lightgbm_walkforward.py apps/bt/tests/unit/scripts/test_run_topix100_streak_353_next_session_intraday_lightgbm_walkforward.py apps/bt/tests/unit/scripts/test_run_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward.py`
- 1330 conditioning 追加検証: `UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt pytest apps/bt/tests/unit/domains/analytics/test_topix100_1330_entry_next_1045_exit_conditioning.py apps/bt/tests/unit/scripts/test_run_topix100_1330_entry_next_1045_exit_conditioning.py`
- 1330 conditioning publication helper 追加検証: `UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt ruff check apps/bt/src/domains/analytics/topix100_1330_entry_next_1045_exit_conditioning_support.py apps/bt/src/domains/analytics/topix100_1330_entry_next_1045_exit_conditioning.py apps/bt/tests/unit/domains/analytics/test_topix100_1330_entry_next_1045_exit_conditioning.py apps/bt/tests/unit/scripts/test_run_topix100_1330_entry_next_1045_exit_conditioning.py`, `UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt pyright apps/bt/src/domains/analytics/topix100_1330_entry_next_1045_exit_conditioning_support.py apps/bt/src/domains/analytics/topix100_1330_entry_next_1045_exit_conditioning.py`, `UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt pytest apps/bt/tests/unit/domains/analytics/test_topix100_1330_entry_next_1045_exit_conditioning.py apps/bt/tests/unit/scripts/test_run_topix100_1330_entry_next_1045_exit_conditioning.py`

## 補足
- 親 issue: `bt-060`
- 依存: `bt-061`
