---
id: bt-060
title: "research cleanup program を phase で管理する"
status: in-progress
priority: high
labels: [bt, research, refactor, workflow, testing]
project: bt
created: 2026-04-18
updated: 2026-04-18
depends_on: []
blocks: []
parent: null
---

# bt-060 research cleanup program を phase で管理する

## 目的
- `apps/bt` の research code を family 単位で棚卸しし、今後の分割順と責務境界を先に固定する。
- notebook / runner / bundle / domain の guardrail を repo 内に明文化し、viewer-only や runner-first の退行を CI で検出できるようにする。
- `topix100_streak_*` と event-conditioned analytics を中心に、今後の分割を big-bang ではなく phase 単位で進められる状態にする。

## 背景
- 2026-04-18 時点で `apps/bt/src/domains/analytics` は 83 module、`apps/bt/scripts/research` は 54 file、`apps/bt/notebooks/playground` は 25 file ある。
- analytics module のうち 43 file が 1000 行超で、research 本体に bundle I/O、latest bundle 解決、notebook 向け helper、shared query helper が混在している。
- notebook は `viewer-only` が SoT だが、現状でも recompute を残す playground や、存在しない runner path を指す playground が混ざっている。
- 以前の `bt-049` / `bt-050` / `bt-051` / `bt-054` は有効だったが、その後に追加された research family までは十分に揃っていない。

## 受け入れ条件
- [x] current research family と hot spot が issue 内で整理されている。
- [x] Phase 0 として viewer-only / runner path / shared viewer helper の guardrail が repo で検証される。
- [x] current violation が修正され、guardrail が green になる。
- [x] Phase 1 以降の分解順が family 単位で明記されている。
- [x] Phase 1-3 を child issue へ分けて追跡できる。

## 実施内容
- [x] research family inventory を作る。
- [x] notebook viewer-only guardrail checker を追加し、local prepush と CI quality に接続する。
- [x] current violation を修正する。
- [ ] Phase 1: shared infra を concrete study から抽出する。
- [ ] Phase 2: high-coupling family を family 単位で分解する。
- [ ] Phase 3: notebook / publication / docs を research surface として正規化する。

## Family Inventory

### 1. TOPIX100 streak / LightGBM chain
- 主対象:
  - `topix100_streak_353_transfer.py`
  - `topix100_streak_353_signal_score_lightgbm.py`
  - `topix100_streak_353_next_session_intraday_lightgbm.py`
  - `topix100_streak_353_next_session_*_lightgbm_walkforward.py`
- 問題:
  - module 間 import が濃く、feature panel / baseline / model / walkforward / publication がまたがっている。
  - notebook 起点だった補助実装が leaf module に残っている。
- 優先度: 最優先

### 2. TOPIX rank / regime family
- 主対象:
  - `topix100_*price*_rank_future_close.py`
  - `topix100_*regime_conditioning.py`
  - `topix_rank_future_close_core.py`
  - `topix_regime_conditioning_core.py`
- 問題:
  - core 抽出は済んだが、leaf 側に report shaping / bundle helper / experiment-specific table shaping が厚く残る。
  - wrapper と published surface の整理が不均一。
- 優先度: 高

### 3. Event-conditioned stock analytics
- 主対象:
  - `topix_close_stock_overnight_distribution.py`
  - `nt_ratio_change_*`
  - `stock_intraday_overnight_share.py`
  - `topix_gap_intraday_distribution.py`
  - `topix100_open_relative_intraday_path.py`
- 問題:
  - concrete study module に shared DuckDB read helper や code normalize helper が入り込んでいる。
  - event filter / stock outcome / deterministic sampling / summary shaping の層が曖昧。
- 優先度: 高

### 4. TOPIX100 intraday / session-boundary family
- 主対象:
  - `topix100_1330_entry_next_1045_exit*.py`
  - `topix100_1445_entry_*`
  - `topix100_open_close_volume_ratio_conditioning.py`
  - `topix100_second_bar_volume_drop_performance.py`
- 問題:
  - session semantics と analytics report shaping が同じ module に集中しやすい。
  - signal family ごとの差分が spec ではなく module copy に寄っている。
- 優先度: 中

### 5. Hedge / strategy audit / forward EPS family
- 主対象:
  - `hedge_1357_nt_ratio_topix.py`
  - `production_strategy_*`
  - `forward_eps_*`
- 問題:
  - family ごとに独自 surface が増えており、bundle/publication helper が横断で揃っていない。
- 優先度: 中

## Phase Breakdown

### Phase 0
- guardrail と inventory を入れる。
- notebook は viewer-only を強制し、runner path の存在を検証する。
- current violation を修正してから次 phase に進む。

### Phase 1
- shared data access / snapshot fallback / code normalize / result codec / publication helper を concrete study から抽出する。
- concrete study を shared infra の SoT に依存させる。

### Phase 2
- `topix100_streak_*` を `data access / feature panel / model / walkforward / publication` に分ける。
- event-conditioned analytics を `event filter / outcome builder / sampling / report shaping` に分ける。

### Phase 3
- notebook / docs / research catalog の surface を揃える。
- published research の最小要件を `runner + bundle + summary + docs reference` に統一する。

## 結果
- 2026-04-18: Phase 0 として research guardrail checker を追加し、`viewer-only notebook` / `shared viewer helper` / `runner path existence` を機械検証できるようにした。
- 2026-04-18: `topix100_sma50_raw_vs_atr_q10_bounce_playground.py` の notebook recompute を除去し、viewer-only に戻した。
- 2026-04-18: `topix100_streak_353_transfer` の canonical runner を追加し、playground の runner path を実在ファイルへ揃えた。
- 2026-04-18: Phase 1 の最初の slice として `readonly_duckdb_support.py` を追加し、event-conditioned analytics 4 module に散っていた read-only DuckDB access / snapshot fallback / date range / code normalize helper を共通化した。

## 補足
- 既存の近い前提: `issues/done/bt-049-analytics-research-refactor-program.md`
- workflow SoT: `.codex/skills/bt-research-workflow/SKILL.md`
- notebook/viewer SoT: `apps/bt/src/shared/research_notebook_viewer.py`
- child issue: `bt-061` / `bt-062` / `bt-063`
