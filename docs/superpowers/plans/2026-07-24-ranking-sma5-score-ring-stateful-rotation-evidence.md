# Daily Ranking SMA5 Score-Ring Stateful Rotation Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** X2 / X3 / X4発生日にhealthy basketへ一度だけ持ち替え、各targetの次triggerまでの複数日累積returnを同期間のsource継続returnとpaired比較する。

**Architecture:** 前回の1日rotation研究からMarket v5 feature panel、baseline position、source event定義を再利用する。新しい小さなdomain moduleでtarget episodeを前計算し、source eventと同日joinしてevent-level等ウェイト差を集計する。

**Tech Stack:** Python 3.12、pandas、NumPy、DuckDB、pytest

## Global Constraints

- 個人研究として単純さと実行速度を優先し、新しい汎用engineを作らない。
- 共通validator、Market DB、strategy、API、UIを変更しない。
- Sourceは`E0_no_sma5_filter` / 60-session baseline trade内の最初のX2/X3/X4 event。
- Source triggerの優先順位はX4 → X3 → X2。
- Targetは同日・同一ringでX2/X3/X4すべて非該当。source自身を除外する。
- Targetは次trigger、ring exit、60-session、terminalの最初まで保有する。
- Source counterfactualは各targetと同じ終了日まで保有する。
- Rotationはone-hopだけ。Costは開始日に一度だけ`0 / 10 / 20 bps`を控除する。
- 全結果はexploratoryとし、recursive portfolioやproduction promotionを追加しない。

---

### Task 1: Stateful paired episode evidence and runner

**Files:**
- Create: `apps/bt/src/domains/analytics/ranking_sma5_score_ring_stateful_rotation_evidence.py`
- Create: `apps/bt/scripts/research/run_ranking_sma5_score_ring_stateful_rotation_evidence.py`
- Create: `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_score_ring_stateful_rotation_evidence.py`

**Interfaces:**
- Consumes: `run_ranking_sma5_score_ring_hard_filter_research()`のfeature panel
- Consumes: 既存rotation moduleと同じbaseline state/source trigger semantics
- Produces: `run_ranking_sma5_score_ring_stateful_rotation_research(...) -> RankingSma5ScoreRingStatefulRotationResult`
- Produces: six-table immutable research bundle

- [ ] **Step 1: Write failing target episode tests**

小さなfixtureでhealthy targetが開始日の次session以降に、最初のtriggerで終了する
ことを固定する。

```python
def test_target_episode_stops_at_first_trigger_and_matches_source_horizon() -> None:
    result = build_stateful_rotation_evidence(feature_df, ring_ids=("core_high_high",))
    event = result.stateful_rotation_event_df.iloc[0]
    assert event["source_trigger_id"] == "X2_count_le_1"
    assert event["median_holding_sessions"] == 3
    assert event["gross_event_paired_delta"] == pytest.approx(
        event["mean_target_cumulative_return"]
        - event["mean_matched_source_cumulative_return"]
    )
    reasons = result.stateful_rotation_exit_reason_df
    assert reasons["target_exit_reason"].tolist() == ["X4_atr20_below_le_neg1"]
```

追加fixtureでtrigger不成立時のring exit、60-session、terminal exitを確認する。

- [ ] **Step 2: Run tests and confirm RED**

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_ranking_sma5_score_ring_stateful_rotation_evidence.py -q
```

Expected: module import failure.

- [ ] **Step 3: Implement source events and target episode preprocessing**

既存と同じbaseline framesをringごとに1回作り、baseline trade内のfirst triggerを
source eventにする。Target episodeはcodeごとに日付順で前計算し、開始日より後の
最初のX4 / X3 / X2 / ring exitを探す。同時成立時はX4 → X3 → X2とする。

```python
def target_exit_reason(row: pd.Series, *, in_ring: bool) -> str | None:
    if not in_ring:
        return "ring_exit"
    if float(row["sma5_atr20_deviation"]) <= -1.0:
        return "X4_atr20_below_le_neg1"
    if float(row["sma5_below_streak"]) >= 3.0:
        return "X3_below_streak_ge_3"
    if float(row["sma5_above_count_5d"]) <= 1.0:
        return "X2_count_le_1"
    return None
```

Target開始日自身はhealthyなのでexit判定せず、翌sessionから最大60sessionまでを
見る。開始・終了Closeが有限で、code行がglobal market sessionに連続している
episodeだけを有効にする。

- [ ] **Step 4: Build paired sleeves and event-level aggregation**

Source eventと同日のhealthy targetsをjoinし、target終了日Closeを使って次を
計算する。

```python
pair["target_cumulative_return"] = pair["target_exit_close"] / pair["target_start_close"] - 1.0
pair["matched_source_cumulative_return"] = pair["source_exit_close"] / pair["source_start_close"] - 1.0
pair["gross_pair_delta"] = (
    pair["target_cumulative_return"] - pair["matched_source_cumulative_return"]
)
```

Sourceにtarget終了日のCloseがないpairは除外する。同一source event内で有効pairを
等ウェイト平均して`gross_event_paired_delta`を作る。Costはevent aggregateへ
一度だけ控除する。

- [ ] **Step 5: Build exactly six evidence tables**

- `stateful_rotation_summary_df`
- `stateful_rotation_annual_df`
- `stateful_rotation_exit_reason_df`
- `stateful_rotation_decision_df`
- `stateful_rotation_event_df`
- `coverage_diagnostics_df`

Summaryにはring、source trigger、cost、event数、target数、holding sessions、
source/target累積return、paired mean/median、positive event rateを含める。

DecisionはCore 10bps median > 0、positive event rate > 50%、positive yearが
過半数、Near1/Near2 10bps median >= 0、Core 20bps median >= 0をすべて要求する。

- [ ] **Step 6: Add cost, equal-weight, and decision tests**

```python
def test_cost_is_applied_once_after_equal_weight_event_aggregation() -> None:
    gross = result.stateful_rotation_summary_df.query("fee_bps == 0").iloc[0]
    net10 = result.stateful_rotation_summary_df.query("fee_bps == 10").iloc[0]
    assert net10["median_event_paired_delta"] == pytest.approx(
        gross["median_event_paired_delta"] - 0.001
    )
```

Target数が異なる2 eventを用意し、pair数ではなくsource eventを同じ重みで集計する
ことも確認する。

- [ ] **Step 7: Implement the small runner**

Defaults:

```python
start_date = "2018-01-01"
end_date = "2026-07-21"
run_id = "20260724_prime_v5_sma5_score_ring_stateful_rotation_v1"
```

Manifest metadata:

```python
{
    "execution_policy": "same_close_one_hop_stateful_rotation",
    "execution_is_optimistic": True,
    "holding_cap": 60,
    "cost_levels_bps": [0, 10, 20],
    "research_status": "exploratory",
}
```

- [ ] **Step 8: Run focused verification and commit**

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_ranking_sma5_score_ring_stateful_rotation_evidence.py \
  tests/unit/domains/analytics/test_ranking_sma5_score_ring_rotation_evidence.py -q
uv run --directory apps/bt ruff check \
  src/domains/analytics/ranking_sma5_score_ring_stateful_rotation_evidence.py \
  scripts/research/run_ranking_sma5_score_ring_stateful_rotation_evidence.py \
  tests/unit/domains/analytics/test_ranking_sma5_score_ring_stateful_rotation_evidence.py
uv run --directory apps/bt pyright \
  src/domains/analytics/ranking_sma5_score_ring_stateful_rotation_evidence.py \
  scripts/research/run_ranking_sma5_score_ring_stateful_rotation_evidence.py
```

```bash
git add \
  apps/bt/src/domains/analytics/ranking_sma5_score_ring_stateful_rotation_evidence.py \
  apps/bt/scripts/research/run_ranking_sma5_score_ring_stateful_rotation_evidence.py \
  apps/bt/tests/unit/domains/analytics/test_ranking_sma5_score_ring_stateful_rotation_evidence.py
git commit -m "feat(bt): add SMA5 stateful rotation research"
```

### Task 2: Canonical run and concise publication

**Files:**
- Create: `apps/bt/docs/experiments/market-behavior/ranking-sma5-score-ring-stateful-rotation-evidence/README.md`
- Modify: `apps/bt/docs/experiments/README.md`
- Modify: `apps/bt/docs/experiments/research-catalog-metadata.toml`

**Interfaces:**
- Consumes: Task 1 runner
- Produces: canonical bundle and Japanese Published Readout

- [ ] **Step 1: Run canonical research**

```bash
uv run --directory apps/bt python \
  scripts/research/run_ranking_sma5_score_ring_stateful_rotation_evidence.py \
  --db-path ~/.local/share/trading25/market-timeseries/market.duckdb \
  --start-date 2018-01-01 \
  --end-date 2026-07-21 \
  --run-id 20260724_prime_v5_sma5_score_ring_stateful_rotation_v1
```

- [ ] **Step 2: Independently inspect the six tables**

From `stateful_rotation_event_df`, recompute each event's target/source cumulative
return, equal-weight paired delta, and 0/10/20bps summary. Verify Core、Near1、
Near2、annual direction、exit reasons、holding days、decision rows.

- [ ] **Step 3: Publish concise Japanese readout**

Report X2 / X3 / X4 separately:

- target availabilityとmedian holding sessions
- Core 0/10/20bps event-level paired median
- positive event rateとpositive years
- Near1/Near2 direction
- `stateful_rotation_candidate` / `insufficient_evidence`
- 1日rotation結果との違い
- optimistic same-Close、overlapping episodes、non-independent holdout、
  capacity caveats

- [ ] **Step 4: Run minimal verification and commit**

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_ranking_sma5_score_ring_stateful_rotation_evidence.py -q
python3 scripts/check-research-guardrails.py
git diff --check
```

```bash
git add \
  apps/bt/docs/experiments/market-behavior/ranking-sma5-score-ring-stateful-rotation-evidence/README.md \
  apps/bt/docs/experiments/README.md \
  apps/bt/docs/experiments/research-catalog-metadata.toml
git commit -m "docs(bt): publish SMA5 stateful rotation evidence"
```
