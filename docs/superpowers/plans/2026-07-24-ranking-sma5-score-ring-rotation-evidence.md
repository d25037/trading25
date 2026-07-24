# Daily Ranking SMA5 Score-Ring Rotation Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** X2 / X3 / X4 到達時に、同日・同一 score ring の健全銘柄へ持ち替える方が翌営業日 return を改善するかをpaired比較する。

**Architecture:** 既存のMarket v5 feature panelとbaseline position stateを再利用する。新しい小さなdomain moduleでsource event、healthy basket、0/10/20bps集計を作り、runnerで5表のbundleを書くだけに限定する。

**Tech Stack:** Python 3.12、pandas、DuckDB、既存VectorBT position-state adapter、pytest

## Global Constraints

- 個人研究として速度と単純さを優先し、共通validator、Market DB、strategy、API、UIを変更しない。
- Sourceは`E0_no_sma5_filter`、60-session capのbaseline position内で最初に発生するX2/X3/X4 eventとする。
- 同時成立時の分類優先順位は既存position-state研究と同じX4 → X3 → X2とする。
- Targetは同日・同一ringでX2/X3/X4のすべてに非該当の銘柄等ウェイトbasketとする。
- Outcomeは当日Close→翌営業日Close。rotation側だけに0/10/20bpsを一度控除する。
- 全結果をexploratoryとし、production promotionやbootstrap gateを追加しない。

---

### Task 1: Rotation evidence module and runner

**Files:**
- Create: `apps/bt/src/domains/analytics/ranking_sma5_score_ring_rotation_evidence.py`
- Create: `apps/bt/scripts/research/run_ranking_sma5_score_ring_rotation_evidence.py`
- Create: `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_score_ring_rotation_evidence.py`

**Interfaces:**
- Consumes: `run_ranking_sma5_score_ring_hard_filter_research()`のMarket v5 feature panel
- Consumes: `build_position_signal_frames()`の`E0_no_sma5_filter` / `X0_no_sma5_exit` baseline state
- Produces: `run_ranking_sma5_score_ring_rotation_research(...) -> RankingSma5ScoreRingRotationResult`
- Produces: `write_ranking_sma5_score_ring_rotation_bundle(...) -> ResearchBundleInfo`

- [ ] **Step 1: Write failing event and target tests**

小さな2銘柄×複数日fixtureを作り、次を固定する。

```python
def test_rotation_uses_first_trigger_and_healthy_same_ring_target() -> None:
    result = build_rotation_evidence(feature_df, ring_ids=("core_high_high",))
    events = result.rotation_event_df
    assert events["trigger_id"].tolist() == ["X4_atr20_below_le_neg1"]
    assert events["target_candidate_count"].tolist() == [1]
    assert events["target_codes"].tolist() == ["2000"]
    assert events["gross_paired_delta"].iloc[0] == pytest.approx(
        events["rotation_return"].iloc[0] - events["source_return"].iloc[0]
    )
```

同時にX2/X3/X4が成立した場合はX4に分類し、source自身、X2、X3、X4該当targetを除外する。

- [ ] **Step 2: Run the test and confirm RED**

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_ranking_sma5_score_ring_rotation_evidence.py -q
```

Expected: module import failure.

- [ ] **Step 3: Implement next-session and source-event construction**

`feature_df`へglobal next-sessionを使ったreturnを付ける。銘柄の次行が市場の翌営業日でない場合はoutcomeをNULLにする。

```python
def attach_next_session_returns(feature_df: pd.DataFrame) -> pd.DataFrame:
    frame = feature_df.sort_values(["code", "date"], kind="stable").copy()
    sessions = pd.Index(sorted(pd.to_datetime(frame["date"]).unique()))
    next_session = pd.Series(sessions[1:], index=sessions[:-1])
    frame["next_date"] = frame.groupby("code")["date"].shift(-1)
    frame["next_close"] = frame.groupby("code")["close"].shift(-1)
    expected = pd.to_datetime(frame["date"]).map(next_session)
    valid = pd.to_datetime(frame["next_date"]).eq(expected)
    frame["next_session_return"] = (
        pd.to_numeric(frame["next_close"], errors="coerce")
        / pd.to_numeric(frame["close"], errors="coerce")
        - 1.0
    ).where(valid)
    return frame
```

各ringでbaseline framesを1回だけ作る。`held_intervals`かつring内の日をbaseline trade単位で走査し、最初のtriggerだけをsource eventにする。trigger分類はX4、X3、X2の順にする。

- [ ] **Step 4: Implement healthy basket and cost aggregation**

Target maskは次で固定する。

```python
healthy = (
    in_ring
    & pd.to_numeric(frame["sma5_above_count_5d"], errors="coerce").gt(1.0)
    & pd.to_numeric(frame["sma5_below_streak"], errors="coerce").lt(3.0)
    & pd.to_numeric(frame["sma5_atr20_deviation"], errors="coerce").gt(-1.0)
    & frame["next_session_return"].notna()
)
```

日付ごとのtarget returnを等ウェイト平均し、eventへmergeする。各costについて次を作る。

```python
events["net_paired_delta"] = (
    events["rotation_return"]
    - events["source_return"]
    - fee_bps / 10_000.0
)
```

結果は以下の5表だけにする。

- `rotation_summary_df`
- `rotation_annual_df`
- `rotation_decision_df`
- `coverage_diagnostics_df`
- `rotation_event_df`

- [ ] **Step 5: Add cost and decision tests**

```python
def test_rotation_cost_is_charged_once_and_decision_uses_frozen_rules() -> None:
    summary = result.rotation_summary_df
    gross = summary.loc[summary["fee_bps"].eq(0), "median_paired_delta"].iloc[0]
    net10 = summary.loc[summary["fee_bps"].eq(10), "median_paired_delta"].iloc[0]
    assert net10 == pytest.approx(gross - 0.001)
```

Core 10bps median > 0、outperform > 50%、positive yearsが過半数、
`near_high_high_1` / `near_high_high_2`の10bps median >= 0、Core 20bps
median >= 0のときだけ`rotation_candidate`とする。

- [ ] **Step 6: Implement runner and bundle test**

Runner defaults:

```python
start_date = "2018-01-01"
end_date = "2026-07-21"
run_id = "20260724_prime_v5_sma5_score_ring_rotation_v1"
```

`write_research_bundle()`で上記5表と次のmetadataを書く。

```python
{
    "execution_policy": "same_close_rotation_next_close_evaluation",
    "execution_is_optimistic": True,
    "primary_ring": "core_high_high",
    "holding_cap": 60,
    "cost_levels_bps": [0, 10, 20],
    "research_status": "exploratory",
}
```

- [ ] **Step 7: Run focused verification and commit**

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_ranking_sma5_score_ring_rotation_evidence.py \
  tests/unit/domains/analytics/test_ranking_sma5_score_ring_hard_filter_evidence.py -q
uv run --directory apps/bt ruff check \
  src/domains/analytics/ranking_sma5_score_ring_rotation_evidence.py \
  scripts/research/run_ranking_sma5_score_ring_rotation_evidence.py \
  tests/unit/domains/analytics/test_ranking_sma5_score_ring_rotation_evidence.py
```

Expected: focused tests and Ruff pass.

```bash
git add \
  apps/bt/src/domains/analytics/ranking_sma5_score_ring_rotation_evidence.py \
  apps/bt/scripts/research/run_ranking_sma5_score_ring_rotation_evidence.py \
  apps/bt/tests/unit/domains/analytics/test_ranking_sma5_score_ring_rotation_evidence.py
git commit -m "feat(bt): add SMA5 score-ring rotation research"
```

### Task 2: Canonical run and concise publication

**Files:**
- Create: `apps/bt/docs/experiments/market-behavior/ranking-sma5-score-ring-rotation-evidence/README.md`
- Modify: `apps/bt/docs/experiments/README.md`
- Modify: `apps/bt/docs/experiments/research-catalog-metadata.toml`

**Interfaces:**
- Consumes: Task 1 runner
- Produces: canonical exploratory bundle and Japanese Published Readout

- [ ] **Step 1: Run the canonical research**

```bash
uv run --directory apps/bt python \
  scripts/research/run_ranking_sma5_score_ring_rotation_evidence.py \
  --db-path ~/.local/share/trading25/market-timeseries/market.duckdb \
  --start-date 2018-01-01 \
  --end-date 2026-07-21 \
  --run-id 20260724_prime_v5_sma5_score_ring_rotation_v1
```

- [ ] **Step 2: Inspect the five result tables**

Read `results.duckdb` and verify X2/X3/X4 separately for Core、
`near_high_high_1`、`near_high_high_2` at 0/10/20bps. Independently
recompute `rotation_return - source_return - cost` from `rotation_event_df`
before quoting metrics.

- [ ] **Step 3: Publish a concise Japanese readout**

Include:

- X2/X3/X4ごとのtarget availability
- Core 10bps paired median、outperform rate、positive years
- `near_high_high_1` / `near_high_high_2`と20bpsの方向
- `rotation_candidate` / `insufficient_evidence`
- same-Close optimistic、1日outcome、既観測holdout、capacity未評価の注意

- [ ] **Step 4: Run minimal verification and commit**

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_ranking_sma5_score_ring_rotation_evidence.py -q
python3 scripts/check-research-guardrails.py
git diff --check
```

```bash
git add \
  apps/bt/docs/experiments/market-behavior/ranking-sma5-score-ring-rotation-evidence/README.md \
  apps/bt/docs/experiments/README.md \
  apps/bt/docs/experiments/research-catalog-metadata.toml
git commit -m "docs(bt): publish SMA5 score-ring rotation evidence"
```
