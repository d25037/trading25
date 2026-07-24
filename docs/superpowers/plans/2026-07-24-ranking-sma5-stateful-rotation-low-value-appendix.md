# SMA5 Stateful Rotation Low-Value Appendix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Long Hybridを`>= 0.7`に固定し、Value上限を`0.2 / 0.3 / 0.4`へ反転した3群で既存one-hop stateful rotationを再実行する。

**Architecture:** 既存stateful episode engineは変更しない。Appendix専用adapterでValue scoreを`1 - value`へ反転し、Long Hybridを`>= 0.7`のbinary maskへ変換して既存3 ringへ渡し、出力ring名だけappendix名へ置換する。

**Tech Stack:** Python 3.12、pandas、NumPy、DuckDB、pytest

## Global Constraints

- Appendix専用の薄いadapterとrunnerだけを追加する。
- 既存stateful episode、共通validator、Market DB、strategy、API、UIを変更しない。
- Long Hybrid条件は全群で`>= 0.7`。
- Value条件はCore `<= 0.2`、Near1 `<= 0.3`、Near2 `<= 0.4`。
- Source / target / cost / episode / decision semanticsは既存stateful研究と同一。
- 結果はexploratory appendixで、production promotionを行わない。

---

### Task 1: Low-Value appendix adapter and runner

**Files:**
- Create: `apps/bt/src/domains/analytics/ranking_sma5_stateful_rotation_low_value_appendix.py`
- Create: `apps/bt/scripts/research/run_ranking_sma5_stateful_rotation_low_value_appendix.py`
- Create: `apps/bt/tests/unit/domains/analytics/test_ranking_sma5_stateful_rotation_low_value_appendix.py`

**Interfaces:**
- Consumes: `build_stateful_rotation_evidence(feature_df)`
- Produces: `build_low_value_appendix_evidence(feature_df) -> RankingSma5ScoreRingStatefulRotationResult`
- Produces: appendix six-table research bundle

- [ ] **Step 1: Write failing boundary tests**

```python
def test_appendix_ring_transform_uses_fixed_long_threshold_and_inverted_value() -> None:
    transformed = transform_low_value_appendix_scores(feature_df)
    assert transformed.loc["core_boundary", "value_composite_equal_score"] == pytest.approx(0.8)
    assert transformed.loc["near1_boundary", "value_composite_equal_score"] == pytest.approx(0.7)
    assert transformed.loc["near2_boundary", "value_composite_equal_score"] == pytest.approx(0.6)
    assert transformed.loc["long_070", "long_hybrid_leadership_score"] == 1.0
    assert transformed.loc["long_0699", "long_hybrid_leadership_score"] == 0.0
```

Value `0.2 / 0.3 / 0.4`はinclusive、Value欠損は欠損のまま、Long Hybrid欠損は
非該当になることも固定する。

- [ ] **Step 2: Run and confirm RED**

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_ranking_sma5_stateful_rotation_low_value_appendix.py -q
```

Expected: module import failure.

- [ ] **Step 3: Implement the appendix transform**

```python
APPENDIX_RING_MAP = {
    "core_high_high": "low_value_core",
    "near_high_high_1": "low_value_near1",
    "near_high_high_2": "low_value_near2",
}

def transform_low_value_appendix_scores(feature_df: pd.DataFrame) -> pd.DataFrame:
    frame = feature_df.copy()
    value = pd.to_numeric(frame["value_composite_equal_score"], errors="coerce")
    leadership = pd.to_numeric(
        frame["long_hybrid_leadership_score"], errors="coerce"
    )
    frame["value_composite_equal_score"] = 1.0 - value
    frame["long_hybrid_leadership_score"] = leadership.ge(0.7).astype(float)
    return frame
```

変換後に既存`build_stateful_rotation_evidence()`を呼び、6表の`ring_id`を
`APPENDIX_RING_MAP`で置換する。Decisionは置換前の既存Core/Near判定をそのまま
使う。

- [ ] **Step 4: Add reuse and no-mutation tests**

```python
def test_appendix_reuses_stateful_result_and_does_not_mutate_input() -> None:
    original = feature_df.copy(deep=True)
    result = build_low_value_appendix_evidence(feature_df)
    pd.testing.assert_frame_equal(feature_df, original)
    assert set(result.stateful_rotation_summary_df["ring_id"]) == {
        "low_value_core",
        "low_value_near1",
        "low_value_near2",
    }
```

FixtureでLong Hybrid `0.7`かつValue `0.2`のsource/targetがCoreへ入り、
Long Hybrid `0.6999`またはValue `0.2001`がCoreから外れることをepisode結果でも
確認する。

- [ ] **Step 5: Implement appendix runner**

Defaults:

```python
start_date = "2018-01-01"
end_date = "2026-07-21"
run_id = "20260724_prime_v5_sma5_stateful_rotation_low_value_appendix_v1"
```

Manifestに次を記録する。

```python
{
    "research_status": "exploratory_appendix",
    "long_hybrid_min": 0.7,
    "value_max_by_ring": {
        "low_value_core": 0.2,
        "low_value_near1": 0.3,
        "low_value_near2": 0.4,
    },
    "cost_levels_bps": [0, 10, 20],
}
```

- [ ] **Step 6: Run focused verification and commit**

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_ranking_sma5_stateful_rotation_low_value_appendix.py \
  tests/unit/domains/analytics/test_ranking_sma5_score_ring_stateful_rotation_evidence.py -q
uv run --directory apps/bt ruff check \
  src/domains/analytics/ranking_sma5_stateful_rotation_low_value_appendix.py \
  scripts/research/run_ranking_sma5_stateful_rotation_low_value_appendix.py \
  tests/unit/domains/analytics/test_ranking_sma5_stateful_rotation_low_value_appendix.py
uv run --directory apps/bt pyright \
  src/domains/analytics/ranking_sma5_stateful_rotation_low_value_appendix.py \
  scripts/research/run_ranking_sma5_stateful_rotation_low_value_appendix.py
```

```bash
git add \
  apps/bt/src/domains/analytics/ranking_sma5_stateful_rotation_low_value_appendix.py \
  apps/bt/scripts/research/run_ranking_sma5_stateful_rotation_low_value_appendix.py \
  apps/bt/tests/unit/domains/analytics/test_ranking_sma5_stateful_rotation_low_value_appendix.py
git commit -m "feat(bt): add low-value stateful rotation appendix"
```

### Task 2: Canonical appendix run and publication

**Files:**
- Create: `apps/bt/docs/experiments/market-behavior/ranking-sma5-stateful-rotation-low-value-appendix/README.md`
- Modify: `apps/bt/docs/experiments/README.md`
- Modify: `apps/bt/docs/experiments/research-catalog-metadata.toml`

**Interfaces:**
- Consumes: Task 1 appendix runner
- Produces: canonical appendix bundle and Japanese Published Readout

- [ ] **Step 1: Run canonical appendix**

```bash
uv run --directory apps/bt python \
  scripts/research/run_ranking_sma5_stateful_rotation_low_value_appendix.py \
  --db-path ~/.local/share/trading25/market-timeseries/market.duckdb \
  --start-date 2018-01-01 \
  --end-date 2026-07-21 \
  --run-id 20260724_prime_v5_sma5_stateful_rotation_low_value_appendix_v1
```

- [ ] **Step 2: Inspect and independently recompute**

Read the six tables and recompute all 27 summary rows from
`stateful_rotation_event_df` at 0/10/20bps. Verify annual direction、decision、
holding days、exit reasons、coverage.

- [ ] **Step 3: Publish concise appendix readout**

Report X2 / X3 / X4 separately:

- Low-Value Core 10bps median、positive rate、positive years
- Low-Value Near1 / Near2、Core 20bps
- high-Value stateful resultとの差
- `stateful_rotation_candidate` / `insufficient_evidence`
- appendix / same-Close / overlapping episode / capacity caveats

- [ ] **Step 4: Minimal verification and commit**

```bash
uv run --directory apps/bt pytest \
  tests/unit/domains/analytics/test_ranking_sma5_stateful_rotation_low_value_appendix.py -q
python3 scripts/check-research-guardrails.py
git diff --check
```

```bash
git add \
  apps/bt/docs/experiments/market-behavior/ranking-sma5-stateful-rotation-low-value-appendix/README.md \
  apps/bt/docs/experiments/README.md \
  apps/bt/docs/experiments/research-catalog-metadata.toml
git commit -m "docs(bt): publish low-value stateful rotation appendix"
```
