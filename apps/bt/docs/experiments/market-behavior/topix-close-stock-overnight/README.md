# TOPIX Close / Stock Overnight

## Published Readout

### Decision
- PIT-safe rerun completed. 旧 baseline の universe-policy 未確認 headline は撤回し、`20260608_pit_safe_topix500` の結果で置き換える。

### Why This Research Was Run
- TOPIX close の大きな変動後、翌営業日寄りでどの銘柄群が相対的に強いかを、PIT-safe な membership で再検証する。
- 旧 readout は `TOPIX500` / `PRIME ex TOPIX500` の universe source が明示されていなかったため、exact `index_membership_daily` rerun で置き換える。

### Data Scope / PIT Assumptions
- Run ID: `20260608_pit_safe_topix500`
- Analysis range: `2016-05-18 -> 2026-06-04`
- Market schema version: `3`
- Universe source: `stock_master_daily,index_membership_daily`
- As-of policy: signal-date membership。`TOPIX500` は `index_membership_daily.index_code = TOPIX500` の exact-date join、`PRIME ex TOPIX500` は signal-date Prime から同日 TOPIX500 membership を除外する。
- Latest snapshot fallback: not allowed. runner は schema v3 または対象 signal date の TOPIX500 membership が欠ける場合に失敗する。

### Main Findings
#### 結論: TOPIX 大幅安後は TOPIX100 / TOPIX500 の翌朝 rebound が残る

| Slice | Days | Strongest group | Mean overnight return | Up ratio |
| --- | ---:| --- | ---:| ---:|
| `close <= -2.31%` | 52 | TOPIX100 | `+0.1366%` | `53.81%` |
| `-2.31% < close <= -1.16%` | 217 | TOPIX100 | `+0.0757%` | `52.78%` |
| `-1.16% < close < 1.16%` | 1887 | PRIME ex TOPIX500 | `+0.0691%` | `45.51%` |
| `1.16% <= close < 2.31%` | 255 | TOPIX100 | `+0.1170%` | `52.24%` |
| `close >= 2.31%` | 43 | TOPIX100 | `-0.0360%` | `50.66%` |

#### 結論: `+1σ..+2σ` は continuation、`>= +2σ` は翌朝 continuation ではない

| Metric | Value |
| --- | ---:|
| TOPIX close sample count | `2454` |
| TOPIX close mean / std | `+0.0509% / 1.1570%` |
| Neutral bucket days | `1887` |
| Positive +1σ..+2σ days | `255` |
| Positive >= +2σ days | `43` |

### Interpretation
- 旧 readout の「`+1σ..+2σ` は強く、`>= +2σ` は反転」という方向性は、PIT-safe rerun でも残った。ただし新しい exact membership では平均 overnight return の大きさは小さく、旧 `+0.89%` headline は採用しない。
- 大幅安後は TOPIX100 / TOPIX500 が相対的に強い。これは crash 後の大型 rebound diagnostic として残せる。
- neutral bucket は大半を占め、PRIME ex TOPIX500 の日常 drift はあるが、up ratio は低く、単独 signal としては弱い。

### Production Implication
- market-state 補助 signal として、`close <= -1σ` の大型 rebound と `+1σ..+2σ` の continuation を候補に残す。
- `close >= +2σ` は翌朝 continuation ではなく、reversion / exhaustion 側の条件として扱う。

### Caveats
- overnight return は event close -> next open で、寄り付き約定、流動性、先物/ADR 情報は未評価。
- `TOPIX100` は signal-date `stock_master_daily.scale_category` 由来。`TOPIX500` / `PRIME ex TOPIX500` headline は exact `index_membership_daily` rerun の結果だけを使う。
- 旧 baseline の数値は下の既存セクションに残るが、この `Published Readout` より優先しない。

### Source Artifacts
- Experiment: `market-behavior/topix-close-stock-overnight`
- Runner: `apps/bt/scripts/research/run_topix_close_stock_overnight_distribution.py`
- Domain logic: `apps/bt/src/domains/analytics/topix_close_stock_overnight_distribution.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix-close-stock-overnight-distribution/20260608_pit_safe_topix500/`
- Bundle tables: `summary_df`, `day_counts_df`, `daily_group_returns_df`
- `summary.json` / legacy digest fields are intentionally not used as publication evidence.

TOPIX の当日引け変動を event day として bucket 化し、個別銘柄群の翌営業日 overnight リターンを観察する実験です。

## Purpose

- `TOPIX が大きく動いて引けた日` の翌朝に、どの銘柄群が相対的に強いかを把握する。
- 単純な bucket 平均だけでなく、`どの bucket がどれくらい頻繁に起きるか` を含めて解釈する。

## Scope

- Event definition:
  - `topix_close_return = (topix_close - prev_topix_close) / prev_topix_close`
- Trade definition:
  - `stock_overnight_return = (next_open - event_close) / event_close`
- Stock groups:
  - `PRIME`
  - `TOPIX100`
  - `TOPIX500`
  - `PRIME ex TOPIX500`

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix_close_stock_overnight_distribution.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix_close_stock_overnight_distribution.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_topix_close_stock_overnight_distribution.py`

## Latest Baseline

- [baseline-2026-03-24.md](./baseline-2026-03-24.md)

## Current Read

- 現行 baseline の TOPIX close 標準偏差は `1.1599%` で、bucket は `±1σ = 1.16%`, `±2σ = 2.32%` を使う。
- `-1.16% < TOPIX close < 1.16%` が 76.95% を占めるため、頻度加重ではこの中立帯の性格が全体像を最も強く決める。
- `TOPIX100` は `1.16% <= TOPIX close < 2.32%` の翌朝が最も強く、平均 overnight return は `0.8929%`、頻度加重後でも全 group で首位。
- ただし `TOPIX close >= 2.32%` まで行くと全 group で翌朝平均がマイナスに反転し、continuation は `+1σ ~ +2σ` 帯に集中している。
- `PRIME ex TOPIX500` は最頻出の中立帯で最も強く、極端日の当たり外れより日常的な drift の積み上がりで効く。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix_close_stock_overnight_distribution.py \
  --sample-size 0
```

この command は
`~/.local/share/trading25/research/market-behavior/topix-close-stock-overnight/<run_id>/`
へ bundle を保存します。

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- `TOPIX close >= +2σ` の翌朝は、指数の follow-through ではなく reversion 前提に切り替えるべきか。
- `+1σ ~ +2σ` の continuation と `>= +2σ` の反転を分ける条件を、出来高・先物・NT 倍率で説明できるか。
- セクターや出来高など、market regime 以外の条件を重ねると優位性が sharpen するか。
