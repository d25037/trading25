# TOPIX Gap / Intraday Distribution

## Published Readout

### Decision
- PIT-safe rerun completed. 旧 baseline の headline は撤回し、`20260608_pit_safe_topix500` の結果で置き換える。

### Why This Research Was Run
- TOPIX 寄り付き gap の強弱に応じて、当日 intraday で `TOPIX500` と `PRIME ex TOPIX500` のどちらが相対的に強いかを、PIT-safe な membership で再検証する。
- 旧 readout は `TOPIX500` / `PRIME ex TOPIX500` の membership が exact PIT で証明されていなかったため、fallback removal 後に high-value queue の最優先 rerun とした。

### Data Scope / PIT Assumptions
- Run ID: `20260608_pit_safe_topix500`
- Analysis range: `2016-05-18 -> 2026-06-05`
- Historical run market schema: `3`（retired; 数値の provenance としてのみ保持）
- Current rerun requirement: Market schema v5 / `provider_adjusted_v1`
- Universe source: `stock_master_daily,index_membership_daily`
- As-of policy: signal-date membership。`TOPIX500` は `index_membership_daily.index_code = TOPIX500` の exact-date join、`PRIME ex TOPIX500` は signal-date `stock_master_daily` の Prime から同日 TOPIX500 membership を除外する。
- Latest snapshot fallback: not allowed. current runner は Market v5 provider-window/current-basis lineage または対象 signal date の TOPIX500 membership が欠ける場合に失敗する。

### Main Findings
#### 結論: weak gap は TOPIX500、strong gap は PRIME ex TOPIX500 が相対的に強い

| Slice | Days | Selected group | Mean intraday return | Win ratio / up ratio |
| --- | ---:| --- | ---:| ---:|
| `gap <= -1.34%` | 84 | TOPIX500 | `+0.2285%` | `54.34%` |
| `-1.34% < gap <= -0.67%` | 244 | TOPIX500 | `-0.0333%` | `48.19%` |
| `-0.67% < gap < 0.67%` | 1729 | flat | `0.0000%` | n/a |
| `0.67% <= gap < 1.34%` | 332 | PRIME ex TOPIX500 | `+0.0691%` | `48.89%` |
| `gap >= 1.34%` | 66 | PRIME ex TOPIX500 | `+0.2253%` | `52.29%` |

#### 結論: fixed rotation は positive だが、large negative gap の内側は弱い

| Metric | Value |
| --- | ---:|
| TOPIX gap sample count | `2455` |
| TOPIX gap mean / std | `+0.0399% / 0.6716%` |
| Trade days / flat days | `726 / 1729` |
| Mean trade return | `+0.0645%` |
| Mean daily return | `+0.0191%` |
| Win trade ratio | `54.68%` |
| Cumulative return | `+54.92%` |
| Max drawdown | `-11.06%` |

### Interpretation
- 旧方向性は大枠では確認されたが、弱い gap は一枚岩ではない。`gap <= -1.34%` では TOPIX500 が `+0.2285%` と強い一方、`-1.34% < gap <= -0.67%` では `-0.0333%` で、weak signal 全体の平均を薄めている。
- strong side は PRIME ex TOPIX500 が両 bucket でプラス。特に `gap >= 1.34%` は `+0.2253%` で、risk-on の小型寄り rotation として解釈しやすい。
- neutral bucket は 1729 日と大半を占めるため、固定 rotation はほとんど flat で、edge は extreme gap day に集中している。

### Production Implication
- market-state / Ranking 補助 signal としては、`gap <= -2σ` と `gap >= +1σ` の方向性だけを候補に残す。`-2σ..-1σ` の weak bucket は単独 long signal としては弱い。
- production strategy に直結するには、寄り付き後に取引可能か、コスト、same-day breadth、sector composition、日次ポジション重複を追加検証する。
- この README 以降、`TOPIX500` / `PRIME ex TOPIX500` headline は exact `index_membership_daily` rerun の結果だけを使う。

### Caveats
- `TOPIX100` は signal-date `stock_master_daily.scale_category` 由来で、今回の production implication は主に `TOPIX500` / `PRIME ex TOPIX500` rotation に限定する。
- intraday return は daily `open -> close` で、実際の寄り付き約定、スプレッド、板、売買代金制約は含まない。
- 旧 baseline の数値は下の既存セクションに historical context として残るが、この `Published Readout` より優先しない。

### Source Artifacts
- Experiment: `market-behavior/topix-gap-intraday-distribution`
- Runner: `apps/bt/scripts/research/run_topix_gap_intraday_distribution.py`
- Domain logic: `apps/bt/src/domains/analytics/topix_gap_intraday_distribution.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix-gap-intraday-distribution/20260608_pit_safe_topix500/`
- Bundle tables: `summary_df`, `day_counts_df`, `rotation_daily_df`, `rotation_signal_summary_df`, `rotation_overall_summary_df`
- `summary.json` / legacy digest fields are intentionally not used as publication evidence.

TOPIX の寄り付き gap を event day として bucket 化し、個別銘柄群の当日 intraday リターンと、そこから導かれる簡易 rotation ルールを観察する実験です。

## 目的

- `TOPIX が窓開けして始まった日` に、どの銘柄群が当日中に相対的に強いかを把握する。
- gap の方向と強さに応じて、`TOPIX500` と `PRIME ex TOPIX500` のどちらを intraday で選ぶべきかを検討する。
- 単純な bucket 平均だけでなく、`どの gap bucket がどれくらい頻繁に起きるか` も含めて解釈する。

## スコープ

- Event definition:
  - `topix_gap_return = (topix_open - prev_topix_close) / prev_topix_close`
- Trade definition:
  - `stock_intraday_return = (close - open) / open`
- Stock groups:
  - `PRIME`
  - `TOPIX100`
  - `TOPIX500`
  - `PRIME ex TOPIX500`
- Fixed rotation rule:
  - `weak => TOPIX500 long`
  - `strong => PRIME ex TOPIX500 long`
  - `neutral => flat`

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix_gap_intraday_distribution.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix_gap_intraday_distribution.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_topix_gap_intraday_distribution.py`

## Latest Baseline

- [baseline-2026-03-24.md](./baseline-2026-03-24.md)

## Current Read

- 現行 baseline の TOPIX gap 標準偏差は `0.6751%` で、bucket は `±1σ = 0.68%`, `±2σ = 1.35%` を使う。
- `-0.68% < gap < 0.68%` が 70.56% を占め、この帯では `TOPIX100` と `TOPIX500` はほぼフラット、`PRIME` と `PRIME ex TOPIX500` は小幅マイナス。
- `gap <= -1.35%` では全 group が明確にプラスで、特に `TOPIX100` / `TOPIX500` が `0.26%` 台と強い。
- `0.68% <= gap < 1.35%` では `PRIME` / `PRIME ex TOPIX500` が優位に回り、`TOPIX100` / `TOPIX500` は逆に弱い。
- sigma ベースにすると `strong` 側のサンプルが十分増え、rotation では `weak` と `strong` の両方が寄与する構図になった。

## 再現方法

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix_gap_intraday_distribution.py \
  --sample-size 0
```

この command は
`~/.local/share/trading25/research/market-behavior/topix-gap-intraday-distribution/<run_id>/`
へ bundle を保存します。

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## 次に見るべき点

- `gap <= -2σ` で `TOPIX100` / `TOPIX500` が強い理由を、セクター構成や寄り付き売買代金で分解できるか。
- `+1σ ~ +2σ` と `>= +2σ` の strong side を分けると、`PRIME ex TOPIX500` の使い方はさらに sharpen するか。
- fixed rotation を `TOPIX100` を含む選択ルールに変えると、trade frequency と drawdown のバランスが改善するか。
