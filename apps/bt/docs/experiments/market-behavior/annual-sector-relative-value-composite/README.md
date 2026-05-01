# Annual Sector-Relative Value Composite

年次 fundamental panel 上で、低 `PBR` / 低 `forward PER` を市場全体の raw percentile として扱うべきか、同一 `sector_33_name` 内の相対 valuation として扱うべきかを比較する研究。

## Published Readout

### Decision

v3 positive-ratio run では、Standard は sector-relative / hybrid valuation に改善余地がある。一方で Prime は `prime_size_tilt_raw` がまだ最良で、sector-relative 化はやや劣化した。したがって次の候補は、Standard に限って `standard_pbr_tilt_hybrid` または `equal_hybrid_valuation` を追加検証し、Prime は現行 `prime_size_tilt` を維持する方針。

### Why This Research Was Run

小型、低 `PBR`、低 `forward PER` が重要で、かつ Prime / Standard で配分が違うことは分かってきた。一方で `PBR` と `forward PER` は業種水準差が大きいため、現行 raw score が「同業内で安い銘柄」ではなく「安く見えやすいセクター」を拾っているだけの可能性がある。そこで、同一 `year x market x sector_33_name` 内 percentile による sector-relative valuation を別 research として検証する。

### Data Scope / PIT Assumptions

入力は v3 parent bundle `/tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/20260501_prime_standard_check/` の `event_ledger_df`。positive `PBR` / positive `forward PER` を要求し、`32,264` realized events から `21,532` scored events を作った。fundamental panel 側で as-of 年次イベントを構築した後に、実現済みイベントのみを使って `year x market` raw score と `year x market x sector_33_name` sector-relative score を作る。sector-relative score は各 sector group に finite な対象銘柄が `5` 以上ある場合だけ有効化し、薄い sector group の過学習を避ける。

### Main Findings

#### 結論

| Market | Score | Top | Events | CAGR | Sharpe | MaxDD | Read |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `standard` | `equal_hybrid_valuation` | `10%` | `706` | `37.16%` | `2.20` | `-30.37%` | best Standard row |
| `standard` | `standard_pbr_tilt_hybrid` | `10%` | `706` | `36.81%` | `2.18` | `-31.15%` | close to raw, sector-adjusted |
| `standard` | `standard_pbr_tilt_raw` | `10%` | `722` | `36.38%` | `2.18` | `-30.23%` | current Standard-weight baseline |
| `standard` | `standard_pbr_tilt_sector_relative` | `10%` | `706` | `36.22%` | `2.14` | `-31.35%` | full sector-relative is okay but not clearly better |
| `standard` | `equal_sector_relative_forward_per` | `10%` | `706` | `36.71%` | `2.18` | `-30.53%` | forward PER sector-neutralization helps |
| `prime` | `prime_size_tilt_raw` | `10%` | `1,256` | `23.42%` | `1.30` | `-32.53%` | best Prime row |
| `prime` | `prime_size_tilt_hybrid` | `10%` | `1,248` | `23.09%` | `1.28` | `-32.85%` | close, but not better |
| `prime` | `prime_size_tilt_sector_relative` | `10%` | `1,248` | `21.89%` | `1.24` | `-32.89%` | sector-relative weakens Prime |

#### Coverage

| Market | Raw score coverage | Sector-relative valuation coverage |
| --- | ---: | ---: |
| `prime` | `100.00%` | `99.37%` |
| `standard` | `100.00%` | `97.84%` |

### Interpretation

Standard は full sector-relative より hybrid が良く、raw valuation の sector allocation premium を完全に消すより、同業内の安さを半分だけ混ぜる方が安定している。特に `equal_sector_relative_forward_per` が強く、Standard では `forward PER` の sector-neutralization が有望に見える。Prime は raw `prime_size_tilt` が最良で、sector-relative 化すると CAGR / Sharpe が落ちるため、Prime の edge は sector allocation を含んだ raw valuation と小型 tilt の組み合わせとして扱う方が現時点では自然。

### Production Implication

Ranking page の既存 `standard_pbr_tilt` / `prime_size_tilt` は維持する。次に入れるなら Standard 限定で `standard_pbr_tilt_hybrid` または `standard_forward_per_sector_hybrid` のような候補を別 score method として検証する。Prime は `prime_size_tilt` を継続し、sector-relative 版を急いで導入しない。実運用では `ADV60` を score に混ぜず、capacity / execution diagnostic として別管理する。

### Caveats

sector-relative score は sector group の銘柄数に敏感で、Standard や Growth の薄い sector では coverage が落ちる。`sector_33_name` の分類変更や上場市場変更は input panel の PIT 解決に依存する。portfolio lens は年次 open-to-close equal-weight で、コスト、スリッページ、capacity、turnover、borrowability は含まない。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/annual_sector_relative_value_composite.py`
- Runner: `apps/bt/scripts/research/run_annual_sector_relative_value_composite.py`
- Tests: `apps/bt/tests/unit/domains/analytics/test_annual_sector_relative_value_composite.py`
- Bundle: `/tmp/trading25-research/market-behavior/annual-sector-relative-value-composite/sector-relative-value-v3/`

## Current Surface

- Domain:
  - `apps/bt/src/domains/analytics/annual_sector_relative_value_composite.py`
- Runner:
  - `apps/bt/scripts/research/run_annual_sector_relative_value_composite.py`
- Bundle:
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`
  - `summary.json`

## Design

- Input: annual first-open/last-close fundamental panel bundle.
- Raw score:
  - `low_pbr_score`: `year x market` 内で低 `PBR` ほど高い percentile。
  - `low_forward_per_score`: `year x market` 内で低 `forward PER` ほど高い percentile。
  - `small_market_cap_score`: `year x market` 内で小型ほど高い percentile。
- Sector-relative score:
  - `sector_low_pbr_score`: `year x market x sector_33_name` 内で低 `PBR` ほど高い percentile。
  - `sector_low_forward_per_score`: `year x market x sector_33_name` 内で低 `forward PER` ほど高い percentile。
  - `min_sector_observations` 未満の sector group は欠損扱い。
- Hybrid score:
  - `hybrid_low_pbr_score`: raw `PBR` score と sector-relative `PBR` score の 50/50。
  - `hybrid_low_forward_per_score`: raw `forward PER` score と sector-relative `forward PER` score の 50/50。
- Selection:
  - `all` / `prime` / `standard` / `growth` の market scope ごと。
  - 年ごとに top `5% / 10% / 15%` を選定。
  - liquidity floor は alpha score に混ぜず、初期比較は no floor のみにする。

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_sector_relative_value_composite.py \
  --output-root /tmp/trading25-research \
  --run-id sector-relative-value-v3
```

positive `PBR` / positive `forward PER` filter を外す確認:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_sector_relative_value_composite.py \
  --output-root /tmp/trading25-research \
  --allow-non-positive-pbr-or-forward-per \
  --run-id sector-relative-value-v3-all-ratios
```
