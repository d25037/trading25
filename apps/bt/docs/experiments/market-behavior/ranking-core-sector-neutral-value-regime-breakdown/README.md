# Ranking Core Sector-Neutral Value Regime Breakdown

Daily Ranking の年次factor breakdownを、raw `Undervalued` ではなく同日・同市場・同33業種内の sector-neutral value percentile でやり直す研究。目的は、2022年以降に銀行業へ集中していた `Momentum Value + Sector Score: Strong` のreturnを、他セクターの sector-neutral value で置換できるかを確認すること。

## Published Readout

### Decision

今回の結論は、sector-neutral value は捨てるべきではないが、現行の `Momentum Value + Sector Score: Strong` を単純に sector-neutral value へ置き換えても、2022年以降の銀行依存returnはまだ他セクターへ十分には移せていない、というもの。

2022-2026 の 20D では、`Sector-Neutral Momentum Value + Sector Score: Strong` は銀行業比率を下げる一方、raw `Momentum Value + Sector Score: Strong` に年次medianで劣後する。ex Banks に限定しても、sector-neutral版がraw版を安定して上回る状態ではない。2026 の 20D ex Banks だけは改善するが、標本は小さく、60Dではまだ弱い。

一方で 2016-2021 の 20D では、sector-neutral momentum value が raw momentum value より良い年が多い。したがって、sector-neutral value は「銀行相場を置換する本命」としてはまだ弱いが、「銀行依存を抑えた別regime用value sleeve」として再検証する価値は残る。

Production implication は、raw `Momentum Value + Sector Score: Strong` を sector-neutral value に置換しないこと。次に進めるなら、単純な daily association ではなく、sector cap / sector-balanced portfolio / ex Banks positive gate を入れた portfolio lens で、銀行業以外からreturnを取れるかを再検証する。

### Main Findings

#### 結論: 2022-2026 の 20D では、sector-neutral化は銀行比率を下げるがreturnも削る

Primary run `20260603_ranking_core_sector_neutral_value_regime_prime_v1` は `2016-05-17` から `2026-05-14`、Prime SoT、horizon `5/10/20/60`、`min_observations=20`。Prime SoT は再編前の `0101` を含む。観測母集団は `4,751,602` stock-days。

| Horizon | Scope | Comparison | Median diff | Sector-neutral wins | Years | Sector-neutral bank share | Raw bank share |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 20D | All sectors | Sector-neutral vs raw `Momentum Value + Sector Score: Strong` | -2.28% | 0 | 5 | 46.8% | 77.2% |
| 20D | ex Banks | Sector-neutral vs raw `Momentum Value + Sector Score: Strong` | -1.77% | 1 | 5 | 0.0% | 0.0% |
| 20D | All sectors | Hybrid vs raw `Momentum Value + Sector Score: Strong` | -1.03% | 1 | 5 | 56.4% | 77.2% |
| 20D | ex Banks | Hybrid vs raw `Momentum Value + Sector Score: Strong` | -0.29% | 2 | 5 | 0.0% | 0.0% |
| 60D | All sectors | Sector-neutral vs raw `Momentum Value + Sector Score: Strong` | +0.02% | 3 | 5 | 47.8% | 78.1% |
| 60D | ex Banks | Sector-neutral vs raw `Momentum Value + Sector Score: Strong` | -0.73% | 1 | 5 | 0.0% | 0.0% |

#### 結論: 年次で見ると、sector-neutral版は銀行を減らすがnon-bankで安定的に勝てていない

20D TOPIX excess median。`Sector-Neutral` は同日・同市場・同33業種内で PBR と Forward PER がともに下位20%の条件。

| Year | Scope | Raw `Momentum Value + Sector Score: Strong` | Sector-neutral | Hybrid | Raw bank share | Sector-neutral bank share |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2022 | All | +3.23% | -0.05% | +2.20% | 82.2% | 27.7% |
| 2022 | ex Banks | -0.08% | -1.85% | -0.37% | 0.0% | 0.0% |
| 2023 | All | +1.57% | +0.42% | +0.61% | 66.2% | 37.9% |
| 2023 | ex Banks | +1.19% | -1.51% | -0.34% | 0.0% | 0.0% |
| 2024 | All | +0.49% | +0.46% | +0.60% | 77.4% | 44.8% |
| 2024 | ex Banks | -1.06% | -2.81% | +0.48% | 0.0% | 0.0% |
| 2025 | All | +2.70% | +0.41% | +1.53% | 74.0% | 46.0% |
| 2025 | ex Banks | -0.80% | -3.88% | -2.39% | 0.0% | 0.0% |
| 2026 | All | +6.68% | +2.78% | +4.91% | 86.0% | 77.6% |
| 2026 | ex Banks | -4.06% | +2.28% | -0.10% | 0.0% | 0.0% |

2026 の ex Banks 20D では sector-neutral版がraw版より良いが、観測数は `43`、sector数は `4` に限られる。60Dでは同じ ex Banks が raw `-12.46%`、sector-neutral `-4.25%` と損失圧縮にはなるものの、絶対値ではまだマイナス。

#### 結論: 2016-2021 の 20D では sector-neutral value に改善余地がある

2016-2021 は銀行業が強い局面ではなく、sector-neutral化の目的が「銀行依存の除去」として機能しやすい。20Dでは sector-neutral momentum value がrawに勝つ年が多い。

| Horizon | Scope | Comparison | Median diff | Sector-neutral wins | Years | Sector-neutral bank share | Raw bank share |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 20D | All sectors | Sector-neutral vs raw `Momentum Value` | +0.61% | 5 | 6 | 3.5% | 17.4% |
| 20D | ex Banks | Sector-neutral vs raw `Momentum Value` | +0.35% | 5 | 6 | 0.0% | 0.0% |
| 20D | All sectors | Sector-neutral vs raw `Momentum Value + Sector Score: Strong` | +0.44% | 4 | 6 | 6.1% | 20.6% |
| 20D | ex Banks | Sector-neutral vs raw `Momentum Value + Sector Score: Strong` | +0.17% | 3 | 6 | 0.0% | 0.0% |
| 60D | All sectors | Sector-neutral vs raw `Momentum Value` | +0.68% | 4 | 6 | 3.5% | 17.4% |
| 60D | ex Banks | Sector-neutral vs raw `Momentum Value` | -0.04% | 3 | 6 | 0.0% | 0.0% |

ただし、`Sector Score: Strong` を重ねた 60D では sector-neutral版がrawを下回る。したがって「sector-neutral value + sector strength」が一貫して良いのではなく、regimeやhorizonで効き方が変わる。

#### 結論: NT regime別では、2016-2021 Flatの損失は軽くなるが、2022-2025の銀行相場は置換できない

20D TOPIX excess median。

| Period | NT regime | Raw `Momentum Value + Sector Score: Strong` | Sector-neutral | Hybrid | Raw bank share | Sector-neutral bank share |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2016-2021 | NT Down <= -3% / 60D | +0.98% | -0.24% | +1.19% | 7.3% | 0.0% |
| 2016-2021 | NT Flat +/-3% / 60D | -1.75% | -1.18% | -1.08% | 29.8% | 3.1% |
| 2016-2021 | NT Up >= +3% / 60D | +3.35% | +3.17% | +0.73% | 4.7% | 0.0% |
| 2022-2025 | NT Down <= -3% / 60D | +0.49% | -2.07% | -0.26% | 84.0% | 51.3% |
| 2022-2025 | NT Flat +/-3% / 60D | +2.47% | +0.35% | +1.47% | 70.5% | 36.4% |
| 2022-2025 | NT Up >= +3% / 60D | +5.18% | +2.61% | +2.70% | 69.2% | 36.8% |

2016-2021 Flatでは sector-neutral / hybrid がrawの損失を圧縮する。一方、2022-2025では銀行比率を下げるほどreturnも下がり、銀行業から他セクターへのreturn置換にはなっていない。

#### 結論: sector breadthは十分に広がっていない

2022-2026、20D、`Momentum Value + Sector Score: Strong` 系のbreadth regime別集計。

| Breadth | Factor | Obs | Bank share | Median | Win | Severe |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| High Breadth | Raw | 2,455 | 67.3% | +2.83% | 65.8% | 3.6% |
| High Breadth | Sector-neutral | 696 | 42.7% | +0.29% | 53.0% | 10.4% |
| High Breadth | Hybrid | 3,854 | 49.0% | +1.02% | 59.6% | 5.6% |
| Mid Breadth | Raw | 2,456 | 77.8% | +2.55% | 62.0% | 4.1% |
| Mid Breadth | Sector-neutral | 589 | 48.1% | +1.30% | 58.2% | 4.2% |
| Mid Breadth | Hybrid | 3,438 | 56.4% | +1.89% | 60.5% | 6.2% |
| Low Breadth | Raw | 687 | 70.4% | -0.80% | 54.1% | 10.7% |
| Low Breadth | Sector-neutral | 211 | 30.4% | -3.10% | 32.5% | 15.1% |
| Low Breadth | Hybrid | 1,150 | 33.9% | -1.00% | 37.4% | 20.6% |

sector-neutral版は銀行比率を下げるが、High / Mid / Low breadth のどれでもrawを明確には上回らない。Low Breadthでは特に悪化する。

### Interpretation

このresearchは、sector-neutral value を「同日・同市場・同33業種内の PBR / Forward PER percentile」として定義した。これは portfolio weight を33業種中立にする完全な sector-neutral portfolio ではない。したがって、今回の結果は「sector-relative valuation signal のdaily association」であり、sector-balanced portfolioの結論ではない。

銀行業が強かった 2022年以降は、raw `Momentum Value + Sector Score: Strong` のheadline returnが銀行業の寄与を大きく含む。sector-neutral化すると銀行比率は下がるが、他セクターがそのreturnを埋めきれていない。これは「銀行業に依存していたreturnを他セクターから得る」という今回の中核仮説に対して、現行の単純な sector-neutral value 定義だけでは不十分、という結果。

ただし 2016-2021 では、sector-neutral value がraw valueより良く見える箇所がある。これは「銀行相場を追う」局面ではなく、「sector内での相対割安を拾う」局面では有効になりうることを示す。したがって、sector-neutral value は production から除外するのではなく、regime別 sleeve / sector-balanced portfolio として再検証する。

### Production Implication

| Candidate | Implication |
| --- | --- |
| Raw `Momentum Value + Sector Score: Strong` | 2022年以降のheadlineは銀行業依存が強く、cross-sector strategyとしては採用しない |
| `Sector-Neutral Momentum Value + Sector Score: Strong` | 銀行比率は下がるが、2022-2026のnon-bank置換には失敗。単純置換はしない |
| `Hybrid Momentum Value + Sector Score: Strong` | rawより銀行比率を下げるが、20Dではrawに劣後。60D all-sectorは互角に近いがex Banksでは弱い |
| 2016-2021 sector-neutral momentum value | 20Dでは改善余地があるため、別regime用value sleeveとして追加検証する |
| Sector cap / sector-balanced portfolio | 次の検証対象。associationではなくportfolio weight制御で銀行returnを他セクターへ再配分できるかを見る |
| Low Breadth | sector-neutral版も悪化するため、long confidence guardとして維持する |

### Caveats

- `Sector-Neutral` は同日・同市場・同33業種内 percentile であり、portfolio sector weight を中立化していない。
- 2026 は `2026-05-14` までの partial year。特に60D forward returnは後半ほど有効観測が減る。
- `Sector Score` は Daily Ranking SoT通り、公式33業種指数scoreと構成銘柄scoreの平均を前提にしたsector overlay。
- 銀行業は `sector_33_name = '銀行業'` で分離した。銀行以外の金融、保険、証券・商品先物取引業は ex Banks に含まれる。
- factor signal は daily close-to-close forward TOPIX excess の association study であり、実行コスト、turnover、capacity、borrow、実ポートフォリオweightは含まない。
- `min_sector_observations=5` のため、小さいsector groupは sector-neutral percentile が欠損する。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/ranking_core_sector_neutral_value_regime_breakdown.py`
- Runner: `apps/bt/scripts/research/run_ranking_core_sector_neutral_value_regime_breakdown.py`
- Tests: `apps/bt/tests/unit/domains/analytics/test_ranking_core_sector_neutral_value_regime_breakdown.py`
- Bundle: `/tmp/trading25-research/market-behavior/ranking-core-sector-neutral-value-regime-breakdown/20260603_ranking_core_sector_neutral_value_regime_prime_v1/`
- Result tables: `annual_strategy_summary_df`, `bank_displacement_df`, `sector_breadth_df`, `sector_year_contribution_df`, `strategy_breadth_regime_df`, `nt_regime_strategy_df`, `strategy_comparison_df`, `current_term_mapping_df`

## Current Surface

- Domain:
  - `apps/bt/src/domains/analytics/ranking_core_sector_neutral_value_regime_breakdown.py`
- Runner:
  - `apps/bt/scripts/research/run_ranking_core_sector_neutral_value_regime_breakdown.py`
- Bundle:
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_ranking_core_sector_neutral_value_regime_breakdown.py \
  --output-root /tmp/trading25-research \
  --run-id 20260603_ranking_core_sector_neutral_value_regime_prime_v1 \
  --start-date 2016-05-17 \
  --end-date 2026-05-14 \
  --markets prime \
  --horizons 5,10,20,60 \
  --min-observations 20 \
  --notes "Annual factor regime rerun with sector-neutral value definitions and bank displacement diagnostics"
```
