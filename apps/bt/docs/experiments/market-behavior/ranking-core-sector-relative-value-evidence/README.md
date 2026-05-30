# Ranking Core Sector-Relative Value Evidence

`neutral_rerating blue + sector_strong` の core sleeve 内で、raw `PBR` / `forward PER` percentile と、同一 `sector_33_name` 内の sector-relative percentile を比較する研究。

## Published Readout

### Decision

Prime の `neutral_rerating blue + sector_strong` core は、raw `low_pbr20_low_fwd_per20` を入口条件として維持する。sector-relative `PBR` / `forward PER` への置換はしない。

sector-relative valuation は同セクター内 alpha の診断としては意味があるが、`sector_relative_only_core` は 20D / 60D TOPIX excess が raw core より大きく劣り、左尾も重い。したがって core 候補の優先順位付けでは、まず raw strong value core を残し、sector-relative は tie-breaker / diagnostic に留める。

`raw_and_sector_relative_core` は 20D では raw core とほぼ同等か少し良いが、60D では raw core を上回らない。sample も `raw_core` の約 22% に縮むため、hard filter としては採用しない。

### Main Findings

#### 結論: raw core を sector-relative core で置換しない

Primary run `20260530_core_sector_relative_value_prime_v2` は `2016-04-01` から `2026-05-14`、Prime、horizon `5/10/20/60`。観測母集団は `536,579`、code `1,719`、date `944`、33セクター coverage は `33`。

| Rule | Obs | Code | 20D TOPIX ex median | 20D win | 20D severe | 60D TOPIX ex median | 60D win | 60D severe | Read |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `raw_core` | 11,593 | 199 | +1.815% | 62.33% | 3.01% | +4.507% | 64.29% | 11.02% | baseline。置換しない |
| `sector_relative_core` | 4,633 | 234 | +1.214% | 57.44% | 5.03% | +3.178% | 60.95% | 13.74% | raw core より弱い |
| `raw_and_sector_relative_core` | 2,628 | 114 | +1.924% | 61.87% | 3.69% | +4.163% | 64.76% | 11.21% | 20D は近いが sample が細り、60D は raw に劣る |
| `raw_only_core` | 8,794 | 133 | +1.742% | 62.34% | 2.82% | +4.385% | 63.54% | 11.17% | sector-relative 非該当でも十分強い |
| `sector_relative_only_core` | 1,768 | 168 | -0.166% | 49.43% | 7.07% | +1.354% | 54.39% | 17.92% | sector内割安だけでは core にならない |
| `hybrid_core` | 19,591 | 615 | +0.982% | 56.16% | 4.96% | +2.785% | 58.33% | 14.63% | 広げすぎ。core quality が薄まる |

#### 結論: sector-relative は sector excess では効くが TOPIX excess 入口ではない

| Rule | 20D sector ex median | 20D win | 60D sector ex median | 60D win | Read |
| --- | ---: | ---: | ---: | ---: | --- |
| `raw_core` | -0.028% | 49.75% | +0.569% | 52.83% | TOPIX excess は強いが sector内優位は薄い |
| `sector_relative_core` | +0.362% | 52.88% | +0.730% | 53.04% | 同セクター内 alpha は改善 |
| `sector_relative_only_core` | +0.482% | 53.73% | +1.322% | 54.74% | sector excess は良いが TOPIX excess が弱い |

sector-relative 化は「同セクター内で相対的に安い銘柄」を拾うので sector excess は改善する。しかし今回の core 戦略の主眼は `sector_strong` の beta を取りに行く TOPIX excess であり、sector-relative-only はそこを満たさない。

#### 結論: raw q1/q1 は sector-relative q1/q1 でなくても強い

20D の `neutral_rerating + sector_strong` matrix では、raw `PBR q1_low` + raw `forward PER q1_low` は sector-relative bucket が q1/q1 でなくても強い行が残る。

| Raw PBR | Raw Fwd PER | Sector PBR | Sector Fwd PER | Obs | 20D TOPIX ex median | Win | Severe | Sector ex median |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `q1_low` | `q1_low` | `q3` | `q1_low` | 832 | +3.453% | 68.51% | 4.69% | +0.556% |
| `q1_low` | `q1_low` | `q2` | `q2` | 1,518 | +2.173% | 64.89% | 2.50% | +0.129% |
| `q1_low` | `q1_low` | `q1_low` | `q1_low` | 3,071 | +2.086% | 62.52% | 3.65% | +0.306% |
| `q1_low` | `q1_low` | `q1_low` | `q2` | 1,579 | +1.903% | 63.52% | 2.66% | +0.175% |
| `q1_low` | `q1_low` | `q2` | `q1_low` | 1,980 | +1.725% | 61.16% | 3.28% | +0.060% |

このため、raw `low_pbr20_low_fwd_per20` が単に「銀行など安い sector を拾っているだけ」とは言い切れない。むしろ core の TOPIX excess は、sector strong beta と raw absolute cheapness の組み合わせとして読む方が自然。

#### 年次安定性

20D TOPIX excess の年次表では、`raw_core` は 2022、2023、2025 で強く、2024/2026 は小幅プラス。`hybrid_core` は 2024/2026 でほぼ中立化し、core を広げすぎると質が落ちる。

| Year | Rule | Obs | 20D TOPIX ex median | Win | Severe |
| --- | --- | ---: | ---: | ---: | ---: |
| 2022 | `raw_core` | 1,160 | +5.037% | 73.4% | 1.7% |
| 2023 | `raw_core` | 3,263 | +2.174% | 64.2% | 5.9% |
| 2023 | `raw_and_sector_relative_core` | 643 | +2.548% | 67.2% | 5.3% |
| 2024 | `raw_core` | 1,880 | +0.404% | 53.3% | 1.5% |
| 2025 | `raw_core` | 4,607 | +1.819% | 63.2% | 1.2% |
| 2025 | `raw_and_sector_relative_core` | 1,095 | +1.961% | 62.9% | 1.3% |
| 2025 | `sector_relative_only_core` | 578 | +1.253% | 56.2% | 10.9% |
| 2026 | `raw_core` | 683 | +0.789% | 53.6% | 8.1% |

### Interpretation

今回の研究は、過去の annual sector-relative valuation 研究とは outcome が違う。annual では同業内 valuation が portfolio selection を改善するかを見たが、今回は `neutral_rerating blue + sector_strong` の短期 core sleeve 内で、候補順位を raw valuation から sector-relative valuation に置換できるかを見た。

結果は置換否定。sector-relative-only は sector excess では見栄えが良いが、TOPIX excess では core として弱い。これは、今回の core が「強いセクターに乗りながら、絶対水準でも安い銘柄を拾う」構造だからだと解釈できる。同セクター内で安いだけの銘柄は、sector beta を TOPIX excess に変える力が弱い。

一方で、`raw_and_sector_relative_core` は 20D では悪くないので、UI では hard filter ではなく confidence badge / tie-breaker として残す余地がある。

### Production Implication

Ranking の core long rule は以下を維持する。

| Candidate | Production implication |
| --- | --- |
| `raw_core` | `neutral_rerating blue + sector_strong + raw low_pbr20_low_fwd_per20` を core baseline とする |
| `raw_and_sector_relative_core` | hard filter にはしない。badge / sort tie-breaker 候補 |
| `sector_relative_only_core` | core へ昇格しない。sector-relative cheapness だけでは買い候補にしない |
| `hybrid_core` | 広げすぎで quality が薄まるため primary sort には使わない |

次に UI へ出すなら、sector-relative valuation は `Core confirmation` のような小さな badge に留める。Ranking color や core eligibility を変える材料ではない。

### Caveats

- Primary universe は Prime。
- 33セクター分類は `stock_master_daily.date = target date` の exact-date PIT join。
- `sector_relative_core` は sector内 percentile のため、sectorごとの観測数・構成銘柄数に影響される。
- outcome は close-to-close forward return で、portfolio capacity、turnover、cost、execution は含まない。
- `sector_relative_only_core` は sector excess が良いため、market-neutral / sector-neutral book では別評価の余地があるが、今回の long core 目的とは違う。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/ranking_core_sector_relative_value_evidence.py`
- Runner: `apps/bt/scripts/research/run_ranking_core_sector_relative_value_evidence.py`
- Tests: `apps/bt/tests/unit/domains/analytics/test_ranking_core_sector_relative_value_evidence.py`
- Bundle: `/tmp/trading25-research/market-behavior/ranking-core-sector-relative-value-evidence/20260530_core_sector_relative_value_prime_v2/`

## Current Surface

- Domain:
  - `apps/bt/src/domains/analytics/ranking_core_sector_relative_value_evidence.py`
- Runner:
  - `apps/bt/scripts/research/run_ranking_core_sector_relative_value_evidence.py`
- Bundle:
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_ranking_core_sector_relative_value_evidence.py \
  --output-root /tmp/trading25-research \
  --run-id 20260530_core_sector_relative_value_prime_v2 \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --markets prime \
  --horizons 5,10,20,60
```
