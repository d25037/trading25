# Ranking Core Factor Regime Breakdown

Daily Ranking の `Momentum Value` core を、年次別に `Undervalued`、`20/60D Momentum`、`ATR20 Accel`、`Balanced Sector Strength`、market breadth regime、`NT 60D Regime`、銀行業除外で分解する研究。

## Published Readout

### Decision

`NT 60D Regime` を追加した目的は、Daily Ranking factor の実力と、TOPIX benchmark に対する市場構造の追い風・逆風を分けること。`NT Up >= +3% / 60D` は Nikkei 225 寄り大型・指数寄与銘柄が TOPIX より強い局面であり、Prime/TSE1 の等ウェイト寄りな銘柄選別は TOPIX excess で不利に見えやすい。

再解析の結論は、2016-2021 の弱さは「因子が全面的に壊れていた」というより、Prime/TSE1 universe baseline 自体が TOPIX に負けやすい市場構造を含んでいたことが大きい。ただし `Momentum Value` が本当に悪い箇所もあり、特に 2016-2021 の `NT Flat +/-3% / 60D` では `Momentum Value` が baseline 比でも `-0.57%`、`Momentum Value + Balanced Sector Strength: Strong` が `-1.17%` と悪い。

2022-2025 は、銀行業を含む集計では `Momentum Value` と `Momentum Value + Balanced Sector Strength: Strong` が NT Down/Flat/Up の全regimeで baseline を上回る。しかし銀行業除外後は、特に `Momentum Value + Balanced Sector Strength: Strong` のedgeが消える。したがってこの stack は頑強な cross-sector strategy ではなく、実質的に銀行セクターの強さに依存していたと読む。

銀行業除外を加えると、2022年以降の `Momentum Value + Balanced Sector Strength: Strong` の強さは銀行業依存と判断するのが妥当である。`Momentum Value` 単体は銀行業除外後も相対的に残る年があるが、`Balanced Sector Strength: Strong` を重ねると 2022/2024/2025/2026 の ex Banks はほぼ消えるかマイナスになる。したがって `Momentum Value + Balanced Sector Strength: Strong` は「汎用的なセクター確認」ではなく、銀行業ベータを取っていた条件として扱う。

Production implication は、`NT 60D Regime` を売買シグナルではなく factor diagnostic / confidence guard として使うこと。`Momentum Value + Balanced Sector Strength: Strong` は production strategy 候補から降格し、銀行業集中のdiagnosticとして扱う。productionに近づけるなら、先に sector cap / ex Banks positive / banks share 上限を満たす別条件として再検証する。`ATR20 Accel` を足した複合も同じ銀行業集中問題を引き継ぐため、headline performanceでは採用しない。

### Main Findings

#### 結論: NT倍率は2018-2020と2026に上昇し、2022-2025はおおむね横ばい圏

Primary run `20260603_ranking_core_factor_breadth_nt_bank_exclusion_prime_v7` は `2016-05-17` から `2026-05-14`、Prime SoT、horizon `5/10/20/60`、`min_observations=20`。Prime SoT は再編前の `0101` を含む。観測母集団は `4,751,602` stock-days。

| Year | NT start | NT end | Change | 60D Up days | 60D Flat days | 60D Down days |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2016 | 12.561 | 12.587 | +0.21% | 0.0% | 45.5% | 0.0% |
| 2017 | 12.605 | 12.525 | -0.63% | 13.4% | 83.8% | 2.8% |
| 2018 | 12.612 | 13.396 | +6.22% | 22.7% | 77.3% | 0.0% |
| 2019 | 13.297 | 13.743 | +3.35% | 11.2% | 88.8% | 0.0% |
| 2020 | 13.670 | 15.207 | +11.24% | 35.5% | 62.0% | 2.5% |
| 2021 | 15.189 | 14.451 | -4.86% | 14.7% | 63.7% | 21.6% |
| 2022 | 14.433 | 13.794 | -4.43% | 0.0% | 94.7% | 5.3% |
| 2023 | 13.766 | 14.141 | +2.73% | 18.7% | 56.5% | 24.8% |
| 2024 | 13.994 | 14.325 | +2.37% | 22.0% | 66.5% | 11.4% |
| 2025 | 14.260 | 14.767 | +3.55% | 31.3% | 46.9% | 21.8% |
| 2026 | 14.905 | 16.151 | +8.36% | 23.3% | 66.3% | 10.5% |

#### 結論: NT別に見ると、2016-2021はFlatでfactorが本当に弱い

20D TOPIX excess median。`Baseline` は同じ期間・同じNT regimeのPrime/TSE1全銘柄median。

| Period | NT regime | Factor | Obs | Factor median | Baseline | Factor - baseline | Win | Severe |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2016-2021 | NT Down <= -3% / 60D | `Momentum Value` | 723 | +0.35% | -1.21% | +1.56% | 50.8% | 5.7% |
| 2016-2021 | NT Down <= -3% / 60D | `Momentum Value + Balanced Sector Strength: Strong` | 124 | +0.98% | -1.21% | +2.19% | 54.8% | 7.3% |
| 2016-2021 | NT Flat +/-3% / 60D | `Momentum Value` | 5,383 | -1.15% | -0.58% | -0.57% | 43.5% | 9.0% |
| 2016-2021 | NT Flat +/-3% / 60D | `Momentum Value + Balanced Sector Strength: Strong` | 1,108 | -1.75% | -0.58% | -1.17% | 40.9% | 11.1% |
| 2016-2021 | NT Up >= +3% / 60D | `Momentum Value` | 798 | -0.04% | -0.66% | +0.61% | 49.5% | 9.5% |
| 2016-2021 | NT Up >= +3% / 60D | `Momentum Value + Balanced Sector Strength: Strong` | 86 | +3.35% | -0.66% | +4.01% | 61.6% | 11.6% |

#### 結論: 2022-2025はNT regimeを問わず `Momentum Value` stackがbaselineを上回る

| Period | NT regime | Factor | Obs | Factor median | Baseline | Factor - baseline | Win | Severe |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2022-2025 | NT Down <= -3% / 60D | `Momentum Value` | 2,551 | +0.43% | -0.50% | +0.92% | 53.2% | 4.9% |
| 2022-2025 | NT Down <= -3% / 60D | `Momentum Value + Balanced Sector Strength: Strong` | 1,474 | +0.49% | -0.50% | +0.98% | 54.3% | 4.4% |
| 2022-2025 | NT Down <= -3% / 60D | `Momentum Value + ATR20 Accel + Balanced Sector Strength: Strong` | 168 | +3.05% | -0.50% | +3.55% | 73.2% | 0.6% |
| 2022-2025 | NT Flat +/-3% / 60D | `Momentum Value` | 6,922 | +1.55% | -0.41% | +1.96% | 59.3% | 4.9% |
| 2022-2025 | NT Flat +/-3% / 60D | `Momentum Value + Balanced Sector Strength: Strong` | 3,124 | +2.47% | -0.41% | +2.88% | 65.3% | 4.1% |
| 2022-2025 | NT Flat +/-3% / 60D | `Momentum Value + ATR20 Accel + Balanced Sector Strength: Strong` | 451 | +2.82% | -0.41% | +3.23% | 74.5% | 2.0% |
| 2022-2025 | NT Up >= +3% / 60D | `Momentum Value` | 1,759 | +3.79% | -0.93% | +4.72% | 72.4% | 1.9% |
| 2022-2025 | NT Up >= +3% / 60D | `Momentum Value + Balanced Sector Strength: Strong` | 660 | +5.18% | -0.93% | +6.11% | 78.3% | 0.6% |
| 2022-2025 | NT Up >= +3% / 60D | `Momentum Value + ATR20 Accel + Balanced Sector Strength: Strong` | 146 | +2.78% | -0.93% | +3.71% | 70.5% | 0.0% |

#### 結論: 2026はNT Upでbaselineが大きく崩れるが、factorはbaseline比では残る

| Period | NT regime | Factor | Obs | Factor median | Baseline | Factor - baseline | Win | Severe |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026 | NT Down <= -3% / 60D | `Momentum Value` | 118 | +2.03% | -1.12% | +3.15% | 62.7% | 2.5% |
| 2026 | NT Down <= -3% / 60D | `Momentum Value + Balanced Sector Strength: Strong` | 87 | +2.79% | -1.12% | +3.91% | 67.8% | 0.0% |
| 2026 | NT Down <= -3% / 60D | `Momentum Value + ATR20 Accel + Balanced Sector Strength: Strong` | 36 | +2.35% | -1.12% | +3.47% | 58.3% | 0.0% |
| 2026 | NT Flat +/-3% / 60D | `Momentum Value` | 413 | +3.37% | -2.21% | +5.57% | 63.4% | 8.5% |
| 2026 | NT Flat +/-3% / 60D | `Momentum Value + Balanced Sector Strength: Strong` | 270 | +8.87% | -2.21% | +11.08% | 78.1% | 4.8% |
| 2026 | NT Up >= +3% / 60D | `Momentum Value` | 44 | -1.24% | -4.58% | +3.35% | 45.5% | 25.0% |

#### 結論: `Momentum Value + Balanced Sector Strength: Strong` は銀行セクター依存で、cross-sectorには頑強ではない

20D TOPIX excess median。`All` / `Banks only` / `ex Banks` は同じ年次・同じfactor条件で分けたもの。銀行業は structurally undervalued になりやすく、`Momentum Value + Balanced Sector Strength: Strong` では 2022年以降の観測が銀行業に強く集中する。

| Year | Factor | All | Banks only | ex Banks | Bank share |
| --- | --- | ---: | ---: | ---: | ---: |
| 2021 | `Momentum Value + Balanced Sector Strength: Strong` | -3.20% | -6.96% | -0.49% | 42.8% |
| 2022 | `Momentum Value + Balanced Sector Strength: Strong` | +3.23% | +4.04% | -0.08% | 82.2% |
| 2023 | `Momentum Value + Balanced Sector Strength: Strong` | +1.57% | +1.69% | +1.19% | 66.2% |
| 2024 | `Momentum Value + Balanced Sector Strength: Strong` | +0.49% | +0.77% | -1.06% | 77.4% |
| 2025 | `Momentum Value + Balanced Sector Strength: Strong` | +2.70% | +3.73% | -0.80% | 74.0% |
| 2026 | `Momentum Value + Balanced Sector Strength: Strong` | +6.68% | +8.19% | -4.06% | 86.0% |

`Momentum Value` 単体は `Balanced Sector Strength: Strong` を重ねた条件より銀行業依存が弱い。ただし 2025/2026 は銀行業が全体のheadlineをかなり押し上げるため、これも頑強な strategy と呼ぶにはまだ弱い。

| Year | Factor | All | Banks only | ex Banks | Bank share |
| --- | --- | ---: | ---: | ---: | ---: |
| 2021 | `Momentum Value` | -0.69% | -3.95% | -0.19% | 25.1% |
| 2022 | `Momentum Value` | +2.02% | +3.34% | +0.52% | 51.8% |
| 2023 | `Momentum Value` | +1.74% | +1.82% | +1.70% | 38.0% |
| 2024 | `Momentum Value` | -0.03% | +0.64% | -0.72% | 37.4% |
| 2025 | `Momentum Value` | +2.05% | +4.02% | +0.19% | 52.8% |
| 2026 | `Momentum Value` | +2.61% | +7.48% | -3.99% | 60.2% |

NT Flatだけに絞っても同じ問題は残る。`Momentum Value + Balanced Sector Strength: Strong` の ex Banks は 2022 `-0.09%`、2024 `-1.04%`、2025 `-1.14%`、2026 `-4.56%` で、All-sector headline とかなり違う。2021の悪さは銀行業除外で `-3.94%` から `-2.11%` まで軽くなるが、ex Banksでもまだ弱い。

### Interpretation

NT regimeを入れると、従来の「2016-2021がマイナスに見えすぎる」違和感は2つに分かれる。ひとつは、Prime/TSE1 universe baselineがTOPIX比で弱い市場構造。もうひとつは、2018/2020/2021のように、`Momentum Value` や `Momentum Value + Balanced Sector Strength: Strong` がbaseline比でも弱い本当のfactor不調。

`Balanced Sector Strength: Strong` 単体は、多くのperiod/regimeでbaselineを大きく上回らない。現在の `Balanced Sector Strength` 定義はDaily Ranking SoT通り、公式33業種指数scoreと構成銘柄scoreの2種類の平均を使うsector overlayだが、NT regime上は単体alphaというより `Momentum Value` と組み合わせた確認条件として読む方が妥当。

銀行業除外は、2020/2021の銀行業ショックだけを説明するものではない。2021の `Momentum Value + Balanced Sector Strength: Strong` は銀行業がかなり悪く、銀行業除外で大きく改善する。一方で 2022-2026 は逆に銀行業が強さの大部分を作っており、銀行業を除くと `Balanced Sector Strength: Strong` を重ねた複合条件はstrategyとして成立していない。したがって `Momentum Value + Balanced Sector Strength: Strong` は銀行業への条件付きbetとして再分類し、cross-sector factor strategy としては採用しない。

`Momentum Value + ATR20 Accel + Balanced Sector Strength: Strong` は 2022-2025 で強く、2026 Downでも悪くない。ただし sample が `118-451` 程度、2026は年次でも `50` に留まる。現段階では production rule ではなく、`Momentum Value + Balanced Sector Strength: Strong` の上位confidence候補として、walk-forwardで確認する。

### Production Implication

| Rule candidate | Implication |
| --- | --- |
| `NT 60D Regime` | 売買シグナルではなく、factor result と benchmark headwind を分ける diagnostic / confidence guard |
| `Momentum Value` | 2022-2026ではbaseline比で一貫して残る。2016-2021 Flatでは caution |
| `Momentum Value + Balanced Sector Strength: Strong` | 銀行業依存が大きく、cross-sector strategy候補から降格。銀行業bet / sector concentration diagnostic として扱う |
| `Momentum Value + ATR20 Accel + Balanced Sector Strength: Strong` | headlineは強いがsample小かつ銀行業集中問題を引き継ぐ。production候補にしない |
| `Balanced Sector Strength: Strong` 単体 | 単体alphaではなく、`Momentum Value` のconfirmationとして扱う |
| Bank Exclusion | 銀行業が structurally undervalued になりやすいため、banks share / ex Banks が悪い stack はstrategy evidenceとして採用しない |
| `NT Up >= +3% / 60D` | TOPIX excess のbaselineが崩れやすい。factor絶対値ではなくbaseline比も併記して判定する |
| Low Breadth | 既存通り long confidenceを下げる market guard。NT regimeと併用して読む |

### Caveats

- 2026 は `2026-05-14` までの partial year。60D forward return は2026後半ほど有効観測が減る。
- `Low Breadth` / `Mid Breadth` / `High Breadth` は anchor date 時点の20Dまたは60D cross-section breadthで、future outcomeから作っていない。
- `NT 60D Regime` は anchor date 時点の `N225_UNDERPX close / TOPIX close` の60営業日変化で、future outcomeから作っていない。
- `Balanced Sector Strength` は Daily Ranking と同じく、公式33業種指数scoreと構成銘柄scoreの平均を前提にしたsector overlay。market breadth / NT regime とは別軸。
- `bank_exclusion_df` は `sector_33_name = '銀行業'` を Banks only、それ以外を ex Banks として分ける。sector分類自体の欠損や名称変更は別途coverage diagnosticsで確認する。
- factor signal は daily close-to-close forward TOPIX excess の association study であり、実行コスト、turnover、capacity は含まない。
- `ATR20 Accel` は close 後に確定するため、production entry timing は別途検証が必要。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/ranking_core_factor_regime_breakdown.py`
- Runner: `apps/bt/scripts/research/run_ranking_core_factor_regime_breakdown.py`
- Tests: `apps/bt/tests/unit/domains/analytics/test_ranking_core_factor_regime_breakdown.py`
- Bundle: `/tmp/trading25-research/market-behavior/ranking-core-factor-regime-breakdown/20260603_ranking_core_factor_breadth_nt_bank_exclusion_prime_v7/`
- Result tables: `year_factor_spread_df`, `year_breadth_summary_df`, `annual_factor_breadth_df`, `nt_ratio_regime_summary_df`, `factor_nt_regime_df`, `bank_exclusion_df`, `factor_resilience_df`, `core_failure_decomposition_df`, `sector_year_contribution_df`, `current_term_mapping_df`

## Current Surface

- Domain:
  - `apps/bt/src/domains/analytics/ranking_core_factor_regime_breakdown.py`
- Runner:
  - `apps/bt/scripts/research/run_ranking_core_factor_regime_breakdown.py`
- Bundle:
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_ranking_core_factor_regime_breakdown.py \
  --output-root /tmp/trading25-research \
  --run-id 20260603_ranking_core_factor_breadth_nt_bank_exclusion_prime_v7 \
  --start-date 2016-05-17 \
  --end-date 2026-05-14 \
  --markets prime \
  --horizons 5,10,20,60 \
  --min-observations 20 \
  --notes "Annual factor x breadth x NT 60D regime with bank exclusion diagnostics"
```
