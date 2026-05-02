# Annual Value Composite Technical Filter

[`annual-value-composite-selection`](../annual-value-composite-selection/README.md)
の選定銘柄に、entry 前営業日時点の `SMA250` trend filter を後掛けして、低PBR・小型・低forward PER composite の左尾を削れるかを見る研究。

## Published Readout

### Decision

`price > SMA250` hard filter は annual value composite には採用しない。逆向きの `price < SMA250` 単体も standalone filter としては採用しない。今回の成果は、`2017` の `SMA250 missing` が DB 左端 warmup 不足であり、これを混ぜた baseline 比較は歪むと確認できたこと、そして `TOPIX < SMA250` の market-stress regime で Standard value top decile が強く見える仮説を得たこと。

今後の value research では fixed `55/25/20` score（`fixed_55_25_20`、ユーザー shorthand: `fixed_55_20`）を主導 score として使わない。readout は `equal_weight`、`walkforward_regression_weight`、または Ranking で使う market-specific score profile を優先する。既存 bundle の比較行として残っている fixed score は historical context としてだけ扱う。

### Why This Research Was Run

annual value composite は年初 entry / 年末 exit の年次 return を見るため、テクニカル条件も entry 前営業日時点で固定できる。`price > SMA250` が value trap を削るのか、あるいは反転初動を捨ててしまうのかを portfolio lens で確認する。

### Data Scope / PIT Assumptions

入力は positive-ratio value bundle `/tmp/trading25-research/market-behavior/annual-value-composite-selection/20260502_share_basis_positive/`。initial focus run は `standard` / no liquidity / top `10%` / `fixed_55_25_20`, `walkforward_regression_weight`, `equal_weight` に限定した。今後の rerun focus は `fixed_55_25_20` を除き、`walkforward_regression_weight` と `equal_weight` を使う。technical feature は parent `market.duckdb` から selected event code の日足を読み、`entry_date` より前の最新 trading session だけで `SMA250` / `price_to_sma250` / `SMA250 slope` を計算する。entry 当日以降の価格は filter 判定に使わない。

### Main Findings

#### 結論

| Filter | Status |
| --- | --- |
| `baseline` | upstream value selection をそのまま評価 |
| `stock_above_sma250` | 個別銘柄の長期 trend filter |
| `topix_above_sma250` | 市場全体の risk-on/off filter |
| `stock_and_topix_above_sma250` | 銘柄 trend と市場 regime の AND |
| `stock_near_or_above_sma250` | 反転初動を残す緩い銘柄 filter |
| `stock_above_sma250_or_positive_slope` | SMA 上抜け前の改善傾向を残す緩い filter |
| `stock_below_sma250` | 個別銘柄が `SMA250` を下回る逆向き filter |
| `stock_below_sma250_and_topix_above_sma250` | 銘柄だけ弱く、市場は上向きの filter |
| `stock_and_topix_below_sma250` | 銘柄も市場も `SMA250` を下回る bear-regime filter |

#### 結論

| Score | Filter | Events | Kept | CAGR | Sharpe | MaxDD | Worst |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `walkforward_regression_weight` | `baseline` | `829` | `100.0%` | `34.71%` | `2.13` | `-30.14%` | `-57.21%` |
| `walkforward_regression_weight` | `stock_above_sma250` | `364` | `43.9%` | `27.83%` | `1.70` | `-30.19%` | `-57.21%` |
| `walkforward_regression_weight` | `stock_above_sma250_or_positive_slope` | `464` | `56.0%` | `26.88%` | `1.76` | `-29.66%` | `-57.21%` |
| `walkforward_regression_weight` | `stock_and_topix_above_sma250` | `308` | `37.2%` | `16.51%` | `1.39` | `-38.47%` | `-57.21%` |
| `equal_weight` | `baseline` | `829` | `100.0%` | `34.68%` | `2.12` | `-29.79%` | `-57.21%` |
| `equal_weight` | `stock_above_sma250` | `377` | `45.5%` | `27.81%` | `1.72` | `-29.65%` | `-57.21%` |
| `fixed_55_25_20` | `baseline` | `829` | `100.0%` | `34.58%` | `2.11` | `-30.44%` | `-57.21%` |
| `fixed_55_25_20` | `stock_above_sma250` | `345` | `41.6%` | `26.09%` | `1.59` | `-30.67%` | `-57.21%` |
| `fixed_55_25_20` | `stock_below_sma250` | `382` | `46.1%` | `23.23%` | `1.52` | `-33.02%` | `-57.21%` |
| `fixed_55_25_20` | `stock_below_sma250_and_topix_above_sma250` | `256` | `30.9%` | `11.66%` | `1.01` | `-46.41%` | `-57.21%` |
| `fixed_55_25_20` | `stock_and_topix_below_sma250` | `126` | `15.2%` | `17.12%` | `4.32` | `-6.59%` | `-31.04%` |

### Interpretation

今回の readout で一番重要なのは、SMA filter の勝ち負けではなく、`SMA250 missing` の正体だった。`2017` は source `stock_data` の左端が `2016-05-02` であるため、entry 前に `250` close rows を作れない。しかも 2017 の Standard value top decile は非常に強く、`price > SMA250` / `price < SMA250` filter はこの強い年を丸ごと落としていた。したがって、`SMA250` 関連の比較は `2017` と short-history names を分けなければ解釈できない。

この結果はかなりはっきりしている。`stock_above_sma250` は event 数を約 `42-45%` まで減らす一方、worst trade は `-57.21%` のまま残り、CAGR / Sharpe は baseline から明確に低下した。`TOPIX > SMA250` を重ねるとさらに悪く、market regime filter としてもこの年次 value selection には合わない。

読みとしては、annual value composite の edge は「すでに上昇 trend にいる value」よりも、年初時点ではまだ `SMA250` 近辺または下にいる反転候補を含んでいる可能性が高い。したがって `price > SMA250` は bad-tail pruning ではなく、右尾と平均を削る filter として働いている。

逆に `stock_below_sma250` 単体は standalone filter としては十分ではない。ただし `stock_and_topix_below_sma250` は 2019 と 2023 だけに限定され、event-level p10 が `-1.11%`、worst が `-31.04%` まで改善した。これは hard rule としては年数が少なすぎるが、「市場全体も長期線を下回った局面で、Standard value top decile が反発しやすい」という別仮説としては面白い。市場が弱いときは momentum より value / low expectation 側へ資金が戻る、という解釈と整合的。

### Missing Diagnostics

`SMA250 missing` は alpha bucket として読んではいけない。`fixed_55_25_20` の focus run では `stock_price_to_sma250 IS NULL` が `102` events あり、そのうち `68` events は `2017` の DB 左端 warmup 不足だった。source `stock_data` は `2016-05-02` 開始なので、`2017-01-04` entry では最大でも `164` prior close rows しかなく、どの銘柄も `SMA250` を作れない。

| History class | Events | Years | Median prior rows | Mean return | Median return | P10 | Worst |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `has_250_prior_closes` | `727` | `8` | `1214` | `22.42%` | `10.51%` | `-18.52%` | `-57.21%` |
| `db_left_boundary_2017` | `68` | `1` | `161` | `69.34%` | `56.23%` | `15.54%` | `-6.80%` |
| `short_history_lt250_prior_closes` | `34` | `9` | `130.5` | `28.19%` | `4.97%` | `-27.16%` | `-57.21%` |

このため、baseline が `price > SMA250` / `price < SMA250` の両方より強く見える主因の一つは、強い `2017` 年を technical filter が丸ごと落としていること。`2017` を除外すると baseline は mean `21.94%` / median `10.05%`、`has_250_prior_closes` のみでは mean `22.42%` / median `10.51%` まで下がる。`SMA250` 解析は今後、`SMA250 calculable universe` と `short-history/new-listing universe` を分けて扱う。

### Excluding 2017 / SMA250-Calculable Effect

`2017` と `SMA250 missing` を除き、2018-2025 の `SMA250 calculable` universe だけで見ると、`price_to_sma250` は単純な hard filter では弱い。`fixed_55_25_20` では `below` が `above` より event-level return と左尾で良いが、year-neutral correlation はほぼゼロから小さなマイナスに留まる。

| Score | Side | Events | Years | Mean | Median | P10 | Worst |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `fixed_55_25_20` | `above` | `345` | `8` | `17.60%` | `7.14%` | `-22.94%` | `-57.21%` |
| `fixed_55_25_20` | `below` | `382` | `8` | `26.77%` | `14.36%` | `-15.61%` | `-57.21%` |
| `equal_weight` | `above` | `377` | `8` | `19.73%` | `8.40%` | `-22.70%` | `-57.21%` |
| `equal_weight` | `below` | `353` | `8` | `28.27%` | `15.25%` | `-13.48%` | `-57.21%` |
| `walkforward_regression_weight` | `above` | `364` | `8` | `19.65%` | `7.46%` | `-22.67%` | `-57.21%` |
| `walkforward_regression_weight` | `below` | `363` | `8` | `27.44%` | `14.65%` | `-12.98%` | `-57.21%` |

`fixed_55_25_20` は今後の主導 score から外すが、この historical run での `price_to_sma250` 年別相関は平均 `-0.039`、中央値 `-0.005`。つまり「SMA250 から遠く上にいるほど良い/悪い」という連続的な強い関係は確認できない。年別には 2019 / 2021 / 2022 で `below` が強く、2025 は `above` が強い。

| Year | Events | Corr | Below mean | Above mean |
| --- | ---: | ---: | ---: | ---: |
| `2018` | `79` | `0.038` | `-22.60%` | `-5.83%` |
| `2019` | `77` | `-0.339` | `39.53%` | `20.85%` |
| `2020` | `77` | `0.072` | `3.35%` | `13.13%` |
| `2021` | `61` | `-0.148` | `29.06%` | `12.92%` |
| `2022` | `68` | `-0.177` | `18.46%` | `11.80%` |
| `2023` | `105` | `0.030` | `41.00%` | `43.79%` |
| `2024` | `128` | `-0.040` | `17.73%` | `13.77%` |
| `2025` | `132` | `0.247` | `31.00%` | `58.81%` |

TOPIX との interaction は別物として残る。`stock_above_topix_below` と `stock_below_topix_below` は 2 年だけの small regime だが、mean / p10 が強い。これは `price_to_sma250` 単体効果ではなく、市場全体が `SMA250` を下回った局面で value rebound が出た可能性として読む。

| Score | Regime | Events | Years | Mean | Median | P10 | Worst |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `fixed_55_25_20` | `stock_above_topix_above` | `289` | `6` | `12.85%` | `4.43%` | `-25.84%` | `-57.21%` |
| `fixed_55_25_20` | `stock_above_topix_below` | `56` | `2` | `42.15%` | `30.64%` | `-2.97%` | `-17.37%` |
| `fixed_55_25_20` | `stock_below_topix_above` | `256` | `6` | `20.19%` | `8.43%` | `-17.12%` | `-57.21%` |
| `fixed_55_25_20` | `stock_below_topix_below` | `126` | `2` | `40.15%` | `23.86%` | `-1.11%` | `-31.04%` |

### Production Implication

Ranking には `price > SMA250` / `price < SMA250` を hard filter として入れない。表示する場合も `Value Score` と technical diagnostic を分け、ユーザーが「trend 状態」を見る補助情報に留める。次に検証するなら、`stock_and_topix_below_sma250` をそのまま採用するのではなく、market stress / rebound bucket として year split、full-calendar idle-day portfolio、Prime/Standard split を確認する。

Value research の score readout は今後 `fixed_55_25_20` を主役にしない。Ranking 実装の market-specific score profile と、研究用の `equal_weight` / `walkforward_regression_weight` を優先する。

### Caveats

年次 rebalance 研究なので、実運用の約定コスト、流動性制約、年中の再判定、position sizing は未反映。`SMA250` は十分な日足履歴がない entry では欠損になり、hard filter では pass しない。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/annual_value_composite_technical_filter.py`
- Runner: `apps/bt/scripts/research/run_annual_value_composite_technical_filter.py`
- Bundle: `/tmp/trading25-research/market-behavior/annual-value-composite-technical-filter/20260502_value_composite_technical_positive_focus/`
- Results DB: `/tmp/trading25-research/market-behavior/annual-value-composite-technical-filter/20260502_value_composite_technical_positive_focus/results.duckdb`
- Summary: `/tmp/trading25-research/market-behavior/annual-value-composite-technical-filter/20260502_value_composite_technical_positive_focus/summary.md`
- Below-SMA rerun bundle: `/tmp/trading25-research/market-behavior/annual-value-composite-technical-filter/20260502_value_composite_technical_below_positive_focus/`

## Current Surface

- Input bundle: `annual-value-composite-selection`
- Output tables:
  - `enriched_selected_event_df`
  - `technical_filter_event_df`
  - `technical_filter_summary_df`
  - `portfolio_daily_df`
  - `portfolio_summary_df`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_value_composite_technical_filter.py \
  --output-root /tmp/trading25-research
```

Positive-ratio value run を明示する場合:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_value_composite_technical_filter.py \
  --input-bundle /tmp/trading25-research/market-behavior/annual-value-composite-selection/20260502_share_basis_positive \
  --output-root /tmp/trading25-research \
  --focus-standard-top10-no-liquidity
```
