# Annual Large-Universe Factor Family

`annual-large-universe-value-profile` の follow-up。`TOPIX100` / `TOPIX500`
相当の大型 universe で、低 `forward PER` を補強できる fundamental factor を
valuation / yield / quality / payout / forecast quality まで広げて確認する。

## Published Readout

### Decision

大型 universe で追加検討すべき fundamental は、`CFO yield` や営業利益率よりも
配当系と一部 quality。`TOPIX100` は `dividend_yield_pct` /
`forecast_dividend_yield_pct` が低 `forward PER` と同等以上に見える。`TOPIX500`
は低 `forward PER` がまだ主軸だが、top `5%` では forecast dividend yield 単独や
`forward PER + ROE/ROA/dividend` overlay が改善余地を示す。`CFO yield`、
`CFO margin`、`CFO / net profit`、営業利益率は、今回の大型 universe では主役候補に
しない。

### Why This Research Was Run

大型 universe では Prime / Standard より return profile が弱く、小型 factor の
独立効果も薄かった。低 `forward PER` が主役に見えるため、`CFO yield`、配当、
営業利益率、ROE、cash conversion などを横断的に見直し、低 `forward PER` を
補強する factor があるかを確認する。

### Data Scope / PIT Assumptions

入力は v3 annual first-open / last-close fundamental panel
`/tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/20260502_share_basis_rerun/`。
`PBR > 0` と `forward PER > 0` を要求し、`32,264` realized events から
large-universe 展開後 `4,523` rows を作った。universe 判定は各 entry date の
`stock_master_daily.scale_category` を使い、`TOPIX100` は
`TOPIX Core30 + TOPIX Large70`、`TOPIX500` はそれに `TOPIX Mid400` を加えた
scope とする。全 factor score は `year x large_universe` 内で再rankする。

### Main Findings

#### 結論

##### 検討した factor family

| Family | Factors |
| --- | --- |
| valuation | `forward_per`, `per`, `pbr` |
| size | `market_cap_bil_jpy` |
| yield | `cfo_yield_pct`, `fcf_yield_pct`, `dividend_yield_pct`, `forecast_dividend_yield_pct` |
| quality | `roe_pct`, `roa_pct`, `operating_margin_pct`, `net_margin_pct`, `cfo_margin_pct`, `fcf_margin_pct`, `equity_ratio_pct` |
| cash conversion / payout | `cfo_to_net_profit_ratio`, `payout_ratio_pct`, `forecast_payout_ratio_pct` |
| forecast quality | `forward_eps_to_actual_eps` |

##### Core controls 後の追加効果

`event_return_winsor_pct` に対する OLS。candidate が core factor 以外のときは
`low_forward_per_score + low_pbr_score + small_market_cap_score + candidate`、
固定効果は `year` と `sector_33_name`。係数は `1sd` あたりの pp。

| Universe | Factor | Coef / t | Read |
| --- | --- | ---: | --- |
| `topix100` | high forecast dividend yield | `+4.38pp / t=1.80` | sample は小さいが最も良い追加候補。 |
| `topix100` | high dividend yield | `+2.76pp / t=1.53` | 配当系は大型上位で有望。 |
| `topix100` | high forecast payout ratio | `+2.11pp / t=1.55` | 配当系の proxy としては残る。 |
| `topix100` | high CFO yield | `-0.09pp / t=-0.05` | CFO yield は補強になっていない。 |
| `topix100` | high operating margin | `-0.46pp / t=-0.28` | 営業利益率は弱い。 |
| `topix500` | high ROE | `+1.16pp / t=1.81` | quality では最も見る価値あり。 |
| `topix500` | high forecast dividend yield | `+0.80pp / t=0.97` | 単独 bucket / portfolio ほど独立効果は強くない。 |
| `topix500` | low PER | `+0.78pp / t=1.03` | forward PER には劣るが補助候補。 |
| `topix500` | high operating margin | `-0.16pp / t=-0.24` | 営業利益率は補強になっていない。 |
| `topix500` | high CFO yield | `-0.76pp / t=-1.21` | CFO yield は core control 後に弱い。 |
| `topix500` | high CFO / net profit | `-1.45pp / t=-2.96` | cash conversion は大型 value では逆効果寄り。 |

##### Bucket spread

各 factor を `year x large_universe` 内 Q1-Q5 に切り、preferred Q5 から opposite Q1
を引いた mean return spread。

| Universe | Strong positive spreads | Weak / negative spreads |
| --- | --- | --- |
| `topix100` | forecast dividend `+17.19pp`, dividend `+16.42pp`, low forward PER `+15.13pp`, low PER `+13.53pp`, low PBR `+13.42pp` | operating margin `-2.13pp`, equity ratio `-4.12pp`, CFO margin `-4.94pp`, CFO / net profit `-5.35pp` |
| `topix500` | low PBR `+12.55pp`, low forward PER `+12.34pp`, forecast dividend `+12.29pp`, dividend `+12.14pp`, low PER `+12.01pp` | operating margin `-0.91pp`, FCF margin `-2.39pp`, equity ratio `-2.50pp`, CFO margin `-4.12pp`, CFO / net profit `-6.05pp` |

##### Portfolio lens

No liquidity floor。年初 open で採用、年末 close まで等ウェイト保有。

| Universe | Top | Score profile | Events | CAGR | Sharpe | MaxDD |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| `topix100` | `5%` | low `forward PER` only | `43` | `20.86%` | `1.18` | `-24.60%` |
| `topix100` | `5%` | low `forward PER` + forward EPS / actual EPS | `43` | `24.99%` | `1.15` | `-35.85%` |
| `topix100` | `5%` | low `forward PER` + forecast dividend yield | `35` | `19.47%` | `1.11` | `-24.08%` |
| `topix100` | `10%` | forecast dividend yield only | `67` | `21.50%` | `1.19` | `-26.26%` |
| `topix100` | `10%` | dividend yield only | `79` | `21.59%` | `1.17` | `-28.01%` |
| `topix100` | `10%` | low `forward PER` only | `79` | `19.16%` | `1.06` | `-27.31%` |
| `topix500` | `5%` | forecast dividend yield only | `173` | `20.35%` | `1.13` | `-33.25%` |
| `topix500` | `5%` | low `forward PER` + ROE | `192` | `19.26%` | `1.10` | `-28.32%` |
| `topix500` | `5%` | low `forward PER` + dividend yield | `192` | `19.88%` | `1.09` | `-32.68%` |
| `topix500` | `5%` | low `forward PER` only | `192` | `19.83%` | `1.07` | `-31.63%` |
| `topix500` | `10%` | low `forward PER` + small market cap | `380` | `18.03%` | `1.06` | `-32.79%` |
| `topix500` | `10%` | low `forward PER` + ROA | `380` | `16.11%` | `0.98` | `-28.97%` |
| `topix500` | `10%` | low `forward PER` + operating margin | `352` | `16.01%` | `0.98` | `-29.22%` |
| `topix500` | `10%` | low `forward PER` only | `380` | `16.94%` | `0.95` | `-35.52%` |

### Interpretation

`TOPIX100` は sample が小さいため断定は避けるが、配当系が明確に候補へ浮上した。
top `10%` では dividend / forecast dividend 単独が低 `forward PER` 単独を上回り、
大型・高配当の defensive value sleeve として読む価値がある。top `5%` では低
`forward PER` の安定性がまだ強く、forecast quality overlay は CAGR を上げるが
drawdown も深くなる。

`TOPIX500` は、低 `forward PER` を主軸にする前回判断は維持。ただし top `5%` では
forecast dividend yield 単独、または `forward PER + ROE/ROA/dividend` が同等以上の
候補になる。top `10%` では `forward PER + small market cap` がまだ最上位で、quality
overlay は drawdown を浅くする代わりに return を少し落とす。

営業利益率、CFO yield、CFO margin、CFO / net profit は、単独 bucket か回帰のどちらか、
または両方で弱い。大型 universe では「収益性が高い会社」より、「低 forward PER と
配当・一部 ROE/ROA の組み合わせ」の方が実用的に見える。

### Production Implication

次に実装候補へ落とすなら、大型 universe は `large_forward_per_dividend` と
`large_forward_per_quality` の2本に分ける。`TOPIX100` は dividend / forecast dividend
を独立 sleeve として検討、`TOPIX500` は低 `forward PER` primary に `ROE/ROA` または
配当を軽く足す。`CFO yield` と営業利益率は、少なくともこの annual large-universe
profile では優先度を下げる。

### Caveats

この研究は annual open-to-close の等ウェイト portfolio lens で、コスト、
スリッページ、capacity、turnover、borrowability は含まない。多数 factor を
見るため、単独上位の見かけだけでなく、core controls 入り回帰と低 `forward PER`
overlay portfolio の両方で読む。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/annual_large_universe_factor_family.py`
- Runner: `apps/bt/scripts/research/run_annual_large_universe_factor_family.py`
- Bundle: `/tmp/trading25-research/market-behavior/annual-large-universe-factor-family/20260506_large_universe_factor_family/`

## Current Surface

- Domain:
  - `apps/bt/src/domains/analytics/annual_large_universe_factor_family.py`
- Runner:
  - `apps/bt/scripts/research/run_annual_large_universe_factor_family.py`
- Bundle:
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_large_universe_factor_family.py \
  --output-root /tmp/trading25-research \
  --input-bundle /tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/20260502_share_basis_rerun \
  --run-id 20260506_large_universe_factor_family
```
