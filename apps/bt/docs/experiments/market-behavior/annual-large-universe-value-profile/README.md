# Annual Large-Universe Value Profile

`annual-first-open-last-close-fundamental-panel` を土台に、年初 entry 時点の
`stock_master_daily.scale_category` から `TOPIX100` / `TOPIX500` 相当の大型
universe に絞り、低 `PBR`・小型・低 `forward PER` のバランスを再評価する研究。

## Published Readout

### Decision

`TOPIX100` / `TOPIX500` の大型 universe では、Standard のような低 `PBR`
heavy profile ではなく、低 `forward PER` を主役にする方が自然。低 `PBR` は
補助因子として残るが、小型はこの universe では独立 alpha としては弱く、特に
`TOPIX500` では係数がほぼ消える。Ranking / research 上は、大型 sleeve を
Prime / Standard とは別 profile として読み、低 `forward PER` primary、低 `PBR`
secondary、小型は tie-break / diversification 程度に留める。

### Why This Research Was Run

Prime / Standard では、小型・低 `PBR`・低 `forward PER` の効き方が別バランスに
なることが分かった。さらに大型の universe、特に `TOPIX100` と `TOPIX500` に絞ると
この value composite がどう変わるかを、market split とは別軸で確認する。

### Data Scope / PIT Assumptions

入力は v3 annual first-open / last-close fundamental panel
`/tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/20260502_share_basis_rerun/`。
`PBR > 0` と `forward PER > 0` を要求し、`32,264` realized events から
large-universe 展開後 `4,523` rows を作った。universe 判定は各 entry date の
`stock_master_daily.scale_category` を使い、`TOPIX100` は
`TOPIX Core30 + TOPIX Large70`、`TOPIX500` はそれに `TOPIX Mid400` を加えた
scope とする。score は `year x large_universe` 内で再rankし、Prime / Standard の
rank を流用しない。

### Main Findings

#### 結論

##### 独立効果

`event_return_winsor_pct` に対する core 3 score の OLS。固定効果は `year` と
`sector_33_name`。係数は `1sd` あたりの pp。

| Universe | Low PBR | Small market cap | Low forward PER | Read |
| --- | ---: | ---: | ---: | --- |
| `topix100` | `+3.46pp / t=1.61` | `+1.30pp / t=0.97` | `+1.93pp / t=0.92` | sample が小さく t は弱いが、PBR と forward PER が残り、小型は薄い。 |
| `topix500` | `+2.21pp / t=3.00` | `+0.16pp / t=0.33` | `+3.25pp / t=4.91` | 低 `forward PER` が最も強く、低 `PBR` が次点。小型の独立効果はほぼ消える。 |

##### Bucket spread

各 factor を `year x large_universe` 内で Q1-Q5 に切り、preferred Q5 から
opposite Q1 を引いた mean return spread。

| Universe | Low PBR spread | Small market cap spread | Low forward PER spread |
| --- | ---: | ---: | ---: |
| `topix100` | `+13.42pp` | `+11.38pp` | `+15.13pp` |
| `topix500` | `+12.55pp` | `+6.11pp` | `+12.34pp` |

##### Portfolio lens

No liquidity floor。年初 open で採用、年末 close まで等ウェイト保有。

| Universe | Top | Score profile | Events | CAGR | Sharpe | MaxDD |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| `topix100` | `5%` | low `forward PER` only | `43` | `20.86%` | `1.18` | `-24.60%` |
| `topix100` | `5%` | low `PBR` 50% / low `forward PER` 50% | `43` | `22.41%` | `1.12` | `-30.77%` |
| `topix100` | `10%` | low `forward PER` only | `79` | `19.16%` | `1.06` | `-27.31%` |
| `topix100` | `10%` | equal weight | `79` | `19.25%` | `1.04` | `-29.01%` |
| `topix500` | `5%` | low `forward PER` only | `192` | `19.83%` | `1.07` | `-31.63%` |
| `topix500` | `5%` | `standard_pbr_tilt` | `192` | `16.92%` | `0.95` | `-41.96%` |
| `topix500` | `10%` | `prime_size_tilt` | `380` | `18.26%` | `1.06` | `-33.21%` |
| `topix500` | `10%` | small 50% / low `forward PER` 50% | `380` | `18.03%` | `1.06` | `-32.79%` |

### Interpretation

大型 universe は「さらに大きい Prime」ではなく、value factor の中身が変わる。
`TOPIX100` は sample が `758` と小さく統計的には弱いが、portfolio lens では低
`forward PER` 単独が最も Sharpe が高く、低 `PBR` を混ぜると CAGR は上がる一方で
drawdown が深くなる。`TOPIX500` は `3,765` observations あり、低 `forward PER`
の独立係数が最も強い。低 `PBR` も有効だが、Standard の `PBR` heavy profile を
そのまま持ち込むと drawdown が悪化しやすい。

小型 factor は、Prime / Standard の広い universe では効いたが、`TOPIX100/500`
内で再rankすると独立効果は弱い。`TOPIX500` top `10%` で `prime_size_tilt` や
small + forward PER が良いのは、純粋な小型 alpha というより、低 `forward PER`
selection を少し分散させる tie-break と読む方がよい。

### Production Implication

大型 universe 用の profile を作るなら、初期案は低 `forward PER` primary にする。
`TOPIX100` は低 `forward PER` 単独、または低 `PBR` を少し混ぜた conservative
profile。`TOPIX500` は低 `forward PER` primary、低 `PBR` secondary、小型は
minor tie-break。Standard 用の `standard_pbr_tilt` を大型 universe に流用しない。

### Caveats

この研究は annual open-to-close の等ウェイト portfolio lens で、コスト、スリッページ、
capacity、turnover、borrowability は含まない。`TOPIX500` は `TOPIX100` を内包するため、
両者は独立サンプルではなく、大型寄りから中型寄りへ広げたときの差分として読む。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/annual_large_universe_value_profile.py`
- Runner: `apps/bt/scripts/research/run_annual_large_universe_value_profile.py`
- Bundle: `/tmp/trading25-research/market-behavior/annual-large-universe-value-profile/20260506_large_universe_value_profile_v2/`

## Current Surface

- Domain:
  - `apps/bt/src/domains/analytics/annual_large_universe_value_profile.py`
- Runner:
  - `apps/bt/scripts/research/run_annual_large_universe_value_profile.py`
- Bundle:
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_large_universe_value_profile.py \
  --output-root /tmp/trading25-research \
  --input-bundle /tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/20260502_share_basis_rerun \
  --run-id 20260506_large_universe_value_profile
```
