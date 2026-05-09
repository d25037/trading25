# New-High Momentum Research

N日新高値をつけた銘柄の、その後の return を event-study で検証する研究。
N は `20 / 60 / 120 / 252` trading days とし、日足確定後にしか分からない signal
なので、評価 return は `next open -> future close` を使う。

## Published Readout

### Decision

N日新高値は、単独の long signal としては採用しない。特に 252d 新高値は
`TOPIX500` と `Growth` で TOPIX excess が悪く、`Standard` でも同日同 universe
baseline に対する lift はマイナスだった。

ただし、新高値は「候補生成」としては残す価値がある。annual value research で効いた
`low PBR` / `low forward PER` / `small cap` を重ねると、単なる `quality_score >= 3`
や出来高条件よりかなり clean になった。採用候補は以下に限定する。

- `Prime ex TOPIX500`: 252d 新高値 + `annual_value_score_3`、または `annual_value_score_ge_2`
- `Standard`: 252d 新高値 + `annual_value_score_3`、`low_pbr_forward_per_15`、`low_forward_per_le_10`
- `TOPIX500`: 252d 新高値 + `low_forward_per_le_15` は候補だが、効果は Prime / Standard より小さい
- `Growth`: 新高値単体は追わない。value 条件付きは強いが event 数が小さく、別研究で扱う

2026-05-09 follow-up の event-driven portfolio lens でも、方向性は同じだった。
「value portfolio に 252d breakout hard filter」ではなく、「252d breakout を event trigger
として、value が強い銘柄だけを保有する」形にすると、Standard は明確に改善する。
特に `standard / annual_value_score_3 / 60d hold` は CAGR `34.73%`、Sharpe `1.47`、
MaxDD `-38.84%` で、252d breakout 単体の `11.20%` / `0.70` を大きく上回った。
一方、Prime ex TOPIX500 は value 条件で改善するが、best は
`low_forward_per_le_10 / 60d hold` の CAGR `19.67%`、Sharpe `1.02` に留まり、
core strategy 候補というより satellite diagnostic に近い。

出来高は単独加点ではない。Prime ex TOPIX500 では volume expansion が 20d/60d
で改善する一方、Standard では `volume_ratio_20d >= 2.0` 単体は悪化し、
`volume_2_without_quality_3` は明確に悪い。出来高は「注目・流動性・crowding」
の混合 proxy として扱い、quality / valuation と組み合わせる。

### Why This Research Was Run

52週高値近辺は、行動ファイナンスの代表的な参照点として扱われる。George and
Hwang (2004) は、52週高値への近さが過去 return より momentum を説明すると報告した。
一方で、Huddart, Lang, and Yetman (2009) や Della Vedova, Grant, and Westerholm
(2023) は、高値更新時の volume / household order flow が投資家行動に強く左右される
ことを示している。したがって、日本株でも「新高値そのもの」ではなく、出来高・価格位置・
ファンダメンタル品質を分けて確認する必要がある。

### Data Scope / PIT Assumptions

`market.duckdb` の `stock_data` / `topix_data` / `stock_master_daily` / `statements`
を直接読む。universe は signal date と同日の `stock_master_daily` で
`TOPIX500` / `Prime ex TOPIX500` / `Standard` / `Growth` に分ける。
`statements` は `disclosed_date <= signal_date` の最新行だけを採用し、future leak を避ける。
`annual_value_score` は `PBR <= 1.0`、`forward PER <= 15`、同日同 universe の
新高値 event 内 market cap 下位 30% を1点ずつ数える。

### Main Findings

#### 結論

252d 新高値単体は、TOPIX excess では Prime ex TOPIX500 / Standard がプラスだが、
同日同 universe の全銘柄 baseline に対しては Standard / Growth で劣後する。
「新高値なら買う」では弱く、市場区分と補助条件が必要。

| Universe | 252d Events | 5d Excess | 20d Excess | 60d Excess | 20d Same-Universe Lift | 20d 5% Loss Rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `TOPIX500` | `55,838` | `-0.06%` | `-0.17%` | `0.04%` | `-0.10%` | `23.7%` |
| `Prime ex TOPIX500` | `130,765` | `-0.02%` | `0.08%` | `0.29%` | `0.02%` | `26.1%` |
| `Standard` | `79,606` | `-0.11%` | `0.34%` | `0.69%` | `-0.48%` | `28.7%` |
| `Growth` | `20,056` | `-0.62%` | `-0.83%` | `-2.80%` | `-0.56%` | `44.4%` |

#### Window Sensitivity

新高値 window を長くしても、単調に良くなるわけではない。Standard は TOPIX excess
だけ見ると 20d/60d/120d/252d すべてプラスだが、same-universe lift は全てマイナス。
Growth はどの window でも悪い。

| Universe | Window | 20d Return | 20d Excess | 20d Same-Universe Lift |
| --- | ---: | ---: | ---: | ---: |
| `TOPIX500` | `20` | `0.30%` | `-0.21%` | `-0.17%` |
| `TOPIX500` | `252` | `0.34%` | `-0.17%` | `-0.10%` |
| `Prime ex TOPIX500` | `20` | `0.51%` | `0.03%` | `-0.08%` |
| `Prime ex TOPIX500` | `252` | `0.66%` | `0.08%` | `0.02%` |
| `Standard` | `20` | `1.04%` | `0.37%` | `-0.45%` |
| `Standard` | `252` | `0.98%` | `0.34%` | `-0.48%` |
| `Growth` | `20` | `0.29%` | `-0.42%` | `-0.51%` |
| `Growth` | `252` | `-0.09%` | `-0.83%` | `-0.56%` |

#### Annual Value 補助条件

annual value research の三本柱を重ねると、結論はかなりはっきりする。
`quality_score >= 3` より、`low forward PER` と `small cap` を含む value 条件の方が強い。

| Universe | 252d Condition | Events | 20d Excess | Lift vs New-High | Same-Universe Lift | 5% Loss Rate |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `Prime ex TOPIX500` | `all` | `130,765` | `0.08%` | `0.00%` | `0.02%` | `26.1%` |
| `Prime ex TOPIX500` | `low_pbr_le_1` | `25,329` | `0.63%` | `0.54%` | `0.51%` | `21.4%` |
| `Prime ex TOPIX500` | `low_forward_per_le_10` | `35,289` | `1.02%` | `0.94%` | `0.96%` | `22.1%` |
| `Prime ex TOPIX500` | `small_market_cap_bottom_30` | `34,155` | `0.89%` | `0.80%` | `0.81%` | `24.3%` |
| `Prime ex TOPIX500` | `annual_value_score_ge_2` | `36,763` | `0.92%` | `0.84%` | `0.82%` | `21.7%` |
| `Prime ex TOPIX500` | `annual_value_score_3` | `9,123` | `1.52%` | `1.44%` | `1.35%` | `20.5%` |
| `Standard` | `all` | `79,606` | `0.34%` | `0.00%` | `-0.48%` | `28.7%` |
| `Standard` | `cfo_positive` | `25,653` | `1.48%` | `1.15%` | `0.64%` | `23.5%` |
| `Standard` | `low_pbr_le_1` | `17,234` | `1.27%` | `0.94%` | `0.42%` | `22.4%` |
| `Standard` | `low_forward_per_le_10` | `24,206` | `1.62%` | `1.29%` | `0.69%` | `22.5%` |
| `Standard` | `low_pbr_forward_per_15` | `13,357` | `1.73%` | `1.39%` | `0.86%` | `20.5%` |
| `Standard` | `annual_value_score_ge_2` | `21,586` | `1.60%` | `1.26%` | `0.69%` | `23.3%` |
| `Standard` | `annual_value_score_3` | `5,048` | `2.97%` | `2.63%` | `1.99%` | `22.1%` |
| `Growth` | `all` | `20,056` | `-0.83%` | `0.00%` | `-0.56%` | `44.4%` |
| `Growth` | `low_pbr_le_1` | `661` | `4.23%` | `5.06%` | `3.33%` | `26.6%` |
| `Growth` | `annual_value_score_ge_2` | `2,161` | `3.54%` | `4.38%` | `3.35%` | `32.2%` |
| `Growth` | `annual_value_score_3` | `410` | `6.09%` | `6.92%` | `4.64%` | `27.8%` |

#### Horizon Stability

Standard / Prime ex TOPIX500 の annual value 条件は 5d から 60d まで改善が残る。
特に `annual_value_score_3` は 60d で大きく伸びる。Growth も value 条件付きは強いが、
410 events の `score_3` は小サンプルなので production 候補にはまだ昇格しない。

| Universe | Condition | 5d Excess | 20d Excess | 60d Excess | 60d Same-Universe Lift |
| --- | --- | ---: | ---: | ---: | ---: |
| `Prime ex TOPIX500` | `all` | `-0.02%` | `0.08%` | `0.29%` | `0.18%` |
| `Prime ex TOPIX500` | `low_pbr_le_1` | `0.14%` | `0.63%` | `1.94%` | `1.59%` |
| `Prime ex TOPIX500` | `low_forward_per_le_15` | `0.15%` | `0.52%` | `1.50%` | `1.54%` |
| `Prime ex TOPIX500` | `annual_value_score_ge_2` | `0.27%` | `0.92%` | `2.49%` | `2.25%` |
| `Prime ex TOPIX500` | `annual_value_score_3` | `0.40%` | `1.52%` | `4.01%` | `3.35%` |
| `Standard` | `all` | `-0.11%` | `0.34%` | `0.69%` | `-0.79%` |
| `Standard` | `low_pbr_le_1` | `0.15%` | `1.27%` | `4.03%` | `2.46%` |
| `Standard` | `low_forward_per_le_15` | `0.23%` | `1.08%` | `2.09%` | `0.90%` |
| `Standard` | `annual_value_score_ge_2` | `0.25%` | `1.60%` | `4.27%` | `2.59%` |
| `Standard` | `annual_value_score_3` | `0.31%` | `2.97%` | `9.75%` | `7.45%` |
| `Growth` | `all` | `-0.62%` | `-0.83%` | `-2.80%` | `-0.60%` |
| `Growth` | `annual_value_score_ge_2` | `0.92%` | `3.54%` | `8.48%` | `9.75%` |
| `Growth` | `annual_value_score_3` | `1.27%` | `6.09%` | `15.99%` | `15.73%` |

#### Event-Driven Portfolio Follow-up

252d new high event を trigger とし、entry は翌営業日 open、exit は `20` / `60` 営業日後
close、signal がない日は cash `0%` として daily portfolio curve を作った。これは
periodic rebalance の value portfolio に breakout hard filter を掛ける実験とは別物で、
breakout 側を candidate generator として扱う。

| Universe | Condition | Hold | Events | Avg active | CAGR | Sharpe | MaxDD | Event mean |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `standard` | `annual_value_score_3` | `20d` | `6,135` | `50.2` | `39.03%` | `1.33` | `-36.90%` | `3.21%` |
| `standard` | `annual_value_score_3` | `60d` | `6,006` | `147.6` | `34.73%` | `1.47` | `-38.84%` | `11.07%` |
| `standard` | `low_pbr_forward_per_15` | `60d` | `12,977` | `318.8` | `25.06%` | `1.35` | `-35.70%` | `7.87%` |
| `standard` | `all` | `60d` | `75,620` | `1858.0` | `11.20%` | `0.70` | `-40.62%` | `3.62%` |
| `prime_ex_topix500` | `low_forward_per_le_10` | `60d` | `34,769` | `854.3` | `19.67%` | `1.02` | `-34.41%` | `5.38%` |
| `prime_ex_topix500` | `annual_value_score_ge_2` | `60d` | `38,584` | `948.0` | `17.81%` | `0.96` | `-35.23%` | `5.24%` |
| `prime_ex_topix500` | `all` | `60d` | `126,552` | `3109.4` | `10.63%` | `0.64` | `-46.23%` | `3.00%` |

### Interpretation

先行研究の 52週高値 momentum は「高値への近さが投資家のアンカーになり、良い情報への
過小反応が残る」という読みが中心。ただし今回の日本株では、新高値単体は market / universe
の差を吸収すると弱い。特に Growth は 252d 新高値でも 20d excess `-0.83%`、
60d excess `-2.80%`、20d loss >=5% が `44.4%` で、momentum を追う対象ではない。

出来高については、Huddart et al. (2009) の「過去レンジ突破時に volume が急増する」
という観察と整合するが、return 改善とは別物だった。Prime ex TOPIX500 では
`volume_ratio_20d >= 2.0` が 20d excess を `0.27%` まで上げるが、Standard では
`0.18%` に落ち、same-universe lift は `-0.65%`。さらに
`volume_2_without_quality_3` は Standard で 20d excess `-0.66%`、loss rate `36.2%`。
出来高は「参加者が増えた」ことは示すが、それが良い買い圧か、短期 crowding かは
quality / value を見ないと分からない。

ファンダメンタル補助は、単純な `quality_score >= 3` より annual value の三本柱が有効。
`quality_score >= 3` は Standard で 20d excess `0.69%` まで改善するが、
`annual_value_score_ge_2` は `1.60%`、`annual_value_score_3` は `2.97%`。
Prime ex TOPIX500 でも `annual_value_score_3` は 20d excess `1.52%`、60d excess `4.01%`。
これは「新高値を追う」より、「annual value 的に安い銘柄が新高値を更新した」
状態を追う方が clean という解釈になる。

### Production Implication

Production signal にするなら、`new_high_Nd` は entry trigger ではなく candidate gate とする。
第一候補は以下。

- `standard_new_high_annual_value_score_3`: `new_high_252d AND annual_value_score = 3`
- `standard_new_high_low_pbr_forward_per`: `new_high_252d AND pbr <= 1.0 AND forward_per <= 15`
- `prime_ex_topix500_new_high_annual_value_score_3`: `new_high_252d AND annual_value_score = 3`
- `prime_ex_topix500_new_high_low_forward_per`: `new_high_252d AND forward_per <= 10`

避けるべきものは以下。

- `growth_new_high`: Growth の新高値単体
- `standard_volume_only_new_high`: Standard の新高値 + 出来高急増のみ
- `volume_2_without_quality_3`: 出来高急増だが quality が弱いもの

### Caveats

取引コスト、limit-up/stop-high、板・約定強度、intraday の発生順序は見ていない。
`stock_data` adjusted daily OHLCV による event-study なので、実運用には流動性・約定可能性・
position sizing の portfolio lens が必要。Growth low PBR は sample が小さいため、
今回の結果だけで採用しない。`small_market_cap_bottom_30` は market cap を全銘柄 universe ではなく、
同日同 universe の新高値 event 内で相対化したものなので、portfolio 実装前に sizing/capacity lens が必要。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/new_high_momentum_research.py`
- Runner: `apps/bt/scripts/research/run_new_high_momentum_research.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_new_high_momentum_research.py`
  - `apps/bt/tests/unit/scripts/test_run_new_high_momentum_research.py`
- Bundle: `/tmp/trading25-research/market-behavior/new-high-momentum-research/20260507_new_high_momentum_value_v2/`
- Event-driven portfolio follow-up bundle: `/tmp/trading25-research/market-behavior/new-high-momentum-research/20260509_new_high_value_event_portfolio_v1/`
- Results DB: `/tmp/trading25-research/market-behavior/new-high-momentum-research/20260507_new_high_momentum_value_v2/results.duckdb`
- Summary: `/tmp/trading25-research/market-behavior/new-high-momentum-research/20260507_new_high_momentum_value_v2/summary.md`

### Prior Research Links

- George and Hwang (2004), “The 52-Week High and Momentum Investing”: https://doi.org/10.1111/j.1540-6261.2004.00695.x
- Liu, Liu, and Ma (2011), “The 52-week high momentum strategy in international stock markets”: https://doi.org/10.1016/j.jimonfin.2010.08.004
- Huddart, Lang, and Yetman (2009), “Volume and price patterns around a stock's 52-week highs and lows”: https://doi.org/10.1287/mnsc.1080.0920
- Della Vedova, Grant, and Westerholm (2023), “Investor Behavior at the 52-Week High”: https://doi.org/10.1017/S002210902200148X
- Onishchenko et al. (2024), “Investor heterogeneity and anchoring-induced momentum”: https://doi.org/10.1016/j.jbef.2024.100926

## Current Surface

- Output tables:
  - `universe_summary_df`
  - `new_high_summary_df`
  - `top_candidates_df`
  - `sampled_events_df`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_new_high_momentum_research.py \
  --output-root /tmp/trading25-research
```
