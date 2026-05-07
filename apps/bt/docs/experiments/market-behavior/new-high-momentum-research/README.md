# New-High Momentum Research

N日新高値をつけた銘柄の、その後の return を event-study で検証する研究。
N は `20 / 60 / 120 / 252` trading days とし、日足確定後にしか分からない signal
なので、評価 return は `next open -> future close` を使う。

## Published Readout

### Decision

N日新高値は、単独の long signal としては採用しない。特に 252d 新高値は
`TOPIX500` と `Growth` で TOPIX excess が悪く、`Standard` でも同日同 universe
baseline に対する lift はマイナスだった。

ただし、新高値は「候補生成」としては残す価値がある。採用候補は以下に限定する。

- `Prime ex TOPIX500`: 252d 新高値 + `low_pbr_le_1`、または `volume_ratio_20d >= 2.0` + `quality_score >= 3`
- `Standard`: 252d 新高値 + `cfo_positive` / `low_pbr_le_1` / `quality_score >= 3`
- `Growth`: 原則追わない。`low_pbr_le_1` は強いが 661 events と小さく、別研究で扱う

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

#### 補助条件

`Prime ex TOPIX500` は low PBR と volume+quality が改善する。`Standard` は
CFO positive / low PBR / quality が強く、出来高だけだと悪化する。

| Universe | 252d Condition | Events | 20d Excess | Lift vs New-High | Same-Universe Lift | 5% Loss Rate |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `Prime ex TOPIX500` | `all` | `130,765` | `0.08%` | `0.00%` | `0.02%` | `26.1%` |
| `Prime ex TOPIX500` | `low_pbr_le_1` | `25,329` | `0.63%` | `0.54%` | `0.51%` | `21.4%` |
| `Prime ex TOPIX500` | `volume_2_quality_3` | `21,810` | `0.34%` | `0.26%` | `0.27%` | `26.5%` |
| `Prime ex TOPIX500` | `volume_ratio_20d_ge_2` | `32,326` | `0.27%` | `0.18%` | `0.18%` | `27.7%` |
| `Standard` | `all` | `79,606` | `0.34%` | `0.00%` | `-0.48%` | `28.7%` |
| `Standard` | `cfo_positive` | `25,653` | `1.48%` | `1.15%` | `0.64%` | `23.5%` |
| `Standard` | `low_pbr_le_1` | `17,234` | `1.27%` | `0.94%` | `0.42%` | `22.4%` |
| `Standard` | `quality_score_ge_3` | `52,647` | `0.69%` | `0.36%` | `-0.13%` | `26.6%` |
| `Standard` | `volume_ratio_20d_ge_2` | `35,983` | `0.18%` | `-0.16%` | `-0.65%` | `31.5%` |
| `Standard` | `volume_2_without_quality_3` | `13,175` | `-0.66%` | `-0.99%` | `-1.48%` | `36.2%` |
| `Growth` | `all` | `20,056` | `-0.83%` | `0.00%` | `-0.56%` | `44.4%` |
| `Growth` | `low_pbr_le_1` | `661` | `4.23%` | `5.06%` | `3.33%` | `26.6%` |
| `Growth` | `quality_score_ge_3` | `10,445` | `-1.21%` | `-0.38%` | `-0.89%` | `42.9%` |
| `Growth` | `close_high_volume_1_5` | `5,403` | `-1.81%` | `-0.98%` | `-1.41%` | `46.1%` |

#### Horizon Stability

Standard の `cfo_positive` / `low_pbr_le_1` は 5d から 60d まで改善が残る。
Prime ex TOPIX500 の low PBR は 60d で特に強い。Growth の low PBR は強いが
sample が小さく、市場全体の Growth 新高値は追わない。

| Universe | Condition | 5d Excess | 20d Excess | 60d Excess | 60d Same-Universe Lift |
| --- | --- | ---: | ---: | ---: | ---: |
| `Prime ex TOPIX500` | `all` | `-0.02%` | `0.08%` | `0.29%` | `0.18%` |
| `Prime ex TOPIX500` | `low_pbr_le_1` | `0.14%` | `0.63%` | `1.94%` | `1.59%` |
| `Prime ex TOPIX500` | `volume_2_quality_3` | `-0.05%` | `0.34%` | `0.55%` | `0.37%` |
| `Standard` | `all` | `-0.11%` | `0.34%` | `0.69%` | `-0.79%` |
| `Standard` | `cfo_positive` | `0.13%` | `1.48%` | `2.72%` | `0.94%` |
| `Standard` | `low_pbr_le_1` | `0.15%` | `1.27%` | `4.03%` | `2.46%` |
| `Standard` | `volume_2_without_quality_3` | `-0.80%` | `-0.66%` | `-0.08%` | `-1.99%` |
| `Growth` | `all` | `-0.62%` | `-0.83%` | `-2.80%` | `-0.60%` |
| `Growth` | `low_pbr_le_1` | `0.93%` | `4.23%` | `10.32%` | `10.35%` |

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

ファンダメンタル補助は Standard で最も有効。`cfo_positive` と `low_pbr_le_1` は
20d / 60d の両方で baseline を大きく上回り、same-universe lift もプラスに転じる。
これは「新高値を追う」より、「財務的に壊れていない / 安すぎる銘柄が高値を更新した」
状態を追う方が clean という解釈になる。

### Production Implication

Production signal にするなら、`new_high_Nd` は entry trigger ではなく candidate gate とする。
第一候補は以下。

- `standard_new_high_cfo_positive`: `new_high_252d AND cfo_positive`
- `standard_new_high_low_pbr`: `new_high_252d AND pbr <= 1.0`
- `prime_ex_topix500_new_high_low_pbr`: `new_high_252d AND pbr <= 1.0`
- `prime_ex_topix500_new_high_volume_quality`: `new_high_252d AND volume_ratio_20d >= 2.0 AND quality_score >= 3`

避けるべきものは以下。

- `growth_new_high`: Growth の新高値単体
- `standard_volume_only_new_high`: Standard の新高値 + 出来高急増のみ
- `volume_2_without_quality_3`: 出来高急増だが quality が弱いもの

### Caveats

取引コスト、limit-up/stop-high、板・約定強度、intraday の発生順序は見ていない。
`stock_data` adjusted daily OHLCV による event-study なので、実運用には流動性・約定可能性・
position sizing の portfolio lens が必要。Growth low PBR は sample が小さいため、
今回の結果だけで採用しない。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/new_high_momentum_research.py`
- Runner: `apps/bt/scripts/research/run_new_high_momentum_research.py`
- Tests:
  - `apps/bt/tests/unit/domains/analytics/test_new_high_momentum_research.py`
  - `apps/bt/tests/unit/scripts/test_run_new_high_momentum_research.py`
- Bundle: `/tmp/trading25-research/market-behavior/new-high-momentum-research/20260507_new_high_momentum_v2/`
- Results DB: `/tmp/trading25-research/market-behavior/new-high-momentum-research/20260507_new_high_momentum_v2/results.duckdb`
- Summary: `/tmp/trading25-research/market-behavior/new-high-momentum-research/20260507_new_high_momentum_v2/summary.md`

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
