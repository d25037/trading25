# Index Market Strength Research

`indices_data` の `sector33` 日足を使い、33業種指数が強い market state にあるかを、20営業日後 return で検証する研究。

## Published Readout

### Decision

初期実装は runner-first の research surface として採用する。production rule はまだ固定しない。初回 run では、単純な「強い breadth」よりも、`120/250` 日の weak breadth が高い局面の 20営業日後 rebound が強く出た。一方、overheat breadth が高い局面は validation を中心に弱く、market が強いかどうかを見る研究は「強いものを買う」より「広範な弱さの反発」と「広範な過熱の回避」に分ける。

### Why This Research Was Run

annual value 解析で使った technical overlay の発想を、個別銘柄ではなく 33業種指数の market state 判定へ転用する。目的は「強い市場」を後追いで説明するだけでなく、20営業日後の左尾が悪い market state を避けられるか確認すること。

### Data Scope / PIT Assumptions

入力は active `market.duckdb` の `indices_data` と `index_master`。対象は `index_master.category = sector33` の指数だけに限定する。各日 `t` の特徴量は、その日までの `close/high/low` だけで作る。目的変数は `t+20` 営業日の close-to-close return。bucket threshold は固定値で、未来の return 分布から bucket 境界を作らない。

### Main Findings

#### 結論

| Lens | Lookback | Bucket | Obs | Mean 20d | Hit | P10 | 読み |
| --- | ---: | --- | ---: | ---: | ---: | ---: | --- |
| `weak_breadth` | `120` | `breadth_high_ge_60pct` | `58` | `+4.45%` | `82.76%` | `-2.34%` | 広範な中期弱さの後の rebound |
| `weak_breadth` | `250` | `breadth_high_ge_60pct` | `64` | `+4.03%` | `81.25%` | `-2.91%` | 長期弱さの後の rebound |
| `rebound_breadth` | `120` | `breadth_low_lt_30pct` | `59` | `+3.96%` | `77.97%` | `-3.75%` | まだ広く戻っていない局面 |
| `return` | `60` | `loss_le_-10pct` | `5,896` | `+3.11%` | `70.64%` | `-5.14%` | index単体でも大幅下落後が強い |
| `overheat_breadth` | `120` | `breadth_high_ge_60pct` | `94` | `-0.88%` | `47.87%` | `-7.54%` | 広範な過熱は20営業日後に弱い |
| `overheat_breadth` | `60` | `breadth_high_ge_60pct` | `45` | `-0.87%` | `35.56%` | `-5.53%` | 短中期過熱も弱い |

#### Split Check

| Lens | Lookback | Period | Obs | Mean 20d | Hit | P10 |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| `weak_breadth high` | `120` | discovery | `33` | `+4.16%` | `81.82%` | `-1.82%` |
| `weak_breadth high` | `120` | validation | `21` | `+2.99%` | `80.95%` | `-11.57%` |
| `weak_breadth high` | `250` | discovery | `45` | `+4.28%` | `84.44%` | `-1.69%` |
| `weak_breadth high` | `250` | validation | `17` | `+2.08%` | `70.59%` | `-12.18%` |
| `overheat_breadth high` | `120` | discovery | `34` | `-2.51%` | `35.29%` | `-8.32%` |
| `overheat_breadth high` | `120` | validation | `28` | `-1.03%` | `39.29%` | `-6.25%` |
| `overheat_breadth high` | `120` | holdout | `32` | `+0.98%` | `68.75%` | `-5.17%` |

#### TOPIX / Beta Decomposition

`60日 return <= -10%` の個別33指数 rebound は raw では強いが、TOPIX を控除するとかなり薄まり、beta-adjusted residual では高ベータ優位ではなくなる。

| Lens | Raw 20d | TOPIX excess | Beta-adjusted | Hit(beta-adj) | 読み |
| --- | ---: | ---: | ---: | ---: | --- |
| `return_20d <= -10%` | `+3.40%` | `-0.24%` | `-0.28%` | `48.17%` | 短期急落後の raw rebound は市場反発寄り |
| `return_60d <= -10%` | `+3.11%` | `+0.28%` | `+0.25%` | `52.00%` | 中期急落後に薄い residual が残る |
| `return_120d <= -10%` | `+2.14%` | `-0.10%` | `-0.04%` | `48.61%` | 長期下落後はほぼ beta / market rebound |
| `price_position_120d low` | `+1.92%` | `+0.22%` | `+0.26%` | `52.93%` | レンジ下限効果も大半は市場反発 |
| `return_20d > +15%` | `+0.45%` | `+0.02%` | `-0.05%` | `45.15%` | 急騰後は residual でも弱い |

`60日 return <= -10%` の beta tercile 別では、raw は high beta が最も強いが、beta-adjusted では low beta が最も強い。

| Beta tercile | Avg beta | Raw 20d | TOPIX excess | Beta-adjusted | Beta-adj hit | Beta-adj P10 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| low beta | `0.74` | `+3.15%` | `+0.60%` | `+1.26%` | `64.82%` | `-4.37%` |
| mid beta | `0.98` | `+2.86%` | `-0.34%` | `-0.26%` | `46.81%` | `-4.91%` |
| high beta | `1.14` | `+4.03%` | `+0.60%` | `+0.11%` | `49.89%` | `-5.10%` |

#### 初期 bucket 定義

| Family | Bucket 方針 |
| --- | --- |
| `return` | `<= -10%`, `-10%..0%`, `0%..5%`, `5%..15%`, `>15%` |
| `rebound_from_low` | `<1%`, `1%..5%`, `5%..10%`, `10%..20%`, `>=20%` |
| `price_position` | `0..20%`, `20..50%`, `50..80%`, `80..100%` |
| `breadth` | `<30%`, `30..60%`, `>=60%` |

### Interpretation

この研究は最初から signal 採用を決めるものではなく、market regime の分類器として使う。初回結果は、強い market を「上がっている指数が多い」と定義するより、広く弱くなった後に反発しやすい局面を捕まえる方が有効そうだった。`weak_breadth high` は discovery と validation で平均 return はプラスだが、validation の P10 は悪化しているため、hard long signal ではなく rebound candidate と読む。

`overheat_breadth high` は discovery / validation で弱く、特に validation は hit rate が低い。ただし holdout はプラスに転じているため、単純な除外ルールとしてはまだ不安定。次は market shock 日、日銀/政策イベント、半期 split を加えて、過熱後の弱さが regime 依存かを分解する。

TOPIX / beta decomposition 後の読みはより慎重になる。raw return の「売られた業種の反発」はかなり市場全体の rebound と beta exposure で説明できる。セクターローテーション alpha として残るのは、`60日 return <= -10%` の beta-adjusted `+0.25%` 程度で、強いとは言いにくい。一方で low beta の売られすぎ業種には beta-adjusted `+1.26%` が残り、これは market beta ではなく defensive / laggard rotation の候補として別に見る価値がある。

### Production Implication

候補が安定すれば、annual value や screening の hard filter より先に、market exposure / sizing / warning diagnostic として使う。現時点では `weak_breadth high` を rebound watch、`overheat_breadth high` を caution watch として扱い、銘柄選定 score へ直接混ぜない。個別業種の raw rebound は market beta に大きく依存するため、production へ使う場合は raw ではなく TOPIX excess / beta-adjusted residual を主指標にする。

### Caveats

`sector33` 指数は指数そのものの OHLC であり、個別銘柄 universe の breadth ではない。33指数間の equal-weight 集計は市場全体の近似で、TOPIX 時価総額加重とは一致しない。初回 run の high weak-breadth bucket は observation が `58-64` 日程度で、validation の左尾は十分に悪い。beta は全期間の日次 return で推定した static beta であり、rolling beta / regime beta ではない。index_master の historical membership は不要だが、指数コード catalog と local `indices_data` の coverage に依存する。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/index_market_strength_research.py`
- Runner: `apps/bt/scripts/research/run_index_market_strength_research.py`
- Bundle: `/tmp/trading25-research/market-behavior/index-market-strength-research/20260504_index_market_strength_sector33_beta_decomp/`
- Results DB: `/tmp/trading25-research/market-behavior/index-market-strength-research/20260504_index_market_strength_sector33_beta_decomp/results.duckdb`
- Summary: `/tmp/trading25-research/market-behavior/index-market-strength-research/20260504_index_market_strength_sector33_beta_decomp/summary.md`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_index_market_strength_research.py \
  --output-root /tmp/trading25-research
```

Focused smoke:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_index_market_strength_research.py \
  --lookbacks 20,60 \
  --horizon-sessions 20 \
  --output-root /tmp/trading25-research \
  --run-id index_market_strength_smoke
```
