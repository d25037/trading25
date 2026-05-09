# Classical Momentum Research

中期 cross-sectional momentum を、`new_high` ではなく classical な過去リターン rank として検証する研究。

## Published Readout

### Decision

classical momentum は、今回の long-only daily portfolio lens では core alpha として弱い。TOPIX500 と Prime ex TOPIX500 では top momentum basket が positive CAGR / Sharpe を出すが、value periodic の水準には全く届かない。Standard / Growth では単純 momentum は pump/fade や高ボラ tail と混ざりやすく、単独採用しない。

次に value と統合するなら、momentum を主因子として広げるより、TOPIX500 / Prime の補助 diagnostic、または value score 上位内の trend confirmation として扱う。Standard はこのまま momentum 加点へ進めず、value 側の pump/fade / liquidity / risk budget の文脈でのみ再利用する。

### Why This Research Was Run

新高値 breakout と出来高の検証では、breakout 単体は弱く、value 条件を足すと改善するが value-only periodic には勝てなかった。一般的な momentum 投資の有効性は `new high` よりも `6-1` / `12-1` の中期リターンで語られることが多いため、classical momentum を別 research として切り出した。

### Data Scope / PIT Assumptions

入力は active `market.duckdb`。universe は `stock_master_daily` の同日 membership で `TOPIX500` / `Prime ex TOPIX500` / `Standard` / `Growth` に分ける。signal は各 rebalance date までに観測できる adjusted close だけで作る。`126d_skip_20d` は signal 日の `20` 営業日前 close と `126` 営業日前 close の return で、直近約1ヶ月を除外する。entry は翌営業日 open、exit は `20` / `60` 営業日後 close。ADV60 `10mn JPY` 未満は除外する。

### Main Findings

#### 結論

| Universe | Best spec | Hold | Top | Events | CAGR | Sharpe | MaxDD | Read |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `topix500` | `126d_skip_20d` | `20d` | `5%` | `2,877` | `16.26%` | `0.80` | `-31.79%` | 一応 positive だが core には弱い |
| `prime_ex_topix500` | `63d_skip_5d` | `20d` | `10%` | `16,074` | `13.36%` | `0.72` | `-46.65%` | 弱い continuation |
| `standard` | `63d_skip_5d` | `60d` | `10%` | `7,497` | `9.87%` | `0.49` | `-59.70%` | 単独採用不可 |
| `growth` | `63d_skip_5d` | `20d` | `10%` | `4,008` | `7.62%` | `0.40` | `-57.69%` | 単独採用不可 |

#### TOPIX500 / Prime には弱い momentum が残る

| Universe | Spec | Hold | Top | CAGR | Sharpe | MaxDD |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `topix500` | `126d_skip_20d` | `20d` | `5%` | `16.26%` | `0.80` | `-31.79%` |
| `topix500` | `63d_skip_5d` | `60d` | `5%` | `14.92%` | `0.79` | `-41.69%` |
| `topix500` | `252d_skip_20d` | `20d` | `5%` | `14.28%` | `0.73` | `-39.19%` |
| `prime_ex_topix500` | `63d_skip_5d` | `20d` | `10%` | `13.36%` | `0.72` | `-46.65%` |
| `prime_ex_topix500` | `63d_skip_5d` | `60d` | `10%` | `13.30%` | `0.71` | `-48.76%` |
| `prime_ex_topix500` | `126d_skip_20d` | `60d` | `10%` | `12.09%` | `0.65` | `-47.35%` |

#### Standard / Growth は弱い

| Universe | Spec | Hold | Top | CAGR | Sharpe | MaxDD |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `standard` | `63d_skip_5d` | `60d` | `10%` | `9.87%` | `0.49` | `-59.70%` |
| `standard` | `126d_skip_20d` | `60d` | `10%` | `7.25%` | `0.39` | `-63.88%` |
| `standard` | `252d_skip_20d` | `60d` | `10%` | `2.25%` | `0.22` | `-59.58%` |
| `growth` | `63d_skip_5d` | `20d` | `10%` | `7.62%` | `0.40` | `-57.69%` |
| `growth` | `126d_skip_20d` | `20d` | `10%` | `1.29%` | `0.20` | `-58.30%` |
| `growth` | `252d_skip_20d` | `60d` | `10%` | `-5.13%` | `-0.03` | `-67.71%` |

### Interpretation

今回の結果は「日本株で momentum が完全に無効」というより、long-only の単純 winners basket だけでは弱い、という読みが妥当。TOPIX500 は一応 continuity が残るが、Sharpe `0.8` 程度で、drawdown も小さくない。Prime ex TOPIX500 も同様に continuation はあるが、value periodic の Sharpe `1.8` 以上とは別物。

Standard / Growth は過去上昇銘柄の中に短期過熱、低流動性、pump/fade が混ざりやすく、直近1ヶ月を skip しても十分に掃除できなかった。特に `252d_skip_20d` は Standard / Growth でかなり弱く、長期 winner を追うほど良いわけではない。

### Production Implication

production ranking の主導線は引き続き value periodic。momentum は以下の範囲に留める。

- `TOPIX500` / `Prime ex TOPIX500` の補助 diagnostic
- value 上位候補内での trend confirmation
- Standard では単純加点せず、pump/fade / liquidity / risk budget と一緒に再検証

次にやるなら、value との統合前に `sector-neutral momentum` / `TOPIX excess momentum` / `residual momentum` を確認する。今回の raw momentum は market / sector beta と高ボラ winner を分離していない。

### Caveats

cost、slippage、税コスト、borrow/short leg は未評価。これは long-only top basket であり、academic momentum の long-short factor とは違う。rebalance は `20` sessions 間隔の固定 schedule で、月末 rebalance そのものではない。ADV60 `10mn JPY` 未満を除外しているが、turnover と実約定 capacity はまだ見ていない。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/classical_momentum_research.py`
- Runner: `apps/bt/scripts/research/run_classical_momentum_research.py`
- Bundle: `/tmp/trading25-research/market-behavior/classical-momentum-research/20260509_classical_momentum_v1/`
- Results DB: `/tmp/trading25-research/market-behavior/classical-momentum-research/20260509_classical_momentum_v1/results.duckdb`
- Summary: `/tmp/trading25-research/market-behavior/classical-momentum-research/20260509_classical_momentum_v1/summary.md`

## Current Surface

- `universe_summary_df`: market-split universe coverage.
- `selected_event_df`: rebalance date, selected top momentum symbols, momentum score, and forward return.
- `portfolio_daily_df`: equal-weight active-position daily portfolio curve with cash on no-signal days.
- `portfolio_summary_df`: CAGR / Sharpe / Sortino / Calmar / MaxDD.

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_classical_momentum_research.py \
  --output-root /tmp/trading25-research \
  --run-id 20260509_classical_momentum_v1 \
  --lookback-specs 63:5,126:20,252:20 \
  --hold-sessions 20,60 \
  --selection-fractions 0.05,0.10 \
  --rebalance-interval-sessions 20 \
  --min-avg-trading-value-mil-jpy 10
```
