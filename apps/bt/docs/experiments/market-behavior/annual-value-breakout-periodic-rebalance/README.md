# Annual Value Breakout Periodic Rebalance

`annual-value-periodic-rebalance` に、entry 前営業日までに観測できる N日 breakout momentum を統合した portfolio-lens research。

## Published Readout

### Decision

`standard` では breakout を hard gate にするより、value score へ小さく加点する `breakout_additive` が有効。現時点の第一候補は `standard / prime_size_tilt / ADV60 >= 10mn / 3m / Top 10` に `20d breakout within 5 sessions` の recency boost を足す形で、value-only の Sharpe `2.16` から `2.23` へ改善した。ただし MaxDD は `-34.96%` から `-36.33%` へ少し悪化するため、hard production 化の前に cost / turnover / tail attribution を確認する。

`prime` では breakout を足しても、既存の value-only 最良条件 `2m / Top 5 / ADV60 >= 10mn / prime_size_tilt` の Sharpe `1.85` を超えない。一方、factor weight を小型寄りに振ると、`PBR 0% / size 75% / forward PER 25%` の `2m / Top 10 / ADV60 >= 10mn` が Sharpe `1.88` まで改善した。Prime では breakout は entry filter ではなく、候補説明・risk diagnostic に留め、weight は `prime_size_tilt` より小型寄りの profile を追加候補にする。

広範探索用には、`--factor-weight-step` で `low_pbr_score` / `small_market_cap_score` / `low_forward_per_score` の simplex grid を生成し、`--skip-portfolio-curves` で selection proxy だけを先に出す二段階運用を追加した。全 cross product を一度に daily curve 化するのは重いため、広範探索は proxy で weight/policy 候補を絞り、上位だけ portfolio confirmation に進める。

### Why This Research Was Run

Nヶ月 rebalance の value portfolio が年次保有より良い結果を出したため、過去の `new-high-momentum-research` で有望だった「value + N日新高値」の相互作用を、event-study ではなく実際の rebalance portfolio lens に移した。

### Data Scope / PIT Assumptions

入力 SoT は `market.duckdb`。value ranking は各 rebalance `entry_date` 時点の PIT fundamentals で計算する。breakout 判定は `entry_date` 当日の high/close を使わず、`signal_date = entry_date` の直前営業日までの price history で計算する。`new_high_Nd` は signal 日の `high > prior_high_Nd` で、prior high は signal 日自身を含めない。

### Main Findings

#### 結論

| Market | Policy | Window | Lookback | Rebalance | Top | Liquidity | CAGR | Sharpe | MaxDD | Events |
| --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| `standard` | `breakout_additive` | `20d` | `5` | `3m` | `10` | `ADV60 >= 10mn` | `63.77%` | `2.23` | `-36.33%` | `380` |
| `standard` | `breakout_additive` | `120d` | `20` | `3m` | `10` | `ADV60 >= 10mn` | `64.22%` | `2.23` | `-35.80%` | `380` |
| `standard` | `breakout_additive` | `60d` | `20` | `3m` | `10` | `ADV60 >= 10mn` | `62.74%` | `2.20` | `-36.69%` | `380` |
| `standard` | `value_only` | `-` | `-` | `3m` | `10` | `ADV60 >= 10mn` | `59.16%` | `2.16` | `-34.96%` | `380` |
| `standard` | `breakout_recent` | `20d` | `20` | `3m` | `10` | `ADV60 >= 10mn` | `48.22%` | `1.84` | `-38.69%` | `380` |

#### Prime focused

| Market | Profile | Policy | Window | Lookback | Rebalance | Top | Liquidity | CAGR | Sharpe | MaxDD | Events |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| `prime` | `PBR0/size75/fPER25` | `value_only` | `-` | `-` | `2m` | `10` | `ADV60 >= 10mn` | `45.50%` | `1.88` | `-30.85%` | `580` |
| `prime` | `prime_size_tilt` | `value_only` | `-` | `-` | `2m` | `5` | `ADV60 >= 10mn` | `50.33%` | `1.85` | `-32.56%` | `290` |
| `prime` | `none` | `breakout_recent` | `60d` | `20` | `3m` | `5` | none | `43.18%` | `1.80` | `-28.75%` | `195` |
| `prime` | `none` | `value_only` | `-` | `-` | `3m` | `10` | none | `39.86%` | `1.79` | `-29.50%` | `400` |
| `prime` | `prime_size_tilt` | `breakout_additive` | `120d` | `20` | `2m` | `10` | `ADV60 >= 10mn` | `38.96%` | `1.67` | `-31.67%` | `580` |

#### Factor weight grid

| Market | Lens | PBR | Size | Forward PER | Liquidity | Rebalance | Top | Metric | Read |
| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | --- | ---: |
| `standard` | portfolio | `0.25` | `0.00` | `0.75` | `ADV60 >= 10mn` | `2m` | `10` | Sharpe | `2.17` |
| `standard` | portfolio | `0.05` | `0.465` | `0.485` | `ADV60 >= 10mn` | `3m` | `10` | Sharpe | `2.16` |
| `standard` | portfolio | `0.40` | `0.35` | `0.25` | `ADV60 >= 30mn` | `2m` | `5` | Sharpe | `2.17` |
| `prime` | proxy | `0.00` | `0.75` | `0.25` | `ADV60 >= 30mn` | `12m` | `5` | Mean return | `30.41%` |
| `prime` | portfolio | `0.00` | `0.75` | `0.25` | `ADV60 >= 10mn` | `2m` | `10` | Sharpe | `1.88` |
| `prime` | proxy | `0.00` | `1.00` | `0.00` | none | `12m` | `5` | Mean return | `30.22%` |

### Interpretation

Standard では、breakout を「買ってよい候補を限定する hard gate」にすると value の強い候補を捨てすぎる。一方、recent breakout を score に少し足すと、同じ Top 10 枠の中で足元の需給が強い銘柄を上へ寄せられ、Sharpe が改善した。20d/5 sessions と 120d/20 sessions が近い結果なので、特定の window に過剰最適化されているというより、value 上位候補内の momentum recency が効いている可能性が高い。

Prime は liquidity と analyst coverage の厚い大型寄り universe なので、breakout の追加情報が value-only の `prime_size_tilt` を上回りにくい。hard gate は MaxDD を少し改善する局面があるが、CAGR/Sharpe を削りやすい。ただし value factor weight 自体は改善余地があり、`size75/fper25` は `prime_size_tilt` より小幅に高い Sharpe と浅い MaxDD を出した。

3要素 weight grid では、Standard は `forward PER` 寄りの weight が強い。`PBR 25% / size 0% / forward PER 75%` は value-only で Sharpe `2.17`、MaxDD `-29.41%` と tail が軽い。一方、breakout additive まで含めると `prime_size_tilt` の `3m / Top 10 / ADV60 >= 10mn` が Sharpe `2.23` で残る。Prime は `size` 寄りが上位で、portfolio confirmation でも `size 75% / forward PER 25%` が `prime_size_tilt` を Sharpe `1.85` から `1.88` へ小幅に上回った。

### Production Implication

Ranking / strategy surface に進めるなら、まず Standard 向けの additive score として扱う。Prime は breakout を UI 上の補助特徴量・説明変数に留める。最適化パラメータ候補としては、Standard は `prime_size_tilt + breakout_additive(20d, 5 sessions, boost=0.10)` を第一候補、低DD寄りの代替として `PBR 25% / size 0% / forward PER 75%` を残す。Prime は `PBR 0% / size 75% / forward PER 25%` を `prime_size_tilt` の改善候補にする。

### Caveats

cost、slippage、税コスト、同日寄り付きでの全入替 execution、turnover は未控除。今回の runner は focused scan を高速化するため、`score_method` / `liquidity_scenario` / `breakout_policy` を絞っている。全組み合わせ探索は重いため、追加拡張時は候補 policy を先に限定する。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/annual_value_breakout_periodic_rebalance.py`
- Runner: `apps/bt/scripts/research/run_annual_value_breakout_periodic_rebalance.py`
- Standard bundle: `/tmp/trading25-research/market-behavior/annual-value-breakout-periodic-rebalance/20260508_standard_value_breakout_2m3m_focused_v2/`
- Prime bundle: `/tmp/trading25-research/market-behavior/annual-value-breakout-periodic-rebalance/20260508_prime_value_breakout_2m3m_focused_v2/`
- Standard factor proxy bundle: `/tmp/trading25-research/market-behavior/annual-value-breakout-periodic-rebalance/20260508_standard_value_factor_grid_proxy_v2/`
- Standard factor portfolio bundle: `/tmp/trading25-research/market-behavior/annual-value-breakout-periodic-rebalance/20260508_standard_value_factor_grid_portfolio_top/`
- Standard factor+breakout portfolio bundle: `/tmp/trading25-research/market-behavior/annual-value-breakout-periodic-rebalance/20260508_standard_value_factor_breakout_top_portfolio/`
- Prime factor proxy bundle: `/tmp/trading25-research/market-behavior/annual-value-breakout-periodic-rebalance/20260508_prime_value_factor_grid_proxy/`
- Prime factor portfolio bundle: `/tmp/trading25-research/market-behavior/annual-value-breakout-periodic-rebalance/20260508_prime_value_factor_grid_portfolio_top/`

## Current Surface

- `rebalance_calendar_df`: Nヶ月 holding window。
- `event_ledger_df`: value-only periodic event ledger。
- `breakout_feature_df`: entry 直前営業日の breakout / recency / signal diagnostics。
- `breakout_scored_panel_df`: value score panel + breakout features。
- `score_method_params_df`: built-in / factor grid score method の PBR / size / forward PER weight mapping。
- `selected_event_df`: score method / liquidity / breakout policy ごとの採用銘柄。
- `portfolio_daily_df`: equal-weight daily curve。
- `portfolio_summary_df`: CAGR、Sharpe、Sortino、Calmar、MaxDD。

## Run

Standard focused:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_value_breakout_periodic_rebalance.py \
  --output-root /tmp/trading25-research \
  --run-id 20260508_standard_value_breakout_2m3m_focused_v2 \
  --market standard \
  --rebalance-months 2 \
  --rebalance-months 3 \
  --selection-count 5 \
  --selection-count 10 \
  --score-method prime_size_tilt \
  --liquidity-scenario adv10m \
  --breakout-policy value_only \
  --breakout-policy breakout_recent \
  --breakout-policy breakout_additive \
  --breakout-window 20 \
  --breakout-window 60 \
  --breakout-window 120 \
  --breakout-window 252 \
  --breakout-lookback-sessions 0 \
  --breakout-lookback-sessions 5 \
  --breakout-lookback-sessions 20 \
  --require-positive-pbr-and-forward-per
```

Broad factor proxy:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_value_breakout_periodic_rebalance.py \
  --output-root /tmp/trading25-research \
  --market standard \
  --rebalance-months 2 \
  --rebalance-months 3 \
  --rebalance-months 6 \
  --rebalance-months 12 \
  --selection-count 5 \
  --selection-count 10 \
  --liquidity-scenario none \
  --liquidity-scenario adv10m \
  --liquidity-scenario adv30m \
  --breakout-policy value_only \
  --factor-weight-step 0.25 \
  --skip-portfolio-curves \
  --require-positive-pbr-and-forward-per
```
