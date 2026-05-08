# Annual Value Periodic Rebalance

`annual-value-composite-selection` の年次 portfolio lens を、Nヶ月ごとに全決済して新しい value-composite ranking 上位銘柄へ全入替する research。

## Published Readout

### Decision

`standard` focused run では、`12m` より短い cadence が明確に良く、現時点の第一候補は `3m / Top 10 / ADV60 >= 10mn JPY`。`2m` も強いが、`3m` の方が CAGR / Sharpe のバランスがやや良い。次は全入替前提の turnover / cost を明示的に差し引く検証へ進める。ただし production Ranking contract はまだ変更しない。

`prime` focused run では、`prime_size_tilt` がほぼ全 cadence / TopN で最良。`ADV60 >= 10mn JPY` は Top 5 では少し効くが、Top 10 では no floor の方が Sharpe が高いケースが多い。Prime では liquidity floor よりも `prime_size_tilt` の score profile 自体が効いている。

### Why This Research Was Run

現行の annual value research は大発会 `Open` で買い、大納会 `Close` まで保有する前提だった。frontend Ranking は日々の ranking surface として実装されているため、実運用候補としては Nヶ月ごとに決済し、当該 entry date 時点の PIT-safe fundamentals で上位 `Top 5 / Top 10` 銘柄へ持ち替える portfolio lens が必要になった。

### Data Scope / PIT Assumptions

入力 SoT は `market.duckdb`。各リバランス期間の entry は当該期間最初の trading day `Open`、exit は次リバランス直前または年末期間最後の trading day `Close`。`stock_master_daily` は各 `entry_date` 時点、statements は各 `entry_date` as-of で解決する。annual bundle を単に分割するのではなく、リバランスごとに銘柄 universe と fundamentals を引き直す。

### Main Findings

#### 結論

| Rebalance | Liquidity | Top | Best score | Events | CAGR | Sharpe | MaxDD |
| ---: | --- | ---: | --- | ---: | ---: | ---: | ---: |
| `3m` | `ADV60 >= 10mn` | `10` | `prime_size_tilt` | `380` | `59.16%` | `2.16` | `-34.96%` |
| `3m` | `ADV60 >= 10mn` | `10` | `equal_weight` | `380` | `55.77%` | `2.13` | `-35.36%` |
| `3m` | `ADV60 >= 10mn` | `10` | `walkforward_regression_weight` | `380` | `57.17%` | `2.12` | `-35.36%` |
| `2m` | `ADV60 >= 10mn` | `10` | `prime_size_tilt` | `580` | `55.93%` | `2.10` | `-33.43%` |
| `2m` | `ADV60 >= 10mn` | `10` | `walkforward_regression_weight` | `580` | `54.12%` | `2.10` | `-34.45%` |
| `6m` | `ADV60 >= 10mn` | `10` | `prime_size_tilt` | `180` | `54.48%` | `2.01` | `-33.68%` |
| `12m` | `ADV60 >= 10mn` | `10` | `equal_weight` | `90` | `43.75%` | `1.75` | `-34.32%` |

#### Prime focused

| Rebalance | Liquidity | Top | Best score | Events | CAGR | Sharpe | MaxDD |
| ---: | --- | ---: | --- | ---: | ---: | ---: | ---: |
| `2m` | `ADV60 >= 10mn` | `5` | `prime_size_tilt` | `290` | `50.33%` | `1.85` | `-32.56%` |
| `3m` | none | `10` | `prime_size_tilt` | `400` | `39.86%` | `1.79` | `-29.50%` |
| `2m` | none | `5` | `prime_size_tilt` | `300` | `44.55%` | `1.78` | `-32.85%` |
| `6m` | none | `10` | `prime_size_tilt` | `200` | `37.85%` | `1.72` | `-29.50%` |
| `2m` | `ADV60 >= 10mn` | `10` | `prime_size_tilt` | `580` | `37.80%` | `1.64` | `-30.35%` |
| `3m` | `ADV60 >= 10mn` | `10` | `prime_size_tilt` | `380` | `35.46%` | `1.57` | `-32.81%` |

### Interpretation

`standard` の value composite は、年1回より四半期程度で ranking を更新した方が signal decay を抑えられる可能性が高い。Top 5 は CAGR が高くなりやすいが Sharpe / MaxDD が悪化しやすく、現時点の practical read は Top 10 優先。`ADV60 >= 10mn` は annual research では alpha score から外す判断だったが、この全入替 lens では少数銘柄の極端な illiquid winner を抑え、risk-adjusted read を安定させている。

Prime は Standard より liquidity floor の効き方が弱い。Top 5 では `ADV60 >= 10mn` が `2m prime_size_tilt` の Sharpe を `1.78` から `1.85` に押し上げる一方、Top 10 では `3m prime_size_tilt` が no floor `1.79`、ADV10m `1.57` で、floor が上位候補を削りすぎる。Prime では `prime_size_tilt` を前提に、liquidity は hard floor より execution/capacity diagnostic として扱う方が自然。

### Production Implication

この段階では production Ranking API / frontend contract は変更しない。次の検証で turnover / cost after ADV floor 後も `3m Top 10` が残るなら、Ranking page の readout ではなく strategy-side rebalance policy として扱うのが自然。

### Caveats

cost、slippage、capacity、税コスト、同日寄り付きで全銘柄を入れ替える執行可能性は未反映。`ADV60` floor は既存 annual research と同じく alpha score へ混ぜず、capacity diagnostic として比較する。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/annual_value_periodic_rebalance.py`
- Runner: `apps/bt/scripts/research/run_annual_value_periodic_rebalance.py`
- Standard bundle: `/tmp/trading25-research/market-behavior/annual-value-periodic-rebalance/20260508_standard_periodic_value_2m3m_fast/`
- Prime bundle: `/tmp/trading25-research/market-behavior/annual-value-periodic-rebalance/20260508_prime_periodic_value_2m3m_fast/`

## Current Surface

- `rebalance_calendar_df`: Nヶ月 holding window。`year` は `YYYY-Mmm-Nm` period id として扱う。
- `event_ledger_df`: period x stock の PIT-safe fundamental snapshot と period return。
- `selected_event_df`: market scope / score method / liquidity scenario / `selection_count` ごとの採用銘柄。
- `portfolio_daily_df`: equal-weight daily curve。
- `portfolio_summary_df`: CAGR、Sharpe、Sortino、Calmar、MaxDD。

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_value_periodic_rebalance.py \
  --output-root /tmp/trading25-research \
  --require-positive-pbr-and-forward-per
```

Focused rerun:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_value_periodic_rebalance.py \
  --output-root /tmp/trading25-research \
  --rebalance-months 3 \
  --rebalance-months 6 \
  --selection-count 5 \
  --selection-count 10 \
  --require-positive-pbr-and-forward-per
```
