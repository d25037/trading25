# Forward EPS Driven V3 Factor Decomposition

## Published Readout

### Decision

`production/forward_eps_driven` を `market.duckdb` v3 の `primeExTopix500` universe で再解析した canonical research。

この研究は、既存の `forward-eps-trade-archetype-decomposition` を一段深掘りし、実約定 trade を entry 日 PIT の `stock_master_daily` market scope、fundamentals、technical feature、market regime、tail cohort、action candidate に分解する。目的は、単純な overlay 探索ではなく、Prime / historical Standard の違い、value/fundamental edge の市場別安定性、過熱条件の hard exclude と size haircut の違いを比較すること。

結論として、v3 `primeExTopix500` でも `forward_eps_driven` は trade-level 平均リターンが full `+6.26%`、holdout `+6.55%` と残る。ただし中央値は full `-3.82%`、holdout `-1.76%` で、勝ちトレードの右裾に依存する構造は変わらない。Production に入れるなら、まずは Prime 側の過熱 tail を position sizing / risk control で抑える方向が自然。Standard 側は full-history では非常に強いが、直近 6 カ月 holdout に Standard trade が無いため、独立した追加ルール化はまだ早い。

### Main Findings

#### 結論

| lens | result | read |
|---|---:|---|
| Full all | 412 trades, avg `+6.26%`, severe loss `23.06%` | edge は残るが損失頻度は高い |
| Holdout 6m | 22 trades, avg `+6.55%`, severe loss `13.64%` | 小標本だが v3 後も崩れていない |
| Prime full | 344 trades, avg `+5.89%`, severe loss `24.42%` | 主体は Prime。過熱 tail の管理が最重要 |
| Standard full | 68 trades, avg `+8.13%`, severe loss `16.18%` | full-history は良いが holdout では trade なし |
| V3 overheat exclude, all | kept avg `+5.58%`, kept severe `18.69%`, worst `-24.31%` | 平均は落ちるが左裾は明確に改善 |
| V3 overheat haircut 0.5x, all | haircut avg `+5.41%`, severe `17.96%`, worst `-24.31%` | hard exclude より sizing ルールの方が production 向き |
| Standard low forward PER | 23 trades, avg `+21.74%`, severe `8.70%` | Standard 専用 candidate。ただし holdout 未検証 |
| Holdout low PBR | 8 trades, avg `+11.97%`, severe `12.50%` | 直近は low PER より low PBR が効いている |

#### Prime / Standard

`market.duckdb` v3 の PIT stock master で entry 日 market scope を引くと、full-history は Prime 344 trades / Standard 68 trades。Prime は trade 数の大半を占めるが、avg `+5.89%`、median `-4.07%`、severe `24.42%` と左裾が重い。Standard は avg `+8.13%`、median `-1.57%`、severe `16.18%` で見た目は良い。ただし holdout 6m は全 22 trades が Prime で、Standard の直近検証はできていない。

このため市場別の結論は「Prime は production risk control 対象」「Standard は別研究で検証する候補」。Standard にだけ低 PER / high FEPS margin / momentum 系を足す判断は、直近データが足りない。

#### Fundamentals

Full all の factor contrast では、PBR high bucket が avg `+15.13%` と low bucket `+4.47%` を上回る一方、severe loss は `40.96%` vs `9.76%` まで悪化する。高 PBR は右裾と左裾を同時に増やすため、単純な high PBR 採用ではなく tail control が必要。

Forward EPS growth / margin は、既に strategy の entry 条件に含まれているため、追加の単独分解では強い独立 edge になっていない。Standard では high forward EPS growth bucket が avg `+14.53%` vs low `-0.32%` と効くが、これも holdout 未検証。直近 holdout の keep candidate では high forward EPS margin が 6 trades avg `-1.20%` と弱い。

Low forward PER は full all では 137 trades avg `+6.72%` と baseline 並み、Prime では 115 trades avg `+3.87%` と弱い。一方 Standard では 23 trades avg `+21.74%`、severe `8.70%` と強い。つまり low PER は全市場共通 rule ではなく、historical Standard candidate として扱う。

#### Technical / Market Regime

Prime の悪い tail は過熱系に寄っている。Full Prime では RSI10 high bucket が avg `-0.44%`、severe `36.23%` で、low bucket avg `+17.29%` と大きく差が出た。Stock return 20d high も avg `-2.33%`、severe `43.48%`。Risk-adjusted return high は平均差こそ小さいが severe を増やす。

Market regime は平均リターンだけ見ると弱い局面の方が良いが、強い TOPIX 局面は severe loss を下げる。Full all の TOPIX 20d high は avg `+2.30%` と低い一方、severe `16.87%` で low bucket `31.71%` より安全。これは「地合いフィルタで平均を上げる」より「地合い・個別過熱を使って sizing を抑える」方向を示す。

#### Tail

Full all severe-loss cohort は 95 trades、median return `-14.18%`。全体 median と比べて PBR `1.29` vs `0.92`、risk-adjusted return `3.08` vs `2.64`、stock 20d return `21.06%` vs `12.05%`、stock 60d return `42.40%` vs `30.23%` と、割安よりも「急騰後に入った forward EPS trade」が典型的な左裾になっている。

Right-tail p90 は 42 trades、avg `+87.63%`、median `+54.62%`。PBR は `1.04` と低すぎず、stock 20d / 60d return は severe-loss cohort より低い。つまり高バリュエーションそのものより、短期過熱と組み合わさったときが危険。

### Interpretation

この runner の SoT は `market.duckdb` v3。market-backed universe preset は window start 日で解決し、trade enrichment は entry 日で特徴量を切る。Future leak を避けるため、latest stock master による market classification は使わない。

現時点の読みは、`forward_eps_driven` の本質は「forward EPS 条件だけで勝つ」より「右裾を取りに行く代わりに、過熱した外れ trade をどう小さくするか」。特に Prime は trade 数が十分あるため、technical overheat overlap を hard exclude ではなく haircut / sizing rule として portfolio-level 検証する価値が高い。

### Production Implication

この readout だけでは production YAML に hard filter を追加しない。次の検証順は以下。

1. Prime を主対象に、`stock_return_20d` / `stock_return_60d` / `risk_adjusted_return` の overheat overlap に対する 0.5x sizing を portfolio-level backtest で検証する。
2. Standard low forward PER は promising だが、直近 holdout が無いため別 research として期間拡張・市場別再検証する。
3. Low PBR は holdout で良いが full-history の平均は弱い。PBR は entry filter ではなく tail diagnostic / secondary rank として扱う。
4. Forward EPS growth margin の追加閾値は、現行条件と重複が大きく、直近 holdout も弱いため優先度を下げる。

### Caveats

- 指標は trade-level であり、portfolio CAGR / max drawdown の直接推定ではない。
- size haircut は trade return に対する proxy。実際の capital sharing / position overlap は別途 portfolio-level verification が必要。
- Holdout の trade 数が少ない場合、full-history と holdout の矛盾を優先して扱う。
- Standard の直近 holdout trade は 0 件。Standard 専用 rule は現時点では仮説扱い。

### Source Artifacts

- Bundle: `~/.local/share/trading25/research/strategy-audit/forward-eps-driven-v3-factor-decomposition/20260430_forward_eps_driven_v3_factor_decomposition_prime_ex_topix500_rerun2`
- Results DB: `~/.local/share/trading25/research/strategy-audit/forward-eps-driven-v3-factor-decomposition/20260430_forward_eps_driven_v3_factor_decomposition_prime_ex_topix500_rerun2/results.duckdb`
- Summary: `~/.local/share/trading25/research/strategy-audit/forward-eps-driven-v3-factor-decomposition/20260430_forward_eps_driven_v3_factor_decomposition_prime_ex_topix500_rerun2/summary.md`
- Runner: `uv run --project apps/bt python apps/bt/scripts/research/run_forward_eps_driven_v3_factor_decomposition.py --dataset primeExTopix500`

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_forward_eps_driven_v3_factor_decomposition.py \
  --dataset primeExTopix500 \
  --holdout-months 6 \
  --run-id 20260430_forward_eps_driven_v3_factor_decomposition_prime_ex_topix500_rerun2
```

## Artifact Tables

- `dataset_summary_df`: market.duckdb v3 universe summary.
- `scenario_summary_df`: `forward_eps_driven` の window 別 single-name 実トレード集計。
- `market_scope_summary_df`: entry 日 market scope ごとの trade-level metrics。
- `factor_bucket_summary_df`: fundamentals / technical / market regime / freshness の factor quantile buckets。
- `factor_contrast_summary_df`: 各 factor の low bucket vs high bucket contrast。
- `tail_profile_df`: all / severe loss / right tail / non severe の feature median profile。
- `action_candidate_summary_df`: hard keep / hard exclude / size haircut 候補の trade-level proxy。
- `enriched_trade_df`: entry 時点特徴量付き trade ledger。
