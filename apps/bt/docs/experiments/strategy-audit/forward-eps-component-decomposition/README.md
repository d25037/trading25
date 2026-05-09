# Forward EPS Component Decomposition

## Purpose

`production/forward_eps_driven` の実現トレードを、既存の `forward-eps-trade-archetype-decomposition` bundle から再利用し、以下の4成分に分解する。

- `value`: 低PBR、低forward PER、小型時価総額
- `expectation`: forward EPS growth / threshold margin、開示 freshness
- `attention`: volume ratio / volume-ratio margin
- `price_momentum`: risk-adjusted return、20d/60d run-up、RSI10

ADV60 は capacity diagnostic として元 bundle に残すが、attention 成分には入れない。これは「流動性があるから alpha」と誤読しないため。

## Published Readout

### Decision

2026-05-09 時点の結論は、`forward_eps_driven` を単一の「forward EPS growth alpha」として読むのは粗い、というもの。Standard では value 成分がかなり強く、Prime ex TOPIX500 では出来高 attention が右尾を作る一方で左尾も濃くする。price momentum / overheat は両方で悪化寄り。

したがって次にやる価値があるのは、`forward_eps_driven` を「value + event attention + overheat risk control」に分けて、hard filter ではなく sizing / risk budget で扱う検証。特に Standard は value が主因で、Prime は attention の右尾 capture と overheat tail の制御が主題。

### Main Findings

#### Prime ex TOPIX500: attention は右尾を作るが、value+attention の同時条件は悪い

| scope | candidate | trades | avg trade | median | severe loss | delta avg | delta severe |
|---|---|---:|---:|---:|---:|---:|---:|
| all | baseline_all | 412 | +6.26% | -3.82% | 23.06% | +0.00pt | +0.00pt |
| all | value_q80 | 83 | +5.58% | -1.52% | 14.46% | -0.68pt | -8.60pt |
| all | attention_q80 | 81 | +9.53% | -4.03% | 30.86% | +3.27pt | +7.81pt |
| all | price_momentum_q80 | 83 | +3.32% | -3.26% | 36.14% | -2.94pt | +13.09pt |
| all | value_attention_q80 | 22 | -6.11% | -6.02% | 31.82% | -12.37pt | +8.76pt |
| all | value_without_attention | 61 | +9.80% | +2.34% | 8.20% | +3.54pt | -14.86pt |
| prime | baseline_all | 344 | +5.89% | -4.07% | 24.42% | +0.00pt | +0.00pt |
| prime | attention_q80 | 67 | +11.90% | -3.94% | 31.34% | +6.01pt | +6.92pt |
| prime | price_momentum_q80 | 69 | -0.80% | -3.94% | 40.58% | -6.69pt | +16.16pt |
| prime | value_without_attention | 48 | +11.09% | +2.51% | 8.33% | +5.20pt | -16.09pt |

`attention_q80` は平均を押し上げるが severe loss も増える。これは「出来高急増はイベント参加・右尾 capture だが、crowding/tail cost もある」という読み。`value_attention_q80` が悪いので、単純に value と出来高を同時に強くすればよいわけではない。

Prime ex TOPIX500 内の historical Standard subset は 68 trades と小さいが、ここでは `price_momentum_q80` が +22.71% と強い一方、`attention_q80` は -1.81%。これは母集団の Standard 全体とは別物なので、production rule に混ぜない。

#### Standard: value は明確に効く。attention は holdout では強いが単独採用には粗い

| scope | candidate | trades | avg trade | median | severe loss | delta avg | delta severe |
|---|---|---:|---:|---:|---:|---:|---:|
| standard | baseline_all | 777 | +3.64% | -4.71% | 29.60% | +0.00pt | +0.00pt |
| standard | value_q80 | 156 | +8.22% | -1.90% | 17.31% | +4.58pt | -12.29pt |
| standard | expectation_q80 | 156 | +7.90% | -4.41% | 30.77% | +4.26pt | +1.17pt |
| standard | attention_q80 | 152 | +4.98% | -4.60% | 34.21% | +1.35pt | +4.61pt |
| standard | price_momentum_q80 | 156 | +2.09% | -7.31% | 38.46% | -1.55pt | +8.86pt |
| standard | value_attention_q80 | 29 | +17.78% | -2.23% | 20.69% | +14.14pt | -8.91pt |
| standard | value_attention_expectation_q80 | 8 | +29.16% | +12.16% | 12.50% | +25.52pt | -17.10pt |
| standard | value_without_attention | 127 | +6.04% | -0.94% | 16.54% | +2.40pt | -13.07pt |

Standard は `value_q80` が平均・左尾の両方で改善し、regression でも value の係数が最も明確。`value_attention_expectation_q80` は headline が強いが 8 trades しかないため、採用候補ではなく「この3成分の重なりは探索価値あり」という位置づけ。

Holdout 6m では Standard の `attention_q80` が +15.39%、`expectation_q80` が +13.21% と強い一方、`value_q80` は +0.78% に落ちる。直近だけで value を捨てるには短すぎるが、attention / expectation が recent regime で効いている可能性は残る。

#### Component bucket と regression は同じ方向を示す

| universe | component | weak bucket avg | strong bucket avg | strong bucket severe |
|---|---|---:|---:|---:|
| Prime ex TOPIX500 all | value | +13.28% | +5.58% | 14.46% |
| Prime ex TOPIX500 all | attention | -0.34% | +9.53% | 30.86% |
| Prime ex TOPIX500 all | price_momentum | +12.22% | +3.32% | 36.14% |
| Standard | value | -3.25% | +8.22% | 17.31% |
| Standard | expectation | +0.04% | +7.90% | 30.77% |
| Standard | price_momentum | +6.33% | +2.09% | 38.46% |

Full-history multivariate regression では、Standard の `value` が +4.27pt / 1sd、t=3.48。Prime ex TOPIX500 の Prime scope では `attention` が +5.52pt / 1sd、t=2.54、`price_momentum` は -4.22pt / 1sd、t=-1.93。R2 はどちらも 2-3% 程度で低く、これだけで forecast rule を置き換えるモデルではない。

### Interpretation

`forward_eps_driven` は「ただのバリュー投資を出来高上昇で filter しただけ」とまでは言い切れないが、少なくとも Standard では value が相当大きい。Prime 側では value よりも attention と price-momentum tail の扱いが重要で、出来高上昇は alpha というより event participation / right-tail capture に近い。

ここでの `price_momentum` は classical momentum ではなく entry 直前の短期過熱成分。これが悪いのは、過去の overheat filter 研究と整合する。

### Production Implication

現時点で `production/forward_eps_driven` の entry hard filter を追加しない。

次の検証候補は以下。

1. Standard: value component を score / sizing に入れた場合、CAGR・Sharpe・turnover が改善するか。
2. Prime ex TOPIX500: attention high は 1.0x のまま、price_momentum high は haircut する risk budget rule。
3. Market-specific: Standard と Prime を同じ rule にしない。

### Caveats

- trade-level decomposition であり、portfolio CAGR / Sharpe ではない。
- 成分スコアは各 `window_label x market_scope` 内の retrospective percentile。production 化する場合は train-only calibration が必要。
- Prime ex TOPIX500 の historical Standard subset は 68 trades と小さく、Standard market 全体の readout と混ぜない。

### Source Artifacts

- Prime ex TOPIX500 bundle: `~/.local/share/trading25/research/strategy-audit/forward-eps-component-decomposition/20260509_forward_eps_component_prime_ex_topix500`
- Standard bundle: `~/.local/share/trading25/research/strategy-audit/forward-eps-component-decomposition/20260509_forward_eps_component_standard`
- Input Prime ex TOPIX500 bundle: `~/.local/share/trading25/research/strategy-audit/forward-eps-trade-archetype-decomposition/20260430_forward_eps_trade_archetype_v3_prime_ex_topix500`
- Input Standard bundle: `~/.local/share/trading25/research/strategy-audit/forward-eps-trade-archetype-decomposition/20260430_forward_eps_trade_archetype_v3_standard`

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_forward_eps_component_decomposition.py \
  --input-bundle ~/.local/share/trading25/research/strategy-audit/forward-eps-trade-archetype-decomposition/20260430_forward_eps_trade_archetype_v3_prime_ex_topix500 \
  --run-id 20260509_forward_eps_component_prime_ex_topix500

uv run --project apps/bt python apps/bt/scripts/research/run_forward_eps_component_decomposition.py \
  --input-bundle ~/.local/share/trading25/research/strategy-audit/forward-eps-trade-archetype-decomposition/20260430_forward_eps_trade_archetype_v3_standard \
  --run-id 20260509_forward_eps_component_standard
```

## Artifact Tables

- `component_trade_df`: 実現トレードに4成分スコアを付与した明細。
- `component_bucket_summary_df`: 成分別 Q1-Q5 bucket の trade metrics。
- `component_overlap_summary_df`: `value_q80` / `attention_q80` / overlap などの候補別 metrics。
- `component_regression_summary_df`: trade return に対する成分 score の単回帰・多変量回帰。
