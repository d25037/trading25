# Forward EPS Trade Archetype Decomposition

## Purpose

`production/forward_eps_driven` の実トレード台帳を起点に、どの種類の銘柄が利益・損失を作っているかを分解する。

この研究は annual all-stock panel ではない。`forward_eps_driven` の entry/exit 条件を実際に通過して約定した銘柄だけを対象に、entry 時点で PIT-safe に以下を付与する。

- forward EPS growth と閾値からの margin
- risk-adjusted return / volume ratio / RSI / TOPIX regime
- PBR
- forward PER
- market cap
- ADV60 は capacity diagnostic として保持するが、選択 overlay の主因子には使わない

## Published Readout

### Decision

2026-04-30 の v3 再精査では、`production/forward_eps_driven` を `market.duckdb` v3 の `primeExTopix500` universe で再実行した。universe は window start 日の `stock_master_daily` で PIT 解決し、trade enrichment の market metadata も entry 日の `stock_master_daily` を使った。

結論は、旧 `primeExTopix500_20260325` snapshot readout より慎重に読むべき。v3 では full-history の baseline が 412 trades、avg +6.26%、median -3.82%、severe loss rate 23.06% になり、旧 snapshot の avg +14.00% よりかなり低い。過熱 overlap は依然として左尾を濃くするが、avg return まで悪いサブセットではなくなったため、即時の期待値改善 filter ではなく left-tail / drawdown risk control 候補として扱う。

### Main Findings

#### 結論: v3 では baseline の平均リターンが低下し、直近 holdout は小標本ながら悪くない

| window | period | trades | symbols | win rate | avg trade | median trade | severe loss | worst |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| train_pre_holdout | 2016-05-02 -> 2025-10-27 | 388 | 273 | 37.63% | +6.05% | -4.07% | 23.97% | -35.34% |
| holdout_6m | 2025-10-28 -> 2026-04-28 | 22 | 20 | 40.91% | +6.55% | -1.76% | 13.64% | -32.05% |
| full | 2016-05-02 -> 2026-04-28 | 412 | 288 | 36.89% | +6.26% | -3.82% | 23.06% | -35.34% |

#### 結論: v3 の `primeExTopix500` 約定には historical Standard 銘柄も混ざる

| window | market_scope | trades | avg trade | median trade | severe loss | median PBR | median forward PER | median mcap bn JPY |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| full | prime | 344 | +5.89% | -4.07% | 24.42% | 0.94 | 13.91 | 23.53 |
| full | standard | 68 | +8.13% | -1.57% | 16.18% | 0.74 | 11.10 | 12.20 |
| holdout_6m | prime | 22 | +6.55% | -1.76% | 13.64% | 1.58 | 15.93 | 40.51 |

`primeExTopix500` は entry 日 PIT の market_scope で見ると full-history で Standard が 68 trades 存在する。これは v3 で stock master を entry 日に戻したために見える差分で、latest master 固定では見落とす。

#### 結論: value overlay は採用決定に足る安定性がない

| window | scope | candidate | trades | coverage | avg trade | median trade | severe loss | delta avg | delta severe |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| holdout_6m | prime | low_pbr_q1 | 8 | 36.36% | +11.97% | +2.21% | 12.50% | +5.42pt | -1.14pt |
| holdout_6m | prime | low_forward_per_q1 | 8 | 36.36% | -2.86% | -3.85% | 12.50% | -9.41pt | -1.14pt |
| full | prime | value_composite_q1 | 115 | 33.43% | +5.72% | -2.92% | 13.91% | -0.17pt | -10.51pt |
| full | standard | low_forward_per_q1 | 23 | 33.82% | +21.74% | +0.64% | 8.70% | +13.61pt | -7.48pt |
| full | all | value_core_low_pbr_low_fper_small_cap | 35 | 8.50% | +1.70% | -3.94% | 14.29% | -4.56pt | -8.77pt |

低PBRは holdout では良く見えるが 8 trades。full-history では severe loss を下げる一方で avg を落とす。低forward PER は historical Standard 部分では強いが、Prime 側では弱い。旧 Standard snapshot の value edge は、この `primeExTopix500` 実約定母集団にそのまま持ち込めない。

#### 結論: 過熱 overlap は期待値改善ではなく左尾抑制として読む

| rule | window | excluded trades | excluded avg | excluded severe | kept trades | kept avg | kept severe | kept worst |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| v3 Q80 overlap >=2 | full | 75 | +9.31% | 42.67% | 337 | +5.58% | 18.69% | -24.31% |
| old thresholds overlap >=2 | full | 61 | +12.01% | 40.98% | 351 | +5.26% | 19.94% | -24.31% |
| v3 Q80 overlap >=2 | holdout_6m | 5 | +2.51% | 40.00% | 17 | +7.74% | 5.88% | -15.20% |
| old thresholds overlap >=2 | holdout_6m | 7 | +1.26% | 28.57% | 15 | +9.02% | 6.67% | -15.20% |

旧 readout では `overheated_overlap_count >= 2` が avg も悪い尾だった。v3 では excluded 側に右尾も残り、full-history では excluded avg が baseline より高い。ただし severe loss rate は 40% 超、worst も -35.34% で、kept 側は worst -24.31% まで改善する。したがって「除けば平均が上がる」ではなく、「大きい負けを抑える代わりに一部右尾も捨てる」ルールとして再検証すべき。

### Interpretation

v3 の最大の差分は universe と market metadata の時点が揃ったこと。latest stock master 固定ではなく entry 日の `stock_master_daily` で見るため、過去の市場区分が約定単位で復元される。結果として `primeExTopix500` の中に historical Standard trade が見え、value 系の解釈も市場別に割る必要が出た。

平均リターンだけなら、今回の v3 full-history では overheat pruning は採用しにくい。一方で severe loss rate と worst trade を見ると、過熱 overlap は依然として左尾の濃縮条件になっている。production に昇格するなら、単純な entry 除外ではなく、position sizing の減額、max exposure、または exit/hold duration との相互作用で検証する方がよい。

### Production Implication

現時点で production YAML に実行条件として追加する判断はしない。優先順位は以下。

1. `overheated_overlap_count >= 2` を hard exclude ではなく risk cap / size haircut として検証する。
2. Low PBR は holdout で良いが標本 8 trades のため、単独採用しない。
3. Historical Standard 側の low forward PER edge は別 research として切り出す。`primeExTopix500` 全体の overlay に混ぜると Prime 側の弱さで希釈される。

### Caveats

- `holdout_6m` は 22 trades しかない。
- 指標は trade-level。CAGR や portfolio equity curve の改善ではない。
- v3 Q80 threshold は今回の full-history trade distribution から再計算したもので、将来の live rule では train-only calibration が必要。

### Source Artifacts

- Bundle: `~/.local/share/trading25/research/strategy-audit/forward-eps-trade-archetype-decomposition/20260430_forward_eps_trade_archetype_v3_prime_ex_topix500`
- Results DB: `~/.local/share/trading25/research/strategy-audit/forward-eps-trade-archetype-decomposition/20260430_forward_eps_trade_archetype_v3_prime_ex_topix500/results.duckdb`
- Runner: `uv run --project apps/bt python apps/bt/scripts/research/run_forward_eps_trade_archetype_decomposition.py --dataset primeExTopix500 --holdout-months 6 --run-id 20260430_forward_eps_trade_archetype_v3_prime_ex_topix500`

## Runner

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_forward_eps_trade_archetype_decomposition.py \
  --dataset primeMarket_20260424 \
  --holdout-months 6 \
  --output-root /tmp/trading25-research \
  --run-id 20260424_forward_eps_value_exposure_prime_market

uv run --project apps/bt python apps/bt/scripts/research/run_forward_eps_trade_archetype_decomposition.py \
  --dataset standardMarket_20260424 \
  --holdout-months 6 \
  --output-root /tmp/trading25-research \
  --run-id 20260424_forward_eps_value_exposure_standard_market
```

## Artifact Tables

- `scenario_summary_df`: `forward_eps_driven` の single-name 実トレード集計。
- `trade_ledger_df`: 実トレード台帳。
- `enriched_trade_df`: 実トレードに entry 時点特徴量を付与した明細。
- `market_scope_summary_df`: market scope ごとの成績と中央値特徴量。
- `feature_bucket_summary_df`: 既存の EPS/出来高/地合い特徴量 bucket。
- `value_feature_bucket_summary_df`: PBR / forward PER / market cap / value composite の bucket。
- `overlay_candidate_summary_df`: 既存 archetype overlay。
- `value_overlay_candidate_summary_df`: 低PBR・低forward PER・小型の追加 overlay。

## Previous Read (2026-04-24 Snapshot)

2026-04-24 時点の `primeMarket_20260424` / `standardMarket_20260424` では、value factor は「production strategy の母集団をさらに絞る overlay」として読むべきで、forward EPS 成長シグナルの置き換えではない。

- Standard full-history: value core は baseline より強いが、holdout 6m では悪化した。
- Prime full-history: small market cap が強いが、low forward PER / low PBR / value core は baseline に負けた。
- Prime holdout 6m: low PBR は baseline を上回ったが、サンプルは23 trades と小さい。
- ADV60 は選択因子としては採用せず、capacity / liquidity diagnostic として残す。

`primeExTopix500_20260325` の全期間では、良いサブセットを足すよりも悪いサブセットを除く方が現実的だった。特に 20d/60d price run-up と risk-adjusted return の Q5 は互いに相関しており、単独除外ではなく「過熱条件が2つ以上重なる銘柄」を除外するのが最もバランスがよい。

```yaml
exclude_if:
  overheated_overlap_count:
    conditions:
      - stock_return_60d_pct >= 58.78
      - stock_return_20d_pct >= 33.71
      - risk_adjusted_return_value >= 3.886
    min_count: 2
```

この候補は `production/forward_eps_driven` の YAML 実行条件としてはまだ未実装。現時点では strategy audit の研究結果として扱い、実装する場合は entry feature availability と screening/backtest の同一性を確認してから signal system へ昇格する。

固定の採用判断は `baseline-2026-04-24.md` を参照する。
