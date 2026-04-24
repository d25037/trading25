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

## Current Read

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
