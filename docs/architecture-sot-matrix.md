# SoT Matrix

## Decision
- `J-Quants` は ingest/proxy 専用であり、verification path の SoT ではない。
- `market.duckdb` は screening / charts / analytics / normal backtest / optimize / attribution / lab の SoT である。
- `market.duckdb.stock_data_raw` は price ingest の SoT であり、`O/H/L/C/Vo + adjustment_factor` を保持する。
- `market.duckdb.stock_data` は `stock_data_raw` から再計算される local adjusted projection であり、既存 read path 互換の公開 series である。
- `market.duckdb.statements` は raw vendor fundamentals provenance の SoT である。official EPS/BPS history を明示表示する場合は raw fields を使ってよい。
- `market.duckdb.statement_metrics_adjusted` は adjusted EPS/BPS/forecast EPS/dividend の consumer-facing SoT である。
- `market.duckdb.daily_valuation` は PER/PBR/forward PER と valuation 用 adjusted EPS/BPS の consumer-facing SoT である。
- `universe_preset` + `stock_master_daily` は all-stock universe selection の PIT SoT である。
- `dataset snapshot` (`dataset.duckdb` + `manifest.v2.json`) は normal run の SoT ではない。`data_source=dataset_snapshot` + `static_universe=true` を明示した archived reproducibility run だけで使う。
- `SignalProcessor + compiled strategy IR + signal registry` は signal semantics の SoT である。

## Guardrails
- verification 系 route / service は live J-Quants client を import しない。許可対象は proxy / sync / bootstrap のみ。
- chart fundamentals / margin / signal overlay は local market DB だけを読む。
- stock split / reverse split の調整は J-Quants `Adj*` 永続値に依存せず、`stock_data_raw` の raw OHLCV と `adjustment_factor` から local で再投影する。
- fundamentals / ranking / screening が valuation または adjusted per-share fields を必要とする場合は `statement_metrics_adjusted` / `daily_valuation` を優先し、raw `statements` から silent recompute しない。
- screening と backtest は missing required data を `skip` ではなく `false` として扱う。
- `shared_config.dataset` は normal run で unsupported。`shared_config.universe_preset` を使い、物理 snapshot は `dataset_snapshot` として明示する。
- chart overlay は `strategy_name` 指定時に `SignalProcessor` ベースで screening/backtest と同じ signal semantics を使う。

## Provenance Contract
- screening / fundamentals / ROE / margin / signal responses は共通 `provenance` を返す。
- `provenance` には `source_kind`, `market_snapshot_id`, `dataset_snapshot_id`, `reference_date`, `loaded_domains`, `strategy_name`, `strategy_fingerprint`, `warnings` を含める。
- `diagnostics` には `missing_required_data`, `used_fields`, `effective_period_type`, `warnings` を含める。

## Rationale
- chart を screening/backtest の検算面として成立させるには、live fetch を排除し、入力データと signal semantics を shared builder / shared processor に固定する必要がある。
- provenance を UI に出すことで「どの SoT を見ているか」を実行時に確認できる。
