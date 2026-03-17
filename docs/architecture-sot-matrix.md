# SoT Matrix

## Decision
- `J-Quants` は ingest/proxy 専用であり、verification path の SoT ではない。
- `market.duckdb` は screening / charts / analytics の SoT である。
- `dataset snapshot` (`dataset.duckdb` + `manifest.v2.json`) は backtest / optimize / attribution / lab の SoT である。
- `SignalProcessor + compiled strategy IR + signal registry` は signal semantics の SoT である。

## Guardrails
- verification 系 route / service は live J-Quants client を import しない。許可対象は proxy / sync / bootstrap のみ。
- chart fundamentals / margin / signal overlay は local market DB だけを読む。
- screening と backtest は missing required data を `skip` ではなく `false` として扱う。
- chart overlay は `strategy_name` 指定時に `SignalProcessor` ベースで screening/backtest と同じ signal semantics を使う。

## Provenance Contract
- screening / fundamentals / ROE / margin / signal responses は共通 `provenance` を返す。
- `provenance` には `source_kind`, `market_snapshot_id`, `dataset_snapshot_id`, `reference_date`, `loaded_domains`, `strategy_name`, `strategy_fingerprint`, `warnings` を含める。
- `diagnostics` には `missing_required_data`, `used_fields`, `effective_period_type`, `warnings` を含める。

## Rationale
- chart を screening/backtest の検算面として成立させるには、live fetch を排除し、入力データと signal semantics を shared builder / shared processor に固定する必要がある。
- provenance を UI に出すことで「どの SoT を見ているか」を実行時に確認できる。
