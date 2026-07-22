# SoT Matrix

## Decision
- `J-Quants` は ingest/proxy 専用であり、verification path の SoT ではない。
- `market.duckdb` は screening / charts / analytics / normal backtest / optimize / attribution / lab の SoT である。
- Market Data Plane は schema v5 / `provider_adjusted_v1` のみを受理する。同期 API は既定の `incremental` と `resetBeforeSync=true` 必須の `initial` だけを提供する。incompatible / malformed root を含む full rebuild は `initial` が既存 `market.duckdb` / `parquet/` を削除して新しい Market v5 root を構築する。
- `market.duckdb.stock_data_raw` は provider publication の SoT であり、raw `O/H/L/C/Vo + adjustment_factor` と provider-adjusted `AdjO/H/L/C/Vo` を同じper-code windowで保持する。
- `market.duckdb.stock_data` は provider-adjusted current convenience projectionである。cutoff-aware Fundamentals / liquidityの入力には使わない。
- `stock_provider_windows` はper-code coverage / provider as-of / canonical source fingerprintのSoT、`stock_adjustment_events`はそのwindowに所有されるevent ledgerのSoTである。
- `market.duckdb.statements` は raw vendor fundamentals provenance の SoT である。official EPS/BPS history を明示表示する場合は raw fields を使ってよい。
- `market.duckdb.statement_metrics_adjusted` は current provider-basis EPS/BPS/forecast EPS/dividend の consumer-facing SoTであり、`current_basis_fundamentals_state` / `current_basis_recompute_pending` がmaterialization freshnessを所有する。
- `market.duckdb.daily_valuation` は PER/PBR/PSR/forward PER/forward PSR と valuation 用 adjusted EPS/BPS、actual/forecast sales の consumer-facing SoT である。
- `universe_preset` + `stock_master_daily` は all-stock universe selection の PIT SoT である。
- `dataset snapshot` (`dataset.duckdb` + 物理`manifest.v2.json`) はpayload `schemaVersion: 4`のMarket v5 provider-basis bundleだけを受理する。normal run の SoT ではなく、`data_source=dataset_snapshot` + `static_universe=true` を明示した archived reproducibility run だけで使う。
- `SignalProcessor + compiled strategy IR + signal registry` は signal semantics の SoT である。
- Research Published Readout の PIT universe invalidation / rerun queue は [`research-pit-invalidation-register.md`](research-pit-invalidation-register.md) を SoT とする。

## Guardrails
- verification 系 route / service は live J-Quants client を import しない。許可対象は proxy / sync / bootstrap のみ。
- chart fundamentals / margin / signal overlay は local market DB だけを読む。
- provider-adjusted OHLCVはJ-Quants publication値を永続化し、raw rows・per-code window・event ledgerのcanonical fingerprintをDuckDB側のset-based aggregateでexact照合する。Pythonへはper-symbol evidenceだけを返し、local reprojectionやcurrent/latest fallbackは行わない。
- provider planはraw/window/event/current-basis sourceが存在する場合に必須とし、完全なfresh empty DBは`empty_source` informationalとして扱う。appendでwindow fingerprintが変わる場合はhistorical event ownershipも同一transactionで更新し、event Parquet exportをdirtyにする。
- normal market syncが`statement_metrics_adjusted` / `daily_valuation`のcurrent provider-basis materializationを担う。standalone materialize routeは持たず、read requestもmaterializationやservice-local adjustmentを行わない。
- fundamentals / ranking / screening が valuation、PSR、forecast sales、または adjusted per-share fields を必要とする場合は `statement_metrics_adjusted` / `daily_valuation` を優先し、raw `statements` から silent recompute しない。
- Fundamentals GET/POSTは同一PIT bundleを読み、`to`をknowledge cutoff、`from`をdisplay lower boundとして厳格なISO date validationを行う。current/latest fallbackと`stocks_latest` fallbackは禁止し、missing/inconsistent basisまたはexact master snapshotの欠損は409、未上場は404、開示なしは200 empty dataで返す。
- screening と backtest は missing required data を `skip` ではなく `false` として扱う。
- `shared_config.dataset` は normal run で unsupported。`shared_config.universe_preset` を使い、物理 snapshot は `dataset_snapshot` として明示する。
- chart overlay は `strategy_name` 指定時に `SignalProcessor` ベースで screening/backtest と同じ signal semantics を使う。
- research readout は historical universe に latest membership を固定した headline を production / Ranking / Screening evidence として使わない。
- `initial` は破壊的操作として typed confirmation を必須とし、`incremental` は既存 root の更新と recoverable gap の backfill に限定する。in-place migration、dual read、旧 schema fallback は行わない。

## Provenance Contract
- screening / fundamentals / ROE / margin / signal responses は共通 `provenance` を返す。
- Fundamentals responseの必須`asOfDate`はeffective local market date、`provenance.reference_date`はknowledge cutoff、`priceBasisDate`はcurrent provider-basis frontierを表す。
- `provenance` には `source_kind`, `market_snapshot_id`, `dataset_snapshot_id`, `reference_date`, `loaded_domains`, `strategy_name`, `strategy_fingerprint`, `warnings` を含める。
- `diagnostics` には `missing_required_data`, `used_fields`, `effective_period_type`, `warnings` を含める。

## Rationale
- chart を screening/backtest の検算面として成立させるには、live fetch を排除し、入力データと signal semantics を shared builder / shared processor に固定する必要がある。
- provenance を UI に出すことで「どの SoT を見ているか」を実行時に確認できる。
