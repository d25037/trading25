# market.duckdb SoT v3 Migration Plan

Date: `2026-04-29`

## Decision

`market.duckdb` を、research / backtest / screening / ranking / Symbol Workbench が参照する
market data の単一 SoT にする。

ここでいう market data は、price、fundamentals、listed master、market / scale category、
index membership、TOPIX / sector / option / margin など、分析・実行判断に使うデータを指す。
portfolio / watchlist / jobs metadata、strategy YAML、backtest result artifacts、research docs は対象外。

既存の `dataset.duckdb` snapshot は backtest 入力 SoT から外す。必要なら再現性用 artifact として
残せるが、通常の research / backtest / screening / ranking は `market.duckdb` を直接読み、
universe は実行日ごとに point-in-time 解決する。

## Non-Negotiable Scope

- `stocks` の latest snapshot を過去全期間に貼る設計をやめる。
- `prime` / `standard` / `growth` / `topix100` / `primeExTopix500` は、実行日の
  master / membership から as-of 解決する。
- TOPIX100 のような scale category 由来 universe は、`stock_master_daily.date = signal_date`
  の row から解決する。
- TOPIX500 のように scale category だけでは厳密に表せない universe は、別途
  `index_membership_daily` を SoT にする。代替 proxy を使う場合は名前に `proxy` を含める。
- 旧 `market.duckdb` と旧 dataset snapshot は自動移行しない。`initial sync` + destructive reset を前提にする。
- `market.duckdb` file を論理 SoT とし、Parquet は export/cache/transport artifact に格下げする。

## Target Schema

### Required tables

| Table | Key | Role |
| --- | --- | --- |
| `market_schema_version` | `version` | incompatible schema 判定と reset guidance |
| `stock_master_daily` | `(date, code)` | `/equities/master?date=...` の PIT listed master |
| `stock_master_intervals` | `(code, valid_from, valid_to, fingerprint)` | daily master から作る read optimization |
| `stocks_latest` | `code` | latest convenience table。PIT判断には使わない |
| `stock_data_raw` | `(code, date)` | raw `O/H/L/C/Vo + adjustment_factor` SoT |
| `stock_data` | `(code, date)` | local adjusted projection |
| `topix_data` | `date` | trading calendar anchor / benchmark |
| `indices_data` | `(code, date)` | sector / index OHLC |
| `index_master` | `code` | index catalog |
| `index_membership_daily` | `(date, index_code, code)` | TOPIX500 など scale category だけで表せない membership |
| `statements` | current key | fundamentals SoT |
| `margin_data` | current key | margin SoT |
| `options_225_data` | current key | N225 options SoT |
| `stock_data_minute_raw` | current key | intraday minute raw SoT |

### `stock_master_daily` columns

Minimum required columns:

- `date`
- `code`
- `company_name`
- `company_name_english`
- `market_code`
- `market_name`
- `sector_17_code`
- `sector_17_name`
- `sector_33_code`
- `sector_33_name`
- `scale_category`
- `listed_date`
- `created_at`

The row is the master state known for `date`. The row must not be forward-filled across days
unless a dedicated interval builder marks the interval derived from adjacent daily snapshots.

## Sync Contract

### Initial sync

Initial sync becomes destructive for market time-series storage:

1. User confirms `resetBeforeSync=true`.
2. Delete/recreate `market-timeseries/market.duckdb` and `market-timeseries/parquet/`.
3. Create schema v3 tables.
4. Fetch TOPIX history first to establish the trading calendar.
5. For every TOPIX trading date in the configured history window, fetch `/equities/master?date=YYYY-MM-DD`.
6. Publish all rows into `stock_master_daily`.
7. Build `stock_master_intervals`.
8. Build `stocks_latest` from the latest complete master date.
9. Sync price / fundamentals / margin / options / indices using existing stage logic, but all universe targeting must use `stock_master_daily` or `stock_master_intervals`.
10. Record master coverage diagnostics in sync metadata.

### Incremental sync

1. Inspect latest `topix_data` date and latest `stock_master_daily` date.
2. Fetch new TOPIX dates.
3. Fetch `/equities/master?date=...` for new trading dates only.
4. Rebuild affected intervals and `stocks_latest`.
5. Continue price / fundamentals / margin / options / indices sync.

### Repair sync

Repair may backfill missing `stock_master_daily` dates. It must not silently treat latest
`stocks_latest` as a substitute for missing daily master.

## Universe Resolver Contract

Add one backend resolver used by research, backtest, screening, ranking, and Symbol Workbench:

```text
resolve_universe(as_of_date, preset, filters) -> code set + provenance
```

Required behavior:

- `as_of_date` is mandatory for historical/research/backtest/screening/ranking calls.
- `prime`, `standard`, `growth` resolve from `stock_master_daily.market_code` as of `as_of_date`.
- legacy names (`prime`, `standard`, `growth`) remain aliases for current market codes (`0111`, `0112`, `0113`).
- `topix100` resolves from `stock_master_daily.scale_category in ('TOPIX Core30', 'TOPIX Large70')`.
- `primeExTopix500` resolves as Prime as of date minus TOPIX500 membership as of date.
- if exact TOPIX500 membership is unavailable, the resolver returns an unsupported error rather than using latest membership.
- every response includes provenance: source table, as-of date, universe preset, coverage warnings, and row counts.

## Dataset Policy

`dataset` stops meaning "physical universe-specific backtest input".

Old model:

```text
datasets/prime/
datasets/standard/
datasets/primeExTopix500/
```

New model:

```text
market.duckdb
universe preset = prime | standard | growth | topix100 | primeExTopix500 | custom
```

Allowed residual uses for dataset bundles:

- archived reproducibility snapshot
- export/import artifact
- CI fixture

Not allowed:

- selecting a latest-universe dataset as the normal backtest/research SoT
- using a fixed dataset universe across historical dates without explicit `static_universe=true`

## Affected Surfaces

| Surface | Required change |
| --- | --- |
| Research | read `market.duckdb` directly; all universe construction via resolver |
| Backtest | replace dataset snapshot SoT with market reader + universe resolver |
| Optimization / Lab | inherit backtest data access; no dataset snapshot shortcut |
| Screening | keep market DB SoT; use resolver for selected strategy universe |
| Ranking | use resolver for daily ranking universe and historical ranking |
| Symbol Workbench | read latest/historical symbol data from market DB; expose PIT master provenance where relevant |
| Dataset API/UI | demote to snapshot/export tooling or replace with universe preset UI |
| Market DB page | show schema version, daily master coverage, interval build status, membership coverage |
| Validation | fail/warn on missing daily master dates, latest-only universe fallback, unsupported TOPIX500 exact membership |
| Research docs | mark old latest-universe studies as invalid if they used current membership for historical dates |

## Implementation Phases

### Phase 0: Spec and issue gate

- Keep this doc as the design SoT.
- Track execution in one GitHub Issue.
- Do not start implementation until the issue acceptance criteria are reviewed.

### Phase 1: Schema v3 and incompatibility guard

- Add `market_schema_version`.
- Add `stock_master_daily`, `stock_master_intervals`, `stocks_latest`, and `index_membership_daily`.
- Mark old DBs incompatible.
- Require destructive `initial sync` reset for old DBs.
- Update DB stats/validation to expose schema version and master coverage.

### Phase 2: Daily master initial/incremental sync

- Add `/equities/master?date=...` backfill over TOPIX trading dates.
- Persist daily master rows.
- Build intervals.
- Continue to expose latest stock search through `stocks_latest`.
- Add tests for missing date detection and no latest fallback.

### Phase 3: Universe resolver

- Implement one resolver for `prime`, `standard`, `growth`, `topix100`, `primeExTopix500`.
- Add provenance to resolver output.
- Add strict errors for unsupported exact membership.
- Replace direct `stocks.scale_category` / latest `stocks` usage in analytics paths.

### Phase 4: Backtest family migration

- Move backtest / attribution / optimize / lab from dataset snapshot SoT to market reader + resolver.
- Preserve strategy YAML `universe preset` semantics.
- Remove normal-run dependency on physical dataset names.
- Update tests and strategy validation.

### Phase 5: Screening / Ranking / Symbol Workbench migration

- Screening: resolve selected strategy universe as of screening date.
- Ranking: resolve daily ranking universe as of ranking date.
- Symbol Workbench: use `stock_master_daily` / `stocks_latest` explicitly depending on historical vs latest UI.
- Surface provenance and coverage warnings in API payloads.

### Phase 6: Dataset demotion

- Rename UI/API language from dataset selection to universe preset where applicable.
- Keep dataset builder only for export/repro fixtures if still needed.
- Mark old dataset bundles unsupported for normal backtest/research execution.

### Phase 7: Research invalidation and rerun

- Mark contaminated current-membership research as invalidated.
- Rerun TOPIX100 / TOPIX500 / PrimeExTopix500 studies with resolver-backed PIT universes.
- Update Published Readouts with new PIT-safe results.

## Acceptance Criteria

- A fresh `initial sync` can build schema v3 from an empty market DB.
- Validation fails or warns if `stock_master_daily` coverage is missing for any TOPIX trading date in range.
- No research/backtest/screening/ranking path uses latest `stocks` or `stocks_latest` for historical universe membership.
- TOPIX100 universe on historical dates is derived from `stock_master_daily` for that date.
- TOPIX500-dependent presets reject exact mode until `index_membership_daily` is available.
- Existing local old `market.duckdb` is rejected with clear reset guidance.
- Dataset snapshot is no longer the normal SoT for backtest / optimize / lab.
- API/UI provenance shows the market DB schema version and universe as-of source.

## Explicit Non-Goals

- Do not migrate old `market.duckdb` in place.
- Do not preserve compatibility for latest-universe backtests as a default mode.
- Do not silently approximate TOPIX500 with latest membership.
- Do not move portfolio/watchlist/jobs metadata into `market.duckdb`.
- Do not move strategy YAML or result artifacts into `market.duckdb`.

## Open Questions

- Which J-Quants endpoint or external source should be the exact SoT for TOPIX500 membership?
- Should `stock_master_daily` store every field from `/equities/master` raw JSON in an audit column?
- Should historical master backfill use every TOPIX trading date or every calendar business date where the endpoint has data?
- What is the acceptable initial sync runtime and request budget for 10 years of daily master snapshots?
