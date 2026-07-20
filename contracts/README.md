# Contracts

`contracts/` は `apps/bt` と `apps/ts` の安定インターフェースを管理する SoT です。

## Scope

- DB schema contract（market / dataset / portfolio）
- strategy / manifest schema
- FastAPI OpenAPI snapshot（bt / ts generated types の SoT）

## Versioning Rules

| Change Type | Impact | Rule |
|---|---|---|
| Additive | Non-breaking | 既存バージョンを維持して拡張 |
| Breaking | Breaking | 新しい versioned file を作成 |

命名規則: `{domain}-{purpose}-v{N}.schema.json`

例:
- `dataset-db-schema-v3.json`
- `dataset-db-schema-v2.json`
- `market-db-schema-v1.json`
- `market-db-schema-v4.json`
- `portfolio-db-schema-v1.json`
- `strategy-config-v3.schema.json`

## Change Process

1. PR で契約変更を提案
2. 互換性（additive / breaking）をレビュー
3. 必要に応じて型再生成:

```bash
cd apps/ts
bun run --filter @trading25/contracts bt:sync
```

4. lint / typecheck / tests を通す
5. CI で検証

## Dependency Direction

| Direction | Status | Notes |
|---|---|---|
| bt -> ts | Removed | bt は FastAPI 単独で動作 |
| ts -> bt | Minimal | backtest client 経由の API 利用のみ |

- 検査: `scripts/check-dep-direction.sh`
- allowlist: `scripts/dep-direction-allowlist.txt`
- 参照: `docs/archive/reports/dependency-audit-phase1b.md`

## Files

| File | Status | Description |
|---|---|---|
| `dataset-schema.json` | **Deprecated** | Minimal dataset snapshot schema (legacy v1). Do not use for new work. |
| `dataset-snapshot-manifest-v3.schema.json` | **Active** | Strict Market v4 event-time PIT manifest payload contract for `dataset.duckdb + parquet + manifest.v2.json`. |
| `dataset-snapshot-manifest-v2.schema.json` | **Superseded** | Historical schemaVersion 2 payload contract. Retained for reference and rejected by runtime. |
| `dataset-snapshot-manifest-v1.schema.json` | **Historical** | Legacy manifest contract used during the dataset.db compatibility transition. Unsupported for new runtime paths. |
| `dataset-db-schema-v3.json` | **Active** | Breaking DuckDB dataset contract carrying forward the supported Dataset tables and adding Market v4 raw prices, exact daily master, retained bases/segments, adjusted metrics, and valuation. |
| `dataset-db-schema-v2.json` | **Superseded** | Superseded by `dataset-db-schema-v3.json`; retained for historical reference only and unsupported for new snapshots. |
| `market-db-schema-v4.json` | **Active** | Fresh-only breaking contract for physical Market Data Plane schema v5 and `provider_adjusted_v1`; provider raw/adjusted prices, a bounded adjustment-event ledger, current-basis statement metrics, and the ASOF valuation view are canonical. |
| `market-db-schema-v3.json` | **Superseded** | Historical physical Market v4 contract with retained event-time adjustment bases. Market v5 rejects it; there is no in-place migration or dual read. |
| `market-db-schema-v2.json` | **Superseded** | Superseded by `market-db-schema-v3.json`. It is not runtime-compatible with physical schema v4 and is retained for historical reference only. |
| `backtest-run-manifest-v1.schema.json` | **Active** | Backtest run manifest emitted by `apps/bt`. |
| `strategy-config-v1.schema.json` | **Deprecated** | Legacy strategy YAML schema before `baseline_*` signal split. |
| `strategy-config-v2.schema.json` | **Deprecated** | Strategy YAML schema after `baseline_*` split and before the 2026-03 signal taxonomy cleanup. |
| `strategy-config-v3.schema.json` | **Active** | Current strategy YAML schema aligned with `period_extrema_*`, `atr_support_*`, `retracement_*`, `bollinger_*`, and `volume_ratio_*`. |
| `fundamentals-metrics-v1.schema.json` | **Deprecated** | Legacy fundamentals API response contract（`bookToMarket` を含む旧版）。 |
| `fundamentals-metrics-v2.schema.json` | **Active** | Fundamentals API response contract (`/api/analytics/fundamentals/{symbol}`), including `cfoToNetProfitRatio`, `tradingValueToMarketCapRatio`, and `tradingValuePeriod`. |
| `portfolio-db-schema-v1.json` | **Deprecated** | Legacy portfolio DB schema contract (without jobs table). |
| `portfolio-db-schema-v2.json` | **Active** | Portfolio DB schema contract (portfolios/watchlists + jobs metadata table). |

## OpenAPI Snapshot

- `apps/ts/packages/contracts/openapi/bt-openapi.json`  
  `bt:sync` で更新される FastAPI 契約スナップショット（`apps/bt` ソースからの直接生成を優先し、失敗時のみ `/openapi.json` 取得にフォールバック）。

## Run Registry Compatibility

- `RunSpec` / `RunMetadata` / `CanonicalExecutionResult` / `ArtifactIndex` は `schema_version=1` を current とする。
- `schema_version=1` の間は additive 変更のみ許可する。
- additive とみなすもの:
  - optional field の追加
  - enum value の追加
  - artifact metadata key の追加
- breaking とみなすもの:
  - field rename / delete
  - required 化
  - 既存 field の意味変更
  - artifact kind の再定義
- breaking 変更は新しい major schema version を追加し、旧 version reader は compatibility path として残す。
- `jobs` table の legacy row は `job_manager` が default `RunSpec` / `RunMetadata` を補完して読み出す。artifact / canonical result reader は `artifact_index -> canonical_result -> legacy columns` の順で解決する。

## Dataset Note

dataset runtime の SoT は `dataset.duckdb + parquet + manifest.v2.json` のみです。  
DB schema は breaking `dataset-db-schema-v3.json`、physical manifest filename は引き続き
`manifest.v2.json` です。payload contract は `dataset-snapshot-manifest-v3.schema.json`
（`schemaVersion: 3`）で、manifest は必須です。物理ファイル名と payload version は
意図的に独立しています。

`dataset.db` と `dataset-db-schema-v2.json` は superseded historical reference 扱いです。
新規実装・runtime 解決・backtest 実行では使用しません。

- runtime は schemaVersion 2、Market v3 以前、lineage metadata 欠落 bundle を受理しない。
- manifest reader は `duckdbSha256` / `parquet.*` に加えて、DuckDB inspection から導いた `logicalCounts` / `coverage` / `dateRange` / `logicalSha256` と event-time basis integrity を検証する。
- `checksums.parquet` は Dataset v3 writer が出力する12個の canonical Parquet filename を全て、かつそれだけを含む。alias、絶対パス、path component、余剰・欠落 key は unsupported とする。
