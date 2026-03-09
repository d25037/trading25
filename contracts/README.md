# Contracts

`contracts/` は `apps/bt` と `apps/ts` の安定インターフェースを管理する SoT です。

## Scope

- DB schema contract（market / dataset / portfolio）
- strategy / manifest schema
- Hono 時代の OpenAPI baseline（移行履歴として保持）

## Versioning Rules

| Change Type | Impact | Rule |
|---|---|---|
| Additive | Non-breaking | 既存バージョンを維持して拡張 |
| Breaking | Breaking | 新しい versioned file を作成 |

命名規則: `{domain}-{purpose}-v{N}.schema.json`

例:
- `dataset-db-schema-v2.json`
- `market-db-schema-v1.json`
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
| `dataset-db-schema-v2.json` | **Active** | Dataset DB schema contract aligned with `apps/ts` Drizzle tables (395 lines). Use this for all new development. |
| `market-db-schema-v2.json` | **Active** | Market DB schema contract with `statements` and `margin_data` tables for DuckDB sync/screening (v2 minor update). |
| `backtest-run-manifest-v1.schema.json` | **Active** | Backtest run manifest emitted by `apps/bt`. |
| `strategy-config-v1.schema.json` | **Deprecated** | Legacy strategy YAML schema before `baseline_*` signal split. |
| `strategy-config-v2.schema.json` | **Deprecated** | Strategy YAML schema after `baseline_*` split and before the 2026-03 signal taxonomy cleanup. |
| `strategy-config-v3.schema.json` | **Active** | Current strategy YAML schema aligned with `period_extrema_*`, `atr_support_*`, `retracement_*`, `bollinger_*`, and `volume_ratio_*`. |
| `fundamentals-metrics-v1.schema.json` | **Deprecated** | Legacy fundamentals API response contract（`bookToMarket` を含む旧版）。 |
| `fundamentals-metrics-v2.schema.json` | **Active** | Fundamentals API response contract (`/api/analytics/fundamentals/{symbol}`), including `cfoToNetProfitRatio`, `tradingValueToMarketCapRatio`, and `tradingValuePeriod`. |
| `portfolio-db-schema-v1.json` | **Deprecated** | Legacy portfolio DB schema contract (without jobs table). |
| `portfolio-db-schema-v2.json` | **Active** | Portfolio DB schema contract (portfolios/watchlists + jobs metadata table). |
| `hono-openapi-baseline.json` | **Archived** | Hono OpenAPI snapshot used as Phase 3 migration baseline. Phase 3F (2026-02-07) で全 90 EP 移行完了・Hono 廃止済み。参照用に保持。 |

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

`dataset-db-schema-v2.json` が現行の authoritative contract です。  
新規実装は v2 基準で整合を取ってください。
