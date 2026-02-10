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
- `strategy-config-v1.schema.json`

## Change Process

1. PR で契約変更を提案
2. 互換性（additive / breaking）をレビュー
3. 必要に応じて型再生成:

```bash
cd apps/ts
bun run --filter @trading25/shared bt:sync
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
- 参照: `docs/reports/dependency-audit-phase1b.md`

## Files

| File | Status | Description |
|---|---|---|
| `dataset-schema.json` | Deprecated | 旧 dataset schema（参照用） |
| `dataset-db-schema-v2.json` | Active | dataset.db 契約 |
| `market-db-schema-v1.json` | Active | market.db 契約 |
| `portfolio-db-schema-v1.json` | Active | portfolio.db 契約 |
| `strategy-config-v1.schema.json` | Active | strategy YAML 契約 |
| `backtest-run-manifest-v1.schema.json` | Active | backtest manifest 契約 |
| `hono-openapi-baseline.json` | Archived | Hono -> FastAPI 移行ベースライン |

## OpenAPI Snapshot

- `apps/ts/packages/shared/openapi/bt-openapi.json`  
  `bt:sync` で更新される FastAPI スナップショット。

## Dataset Note

`dataset-db-schema-v2.json` が現行の authoritative contract です。  
新規実装は v2 基準で整合を取ってください。
