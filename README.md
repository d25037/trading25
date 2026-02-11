# trading25 Monorepo

FastAPI バックエンド（`apps/bt`）と TypeScript クライアント（`apps/ts`）を統合したモノレポです。

## Current Architecture

```
JQUANTS API ──→ FastAPI (:3002) ──→ SQLite (market.db / portfolio.db / datasets)
                     ↓
                  ts/web (:5173, /api proxy)
                  ts/cli
```

- バックエンドは `apps/bt` の FastAPI に一本化済み
- financial-analysis のロジック SoT は `apps/bt`（ts 側は API consumer / proxy）
- `apps/ts/packages/api` は互換 API レイヤーとして bt API をプロキシ

## Repository Layout

- `apps/bt` - Python 3.12 + FastAPI + vectorbt + typer CLI
- `apps/ts` - Bun workspace（web / cli / shared / clients-ts / compatibility api）
- `contracts` - bt/ts 間の安定インターフェース（JSON Schema, OpenAPI baseline）
- `docs` - ロードマップ、設計判断、監査レポート
- `issues` - ローカル Issue 管理（`issues/` と `issues/done/`）
- `scripts` - ルート統合スクリプト

## Quick Start

### 1) FastAPI 起動（apps/bt）
```bash
cd apps/bt
uv sync
uv run bt server --port 3002
```

### 2) Web 起動（apps/ts）
```bash
cd apps/ts
bun install
bun run dev
```

`bun run dev:full` を使うと、起動前に `bt:sync`（OpenAPI 取得と型生成）を実行します。

## Monorepo Commands (root)

```bash
./scripts/lint.sh         # dep-direction + apps/ts lint + apps/bt ruff
./scripts/typecheck.sh    # apps/ts typecheck + apps/bt pyright
./scripts/test-packages.sh # package unit tests (ts packages + bt unit)
./scripts/test-apps.sh    # app integration tests (ts apps + bt api/integration)
./scripts/test.sh         # test-packages + test-apps
```

## OpenAPI Contract Sync

FastAPI スキーマ更新後は `apps/ts` で次を実行:

```bash
bun run --filter @trading25/shared bt:sync
```

## CI

`.github/workflows/ci.yml` で以下を実行:

1. Codex skills audit（`scripts/skills/audit_skills.py --strict-legacy`）
2. Lint
3. Typecheck
4. Package unit tests
5. App integration tests
