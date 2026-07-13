---
description: Trading25 TypeScript monorepo with frontend and split contracts/utils/api-clients libraries
globs: "*.ts, *.tsx, *.html, *.css, *.js, *.jsx, package.json, biome.json"
alwaysApply: false
---

Trading25 TypeScript monorepo for financial data analysis with strict TypeScript/Biome compliance.

## Architecture

- **`packages/web/`** - React 19 + Vite + Tailwind CSS v4 → [Web AGENTS.md](./packages/web/AGENTS.md)
- **`packages/contracts/`** - OpenAPI generated 型、API response 型、`bt:sync`
- **`packages/utils/`** - logger/env/date/path など共通ユーティリティ
- **`packages/api-clients/`** - shared FastAPI clients (backtest / analytics) and HTTP utilities

## Critical Rules

**NEVER relax TypeScript or Biome rules** - Fix code to comply with strict standards.

**NEVER unify logger implementations** - Frontend (browser) and Node.js require separate loggers.

### Type-Safe Patterns

```typescript
// Use type guards instead of non-null assertions
import { getFirstElementOrFail } from '@trading25/utils/test-utils';
const first = getFirstElementOrFail(array, 'Expected element');
```

### API Response Type Pattern

API レスポンス型は `@trading25/contracts/types/api-response-types` で一元管理。web と shared api-clients の型ファイルは re-export パターンで参照する（`backtest.ts` が模範）。型の重複定義は禁止。

```typescript
// web/src/types/ranking.ts - re-export pattern
export type { RankingItem, Rankings } from '@trading25/contracts/types/api-response-types';
// Frontend-specific types remain local
export interface RankingParams { ... }
```

## TypeScript Configuration

Root `tsconfig.json` は contracts/utils を対象にし、web は別設定で型検査する。
- **Root**: packages/contracts + packages/utils
- **Web**: JSX + DOM APIs (React 19)
- **API clients**: shared HTTP clients are checked from `packages/api-clients`

## Essential Commands

```bash
# Development
bun run workspace:dev       # Web only (FastAPI :3002 にプロキシ)
bun run workspace:dev:sync  # bt:sync + web:dev (sync失敗時はwarningで継続)
bun run web:dev             # Vite (port 5173)

# Build & Test
bun run workspace:build             # All packages
bun run workspace:test              # All tests
bun run quality:typecheck           # TypeScript checking
bun run quality:lint && bun run quality:check:fix   # Code quality

# bt contract sync (serverless local generation first; HTTP fetch is fallback)
bun run --filter @trading25/contracts bt:sync  # Generate schema + generate types

# Headless operations are handled by bt CLI
uv run --project ../bt bt --help
```

`bun run api:hint` は FastAPI 起動コマンドへの案内表示であり、API サーバーの起動コマンドではない。

## CI

`.github/workflows/ci.yml` は `main` への push、pull request、`workflow_dispatch` で実行される。

**ランタイム**: ubuntu-latest, Bun 1.3.8

**主要ジョブ**:
- `repo-guardrails`: `scripts/skills/audit_skills.py --strict-legacy` と privacy leak check
- `quality`: `./scripts/lint.sh` と `./scripts/typecheck.sh`
- `contract-tests`: `./scripts/check-contract-sync.sh`
- `package-unit-tests` / `app-integration-tests` / `coverage-gate`: package/web/bt tests と coverage threshold
- `web-e2e`: Playwright smoke（必要な scope のとき）

## Technology Stack

- **Core**: TypeScript + Bun workspaces + Biome 2.4.15
- **Testing**: Bun (backend) + Vitest (web)
- **Web**: React 19 + Vite 8 + Tailwind CSS v4 + TanStack Query + Zustand
- **API**: FastAPI (`apps/bt`, port `3002`) + OpenAPI
- **Data**: FastAPI (:3002) + OpenAPI generated types + JQuants API

## Environment Variables

環境変数の SoT はプロセス環境。非機密設定は `~/.config/trading25/config.env` を shell source し、J-Quants API key は `scripts/dev-bt-server.sh` が `JQUANTS_API_KEY` 環境変数 override または macOS Keychain から bt FastAPI process へ注入する。repo root `.env` は使用しない。

```
JQUANTS_API_KEY         # JQuants API key (v2 API)
JQUANTS_PLAN            # Required: free, light, standard, premium (rate limit)
LOG_LEVEL               # debug, info, warn, error
NODE_ENV                # development, production
```

## XDG-Compliant Paths

- **Market DB (DuckDB)**: `$HOME/.local/share/trading25/market-timeseries/market.duckdb`
- **Datasets**: `$HOME/.local/share/trading25/datasets/`
- **Portfolio**: `$HOME/.local/share/trading25/portfolio.db`

Customize with `XDG_DATA_HOME` environment variable.

## Skills

Project-specific skills are defined in `../../.codex/skills/*/SKILL.md`. Refer to these for domain-specific guidance.

### API エンドポイント参照

APIエンドポイントの確認・デバッグ時は正式名の **`ts-api-endpoints` skill** を使用すること。shorthand として **`api-endpoints`** も許可する。`curl` でAPIを叩く際のパス確認に必須。Swagger UI (`http://localhost:3002/doc`) も利用可能。

### Process Skills

Repository-local process/domain skill は現在の Codex skill catalog に公開されたものを使い、`~/.agents/skills` の存在を仮定しない。ts 作業では root `AGENTS.md` と project-specific skills を優先する。
