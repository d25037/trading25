---
description: Trading25 TypeScript monorepo with frontend, shared library, and CLI
globs: "*.ts, *.tsx, *.html, *.css, *.js, *.jsx, package.json, biome.json"
alwaysApply: false
---

Trading25 TypeScript monorepo for financial data analysis with strict TypeScript/Biome compliance.

## Architecture

- **`packages/web/`** - React 19 + Vite + Tailwind CSS v4 → [Web AGENTS.md](./packages/web/AGENTS.md)
- **`packages/shared/`** - JQuants API clients, shared API types, TA/FA utilities → [Shared AGENTS.md](./packages/shared/AGENTS.md)
- **`packages/cli/`** - Gunshi CLI for dataset/portfolio/analysis → [CLI AGENTS.md](./packages/cli/AGENTS.md)
- **`packages/api-clients/`** - FastAPI client packages (backtest/JQuants)

## Critical Rules

**NEVER relax TypeScript or Biome rules** - Fix code to comply with strict standards.

**NEVER unify logger implementations** - Frontend (browser) and Node.js require separate loggers.

### Type-Safe Patterns

```typescript
// Use type guards instead of non-null assertions
import { getFirstElementOrFail } from '@trading25/shared/test-utils';
const first = getFirstElementOrFail(array, 'Expected element');
```

### API Response Type Pattern

API レスポンス型は `@trading25/shared/types/api-response-types` で一元管理。Web/CLI の型ファイルは re-export パターンで参照する（`backtest.ts` が模範）。型の重複定義は禁止。

```typescript
// web/src/types/ranking.ts - re-export pattern
export type { RankingItem, Rankings } from '@trading25/shared/types/api-response-types';
// Frontend-specific types remain local
export interface RankingParams { ... }
```

## TypeScript Configuration

Root `tsconfig.json` は shared / cli を対象にし、web は別設定で型検査する。
- **Root**: packages/shared, packages/cli
- **Web**: JSX + DOM APIs (React 19)
- **API**: Node.js-specific (ESNext)

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

# Database & Dataset (requires API server for dataset commands)
bun run cli:run db sync             # Market data sync
bun run cli:run db validate         # Validate integrity
bun run cli:run dataset create prime.db --preset primeMarket

# Analysis
bun run cli:run analysis roe 7203
bun run cli:run analysis ranking --limit 20
bun run cli:run analysis screening
bun run cli:run analysis factor-regression 7203
bun run cli:run analysis portfolio-factor-regression 1

# Portfolio
bun run cli:run portfolio create "My Portfolio"
bun run cli:run portfolio add-stock "My Portfolio" 7203 --quantity 100 --price 2500

# Watchlist
bun run cli:run watchlist create "Tech Stocks"
bun run cli:run watchlist add-stock "Tech Stocks" 7203
bun run cli:run watchlist show "Tech Stocks"
bun run cli:run watchlist list

# Backtest (requires trading25-bt backend on port 3002)
bun run cli:run backtest run <strategy.yaml>
bun run cli:run backtest cancel <job-id>
bun run cli:run backtest list
bun run cli:run backtest results <job-id>

# bt contract sync (serverless local generation first; HTTP fetch is fallback)
bun run --filter @trading25/shared bt:sync  # Generate schema + generate types
```

`bun run api:hint` は FastAPI 起動コマンドへの案内表示であり、API サーバーの起動コマンドではない。

## CI

`.github/workflows/ci.yml` により全ブランチ push および PR で自動実行される。

**ランタイム**: ubuntu-latest, Bun 1.3.8

**ステップ**:
1. Lint (`bun run quality:lint`)
2. bt OpenAPI 型生成 (`cd packages/shared && bun run bt:generate-types`)
3. Build shared package (`bun run --filter @trading25/shared build`)
4. Typecheck (`bun run quality:typecheck`)
5. Test with coverage (`bun run workspace:test:coverage`)
6. Coverage threshold 検証 (`bun run coverage:check`)

## Technology Stack

- **Core**: TypeScript + Bun workspaces + Biome 2.1.4
- **Testing**: Bun (backend) + Vitest (web)
- **Web**: React 19 + Vite 7 + Tailwind CSS v4 + TanStack Query + Zustand
- **API**: FastAPI (`apps/bt`, port `3002`) + OpenAPI
- **Data**: FastAPI (:3002) + OpenAPI generated types + JQuants API
- **CLI**: Gunshi + Chalk + Ora

## Environment Variables

`.env` のSoTはリポジトリルート（`<repo-root>/.env`）。

```
JQUANTS_API_KEY         # JQuants API key (v2 API)
JQUANTS_PLAN            # Required: free, light, standard, premium (rate limit)
LOG_LEVEL               # debug, info, warn, error
NODE_ENV                # development, production
```

## XDG-Compliant Paths

- **Market DB**: `$HOME/.local/share/trading25/market.db`
- **Datasets**: `$HOME/.local/share/trading25/datasets/`
- **Portfolio**: `$HOME/.local/share/trading25/portfolio.db`

Customize with `XDG_DATA_HOME` environment variable.

## Skills

Project-specific skills are defined in `../../.codex/skills/*/SKILL.md`. Refer to these for domain-specific guidance.

### API エンドポイント参照

APIエンドポイントの確認・デバッグ時は **`api-endpoints` skill** を使用すること。`curl` でAPIを叩く際のパス確認に必須。Swagger UI (`http://localhost:3002/doc`) も利用可能。

### User-Level Skills

ユーザーレベルのスキル（`~/.codex/skills/`）も利用可能:

- **`local-issues`** — ファイルベースのIssue管理。`issues/` ディレクトリ内のMarkdownファイルでIssueを管理する。操作: open, list, show, edit, start, close, wontfix, deps, summary。Issueの作成・更新・クローズ時はこのスキルのフォーマットに従うこと。
- **`ask-codex`** — Plan mode でプラン確定前に Codex CLI へ評価を依頼する。

## CLI Development

When creating command-line interfaces, use the `use-gunshi-cli` skill.
