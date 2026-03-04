---
description: Trading25 TypeScript monorepo with frontend and split contracts/domain/utils libraries
globs: "*.ts, *.tsx, *.html, *.css, *.js, *.jsx, package.json, biome.json"
alwaysApply: false
---

Trading25 TypeScript monorepo for financial data analysis with strict TypeScript/Biome compliance.

## Architecture

- **`packages/web/`** - React 19 + Vite + Tailwind CSS v4 → [Web AGENTS.md](./packages/web/AGENTS.md)
- **`packages/contracts/`** - OpenAPI generated 型、API response 型、`bt:sync`
- **`packages/domain/`** - dataset/portfolio/watchlist/portfolio-performance 等のドメイン実装
- **`packages/utils/`** - logger/env/date/path など共通ユーティリティ
- **`packages/api-clients/`** - FastAPI client packages (backtest/JQuants)

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

API レスポンス型は `@trading25/contracts/types/api-response-types` で一元管理。Web/CLI の型ファイルは re-export パターンで参照する（`backtest.ts` が模範）。型の重複定義は禁止。

```typescript
// web/src/types/ranking.ts - re-export pattern
export type { RankingItem, Rankings } from '@trading25/contracts/types/api-response-types';
// Frontend-specific types remain local
export interface RankingParams { ... }
```

## TypeScript Configuration

Root `tsconfig.json` は contracts/domain/utils を対象にし、web は別設定で型検査する。
- **Root**: packages/contracts + packages/domain + packages/utils
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

# bt contract sync (serverless local generation first; HTTP fetch is fallback)
bun run --filter @trading25/contracts bt:sync  # Generate schema + generate types

# Headless operations are handled by bt CLI
uv run --project ../bt bt --help
```

`bun run api:hint` は FastAPI 起動コマンドへの案内表示であり、API サーバーの起動コマンドではない。

## CI

`.github/workflows/ci.yml` により全ブランチ push および PR で自動実行される。

**ランタイム**: ubuntu-latest, Bun 1.3.8

**ステップ**:
1. Lint (`bun run quality:lint`)
2. bt OpenAPI 型生成 (`cd packages/contracts && bun run bt:generate-types`)
3. Build contracts/domain/utils package (`bun run workspace:build`)
4. Typecheck (`bun run quality:typecheck`)
5. Test with coverage (`bun run workspace:test:coverage`)
6. Coverage threshold 検証 (`bun run coverage:check`)

## Technology Stack

- **Core**: TypeScript + Bun workspaces + Biome 2.1.4
- **Testing**: Bun (backend) + Vitest (web)
- **Web**: React 19 + Vite 7 + Tailwind CSS v4 + TanStack Query + Zustand
- **API**: FastAPI (`apps/bt`, port `3002`) + OpenAPI
- **Data**: FastAPI (:3002) + OpenAPI generated types + JQuants API

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
