# Trading25 TypeScript Workspace

`apps/ts` は trading25 のフロントエンド/共有ライブラリを管理する Bun workspace です。
バックエンド実行ロジックは `apps/bt` の FastAPI (`http://localhost:3002`) に統一されています。

## Package Layout

| Package | Role | Status |
|---|---|---|
| `packages/web` | React 19 + Vite フロントエンド | Active |
| `packages/contracts` | OpenAPI generated 型・API response 型・`bt:sync` | Active |
| `packages/utils` | logger/env/date/path 等の共通ユーティリティ | Active |
| `packages/api-clients` | FastAPI クライアント（backtest/JQuants） | Active |

旧 `packages/api`（Hono）は撤去済みです。

## Quick Start

### 1) バックエンドを起動（apps/bt）
```bash
cd apps/bt
uv run bt server --port 3002
```

### 2) この workspace を起動
```bash
cd <repo-root>
cp .env.example .env
cd apps/ts
bun install
bun run workspace:dev
```

`bun run workspace:dev:sync` は起動前に `bt:sync` を実行し、まず `apps/bt` ソースから OpenAPI を直接生成して型を再生成します（失敗時は warning を出して `web:dev` を継続実行）。
`main` ブランチでは `workspace:dev` を既定にし、`workspace:dev:sync` は契約更新確認が必要な時だけ使う運用を推奨します。

通常の build/test/lint/typecheck script は `package.json` から各 workspace command を直接呼びます。
`scripts/tasks.ts` は `.env` 注入が必要な `web:dev`、optional sync を含む `workspace:dev:sync`、および `workspace:clean` のような独自 orchestration に限定しています。

## Common Commands

```bash
# Development
bun run workspace:dev        # web 起動 (Vite :5173, /api -> :3002 proxy)
bun run web:dev
bun run workspace:dev:sync   # bt:sync + web:dev

# Quality
bun run quality:lint
bun run quality:check:fix
bun run quality:typecheck
bun run quality:deps:audit

# Tests
bun run workspace:test
bun run packages:test
bun run apps:test
bun run workspace:test:coverage
bun run --filter @trading25/web e2e:smoke

# Build
bun run workspace:build
```

`bun run api:hint` は FastAPI 起動コマンドへの案内表示のみで、API サーバー起動には使用しません。

## Dependency Policy

`apps/ts` の依存は「削ること」より「責務が明確で drift しないこと」を優先します。宣言の棚卸しは `bun run quality:deps:audit` で行い、未使用依存、import と manifest の不整合、root override と package 宣言の version drift を検査します。

- `runtime keep`: `react`, `react-dom`, `@tanstack/react-query`, `@tanstack/react-router`, `zustand`, `lucide-react`, `lightweight-charts`, `@monaco-editor/react`, `monaco-editor`
- `tooling keep`: `vite`, `vitest`, `@playwright/test`, `@biomejs/biome`, `typescript`, `bun-types`
- `transitive pin`: root `overrides` の `@redocly/openapi-core`, `rollup`, `minimatch`, `flatted`, `monaco-editor`
- `remove`: `@radix-ui/react-label`, `tsx`

`zustand` は完全撤去ではなく縮小方針です。URL と相性の良い page selection state は TanStack Router search params を SoT にし、`zustand` は chart preset / panel visibility / active job tracking のような session-local state に限定します。

## API / Type Sync

- Web は `/api` を `BT_API_URL` にプロキシ（default: `http://localhost:3002`）
- API ドキュメントは FastAPI 側 `http://localhost:3002/doc`
- `bt:sync` はサーバー起動不要（`apps/bt` ソースから OpenAPI 生成）。生成不能時のみ FastAPI 取得にフォールバック
- スキーマ変更時は以下を実行

```bash
bun run --filter @trading25/contracts bt:sync
```

## CLI Usage

運用用CLIは `apps/bt` の `bt` コマンドに統合済みです。
`apps/ts` からの headless 運用は廃止しました。

## Environment Variables

環境変数の SoT はリポジトリルートの `.env`（`<repo-root>/.env`）。

```bash
JQUANTS_API_KEY
JQUANTS_PLAN
BT_API_URL           # bt FastAPI URL for TS clients, OpenAPI fallback, and Vite proxy (default: http://localhost:3002)
LOG_LEVEL
NODE_ENV
```

## XDG Data Paths

- `$HOME/.local/share/trading25/market-timeseries/market.duckdb`
- `$HOME/.local/share/trading25/datasets/`
- `$HOME/.local/share/trading25/portfolio.db`

詳細ガイドは `AGENTS.md` を参照してください。
