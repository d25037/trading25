# Trading25 TypeScript Workspace

`apps/ts` は trading25 のフロントエンド/CLI/共有ライブラリを管理する Bun workspace です。
バックエンド実行ロジックは `apps/bt` の FastAPI (`http://localhost:3002`) に統一されています。

## Package Layout

| Package | Role | Status |
|---|---|---|
| `packages/web` | React 19 + Vite フロントエンド | Active |
| `packages/cli` | Gunshi CLI（dataset/portfolio/analysis/backtest） | Active |
| `packages/shared` | 共有ロジック、DBアクセス、型公開、`bt:sync` | Active |
| `packages/clients-ts` | FastAPI クライアント（backtest/JQuants） | Active |

旧 `packages/api`（Hono）は撤去済みです。

## Quick Start

### 1) バックエンドを起動（apps/bt）
```bash
cd apps/bt
uv run bt server --port 3002
```

### 2) この workspace を起動
```bash
cd apps/ts
bun install
bun run dev
```

`bun run dev:full` は起動前に `bt:sync` を実行し、まず `apps/bt` ソースから OpenAPI を直接生成して型を再生成します（失敗時は `http://localhost:3002/openapi.json` 取得にフォールバック）。

## Common Commands

```bash
# Development
bun run dev           # web 起動 (Vite :5173, /api -> :3002 proxy)
bun run dev:web
bun run dev:cli
bun run dev:full      # bt:sync + dev:web

# Quality
bun run lint
bun run check:fix
bun run typecheck:all

# Tests
bun run test
bun run test:packages
bun run test:apps
bun run test:coverage
bun run --filter @trading25/web e2e:smoke

# Build
bun run build
```

`bun run dev:api` は FastAPI 起動コマンドへの案内表示のみで、API サーバー起動には使用しません。

## API / Type Sync

- Web は `/api` を `http://localhost:3002` にプロキシ
- CLI の既定 API URL は `http://localhost:3002`（`API_BASE_URL` で上書き可）
- API ドキュメントは FastAPI 側 `http://localhost:3002/doc`
- `bt:sync` はサーバー起動不要（`apps/bt` ソースから OpenAPI 生成）。生成不能時のみ FastAPI 取得にフォールバック
- スキーマ変更時は以下を実行

```bash
bun run --filter @trading25/shared bt:sync
```

## CLI Examples

```bash
# DB
bun cli db sync
bun cli db validate
bun cli db refresh

# Dataset
bun cli dataset create prime.db --preset primeMarket
bun cli dataset info prime.db
bun cli dataset validate prime.db

# Analytics
bun cli analysis roe 7203
bun cli analysis ranking --limit 20
bun cli analysis screening
bun cli analysis factor-regression 7203
bun cli analysis portfolio-factor-regression 1

# Portfolio / Watchlist
bun cli portfolio create "My Portfolio"
bun cli watchlist create "Tech Stocks"
```

## Environment Variables

```bash
JQUANTS_API_KEY
JQUANTS_PLAN
API_BASE_URL         # default: http://localhost:3002
BT_API_URL           # backtest client base URL (default: http://localhost:3002)
LOG_LEVEL
NODE_ENV
```

## XDG Data Paths

- `$HOME/.local/share/trading25/market.db`
- `$HOME/.local/share/trading25/datasets/`
- `$HOME/.local/share/trading25/portfolio.db`

詳細ガイドは `AGENTS.md` を参照してください。
