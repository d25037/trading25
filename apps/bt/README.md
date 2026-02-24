# trading25-bt

`apps/bt` は trading25 の単一バックエンドです。  
FastAPI サーバー（`:3002`）と Python CLI（`bt`）を提供します。  
ユーザー向け一次CLIは `apps/ts/packages/cli` で、`apps/bt` のCLIは実行基盤・運用向けユーティリティとして位置付けます。

## Responsibilities

- FastAPI API（OpenAPI 公開、統一エラーレスポンス、correlation ID）
- SQLite 管理（`market.db` / `portfolio.db` / `datasets`）
- vectorbt ベースのバックテストと最適化
- strategy config / signal / lab の実行基盤

## Quick Start

```bash
# Install dependencies
uv sync

# Start FastAPI server
uv run bt server --port 3002

# CLI help
uv run bt --help
```

## Common Commands

```bash
# Backtest CLI
uv run bt list
uv run bt backtest <strategy_name>
uv run bt backtest <strategy_name> --optimize
uv run bt validate <strategy_name>
uv run bt cleanup --days 7

# Quality
uv run ruff check src tests
uv run pyright src
uv run pytest tests
```

## API

- Base URL: `http://localhost:3002`
- OpenAPI: `GET /openapi.json`
- Swagger UI: `http://localhost:3002/doc`
- `/docs` と `/redoc` は無効化

主要カテゴリ:
- `/api/jquants/*`
- `/api/chart/*`
- `/api/analytics/*`
- `/api/db/*`
- `/api/dataset*`
- `/api/portfolio*`
- `/api/watchlist*`
- `/api/backtest*`, `/api/optimize*`, `/api/lab*`, `/api/indicators*`

## Data Paths (XDG)

共有データルート: `~/.local/share/trading25/`

- `market.db`
- `portfolio.db`
- `datasets/`
- `strategies/experimental/`
- `strategies/production/`
- `strategies/legacy/`
- `backtest/results/`

共有テンプレート等の最小セットのみ `apps/bt/config/strategies/reference/` に保持します。

## Integration with apps/ts

- ユーザー向け一次CLIは `apps/ts/packages/cli`（Gunshi CLI）
- `apps/bt` の `bt` CLI は実行基盤・運用向け
- ポートフォリオ操作は `apps/ts/packages/cli`（`bun cli portfolio ...`）を使用
- `apps/ts/packages/web` は `/api` を FastAPI (`:3002`) にプロキシ
- `apps/ts/packages/cli` は FastAPI API を直接呼び出し
- OpenAPI 変更時は `apps/ts` で `bun run --filter @trading25/shared bt:sync` を実行

## Tech Stack

- Python 3.12
- FastAPI / uvicorn
- SQLAlchemy Core
- vectorbt / pandas / numpy / scipy
- pydantic / typer / loguru
- pytest / ruff / pyright

## Documentation

- `AGENTS.md` - 運用ルールと責務
- `../../docs/bt-src-layering-guide.md` - `src` 5層構成の配置ルール
- `docs/` - 戦略・コマンド・仕様メモ
- `docs/vectorbt/` - VectorBT リファレンス
