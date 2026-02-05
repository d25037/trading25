## Role
あなたは apps/bt/ と apps/ts/ の結合を統合管理するオーケストレーターです。
subagentsを用いてそれぞれのプロジェクトを横断的に把握します。

## データフロー・ポート割り当て

```
JQUANTS API ──→ ts/api (:3001) ──→ bt (REST APIクライアント)
                  ↑                    ↓
               ts/shared           bt/server (:3002)
                  ↑                    ↓
               ts/web (:5173) ←── /bt proxy ──→ bt/server
```

| サービス | ポート | 技術 |
|---|---|---|
| ts/api | 3001 | Hono + bun |
| bt/server | 3002 | FastAPI + uvicorn |
| ts/web | 5173 | Vite + React 19 |

- **ts/api** が唯一のJQuants API窓口かつデータベース管理者
- **bt** は ts/api 経由でデータにアクセス（直接DB禁止）
- **ts/web** は `/bt` パスを bt/server にプロキシ

## OpenAPI契約

bt が FastAPI の OpenAPI スキーマを公開し、ts/shared が型を自動生成する。
```bash
bun run --filter @trading25/shared bt:sync   # bt の OpenAPI → TS型生成
```
スキーマ変更時は必ず `bt:sync` を実行し、`contracts/` 配下も更新すること。

## 共有XDGパス

両プロジェクトが `~/.local/share/trading25/` を共有:
- `market.db` / `datasets/` / `portfolio.db` — ts が管理
- `strategies/experimental/` / `backtest/results/` — bt が管理

## bt (Python / uv)
VectorBT基盤の高速バックテスト・Marimo Notebook実行システム。
FastAPI サーバー（:3002）とtyper CLI を提供。

```bash
uv sync                          # 環境セットアップ
uv run bt server --port 3002     # APIサーバー起動
uv run bt backtest <strategy>    # バックテスト実行
uv run pytest tests/             # テスト
uv run ruff check src/           # リント
uv run pyright src/              # 型チェック
```

主要技術: Python 3.12, vectorbt, pydantic, FastAPI, pandas, ruff, pyright, pytest

## ts (TypeScript / bun)
日本株式の解析を行うTypeScriptモノレポ。ランタイムは **bun** を使用。

| パッケージ | 役割 |
|---|---|
| `packages/api/` | Hono OpenAPI サーバー（JQuants API窓口・DB管理） |
| `packages/web/` | React 19 + Vite フロントエンド |
| `packages/shared/` | 共有ライブラリ（JQuants, SQLite, TA/FA指標） |
| `packages/cli/` | Gunshi CLI（dataset/portfolio/analysis） |

```bash
bun dev                          # web + api 同時起動
bun dev:full                     # bt:sync + dev
bun run test                     # テスト
bun typecheck:all                # 型チェック
bun lint && bun check:fix        # リント（Biome）
```

主要技術: TypeScript, Bun, Hono, React 19, Vite, Tailwind CSS v4, Biome, Drizzle ORM

## Issue管理

プロジェクトルートの `issues/`（オープン）、`issues/done/`（クローズ済み）で管理。
フォーマット: `{id}-{slug}.md`（例: `bt-016-test-coverage-70.md`）

## CI

`.github/workflows/ci.yml` により全ブランチ push / PR で自動実行。
- **ts**: lint → 型生成 → build → typecheck → test + coverage
- **bt**: lint → typecheck → test + coverage（ゲート65%）
