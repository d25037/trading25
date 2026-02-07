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
- **bt** は SQLite に直接アクセス（`contracts/` スキーマ準拠、SQLAlchemy Core 使用）
  - **market.db**: 読み取り専用（Phase 3B: `sqlite3 ?mode=ro`）+ 書き込み（Phase 3D: SQLAlchemy Core）
  - **portfolio.db**: CRUD（Phase 3E: SQLAlchemy Core）
  - **dataset.db**: 読み取り専用（Phase 3D: SQLAlchemy Core）
- **ts/web** は `/bt` パスを bt/server にプロキシ

## OpenAPI契約

bt が FastAPI の OpenAPI スキーマを公開し、ts/shared が型を自動生成する。
```bash
bun run --filter @trading25/shared bt:sync   # bt の OpenAPI → TS型生成
```
スキーマ変更時は必ず `bt:sync` を実行し、`contracts/` 配下も更新すること。

## contracts/ ガバナンス

`contracts/` に bt/ts 間の安定インターフェースを定義。詳細は [`contracts/README.md`](contracts/README.md) 参照。
- **バージョニング**: additive (minor) / breaking (major) → 新版ファイル作成
- **命名規則**: `{domain}-{purpose}-v{N}.schema.json`
- **凍結ファイル**: `hono-openapi-baseline.json`（Phase 3 完了まで変更禁止）

## エラーレスポンス（Hono 互換）

bt/ts 共通の統一エラーレスポンスフォーマット:
```json
{"status":"error","error":"Not Found","message":"...","details?":[...],"timestamp":"...","correlationId":"..."}
```
- FastAPI: 例外ハンドラが `HTTPException(detail=...)` を自動変換
- correlation ID: `x-correlation-id` ヘッダで伝播（なければ自動生成）
- ErrorResponse スキーマは OpenAPI で全エンドポイントに 400/404/500 として公開

## ミドルウェア構成（FastAPI）

登録順（LIFO: 下から上に実行）:
1. **RequestLoggerMiddleware** — リクエストロギング（最外側、Hono `httpLogger` 互換）
2. **CorrelationIdMiddleware** — correlation ID 管理
3. **CORSMiddleware** — CORS（最内側）

- OpenAPI 設定は `openapi_config.py` に集中管理
- ドキュメント UI: `/doc`（Swagger UI）、`/docs` `/redoc` は無効

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
- **bt**: lint → typecheck → test + coverage（ゲート70%）

## ロードマップ

[`docs/unified-roadmap.md`](docs/unified-roadmap.md) で Phase 1-5 を管理。現在 Phase 3B-1 完了、Phase 3B-2a 完了。
