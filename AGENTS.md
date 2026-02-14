## Role
あなたは apps/bt/ と apps/ts/ の結合を統合管理するオーケストレーターです。
subagentsを用いてそれぞれのプロジェクトを横断的に把握します。

## データフロー・ポート割り当て

```
JQUANTS API ──→ FastAPI (:3002) ──→ SQLite (market.db / portfolio.db / datasets)
                     ↓
                  ts/web (:5173)
                  ts/cli
```

| サービス | ポート | 技術 |
|---|---|---|
| bt/server | 3002 | FastAPI + uvicorn |
| ts/web | 5173 | Vite + React 19 |

- **FastAPI** が唯一のバックエンド（117 EP: Hono 移行 90 + bt 固有 27）
- **bt** は SQLite に直接アクセス（`contracts/` スキーマ準拠、SQLAlchemy Core 使用）
  - **market.db**: 読み書き（SQLAlchemy Core）
  - **portfolio.db**: CRUD（SQLAlchemy Core）
  - **dataset.db**: 読み書き（SQLAlchemy Core）
- `market.db` の `incremental sync` は `topix_data` / `stock_data` だけでなく `indices_data` も更新する（`indices-only` は指数再同期専用モード）
- Backtest 実行パスは `BT_DATA_ACCESS_MODE=direct` で DatasetDb/MarketDb を直接参照し、FastAPI 内部HTTPを経由しない
- Strategy 設定検証の SoT は backend strict validation（`/api/strategies/{name}/validate` と保存時検証）で、frontend のローカル検証は補助扱い（deprecated）
- 市場コードフィルタは legacy (`prime/standard/growth`) と current (`0111/0112/0113`) を同義として扱う
- **ts/web** は `/api` パスを FastAPI (:3002) にプロキシ
- **Hono サーバー** (:3001) は廃止済み（`apps/ts/packages/api` は削除済み）

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
- **現行追加契約**: `fundamentals-metrics-v1.schema.json`（fundamentals API 指標拡張）
- **アーカイブ**: `hono-openapi-baseline.json`（Phase 3 移行 baseline、参照用に保持）

## エラーレスポンス

統一エラーレスポンスフォーマット:
```json
{"status":"error","error":"Not Found","message":"...","details?":[...],"timestamp":"...","correlationId":"..."}
```
- FastAPI: 例外ハンドラが `HTTPException(detail=...)` を自動変換
- `RequestLoggerMiddleware` が `JQuantsApiError`(502/504) / `SQLAlchemyError`(500) / 汎用例外(500) をキャッチし統一フォーマットで返却
- correlation ID: `x-correlation-id` ヘッダで伝播（なければ自動生成）
- 内部HTTP呼び出し（`src/api/client.py`）も `x-correlation-id` を伝播
- ErrorResponse スキーマは OpenAPI で全エンドポイントに 400/404/500 として公開

## J-Quants Proxy キャッシュ/観測

- `JQuantsProxyService` は in-memory TTL + singleflight を使用
  - `/markets/margin-interest`: 5分
  - `/fins/summary`（`/statements` / `/statements/raw` で共有）: 15分
- 実外部呼び出しは `event="jquants_fetch"`、キャッシュ状態は `event="jquants_proxy_cache"` で構造化ログ出力

## ミドルウェア構成（FastAPI）

登録順（LIFO: 下から上に実行）:
1. **RequestLoggerMiddleware** — リクエストロギング（最外側）
2. **CorrelationIdMiddleware** — correlation ID 管理
3. **CORSMiddleware** — CORS（最内側）

- OpenAPI 設定は `openapi_config.py` に集中管理
- ドキュメント UI: `/doc`（Swagger UI）、`/docs` `/redoc` は無効

## 共有XDGパス

両プロジェクトが `~/.local/share/trading25/` を共有:
- `market.db` / `datasets/` / `portfolio.db` — FastAPI が管理
- `strategies/experimental/` / `backtest/results/` / `backtest/attribution/` — bt が管理

## bt (Python / uv)
VectorBT基盤の高速バックテスト・Marimo Notebook実行システム。
FastAPI サーバー（:3002）とtyper CLI を提供。

```bash
uv sync                          # 環境セットアップ
uv run bt server --port 3002     # APIサーバー起動
uv run bt backtest <strategy>    # バックテスト実行
uv run bt lab generate --entry-filter-only --allowed-category fundamental
uv run bt lab evolve <strategy> --entry-filter-only --allowed-category fundamental
uv run bt lab optimize <strategy> --entry-filter-only --allowed-category fundamental
uv run bt lab improve <strategy> --entry-filter-only --allowed-category fundamental
uv run pytest tests/             # テスト
uv run ruff check src/           # リント
uv run pyright src/              # 型チェック
```

- Lab `evolve/optimize` の API/Web は `target_scope`（`entry_filter_only` / `exit_trigger_only` / `both`）を受け付ける（`entry_filter_only` は互換フラグとして維持）
- Lab `evolve/optimize` の frontend `allowed categories` は `all` / `fundamental only` を提供

主要技術: Python 3.12, vectorbt, pydantic, FastAPI, pandas, ruff, pyright, pytest

## ts (TypeScript / bun)
日本株式の解析を行うTypeScriptモノレポ。ランタイムは **bun** を使用。

| パッケージ | 役割 |
|---|---|
| `packages/web/` | React 19 + Vite フロントエンド |
| `packages/shared/` | 共有ライブラリ（OpenAPI 生成型, JQuants, TA/FA指標） |
| `packages/cli/` | Gunshi CLI（dataset/portfolio/analysis/backtest attribution） |
| `packages/clients-ts/` | FastAPI クライアント（backtest/JQuants） |

```bash
bun dev                          # web 起動（FastAPI :3002 にプロキシ）
bun dev:full                     # bt:sync + dev
bun run test                     # テスト
bun typecheck:all                # 型チェック
bun lint && bun check:fix        # リント（Biome）
bun run --filter @trading25/web e2e:smoke  # web E2E smoke（Playwright）
bun run cli backtest attribution run <strategy> --wait
```

- Backtest UI は `Attribution` サブタブ内に `Run` / `History` を持ち、進捗取得は 2 秒ポーリング

主要技術: TypeScript, Bun, React 19, Vite, Tailwind CSS v4, Biome, OpenAPI generated types

## Issue管理

プロジェクトルートの `issues/`（オープン）、`issues/done/`（クローズ済み）で管理。
フォーマット: `{id}-{slug}.md`（例: `bt-016-test-coverage-70.md`）

## Skills ガバナンス

- プロジェクト正本のスキルは `/.codex/skills` に配置する
- `apps/ts/.claude/skills` と `apps/bt/.claude/skills` は legacy 参照用（read-only）
- 参照生成: `scripts/skills/refresh_skill_references.py`
- 監査: `scripts/skills/audit_skills.py --strict-legacy`

## CI

`.github/workflows/ci.yml` により全ブランチ push / PR で自動実行。
- **skills**: audit（stale検知 / frontmatter検証 / legacy変更検知）
- **ts**: lint → 型生成 → build → typecheck → test + coverage
- **web e2e**: Playwright Chromium smoke（bt server :3002 を起動して実行）
- **bt**: lint → typecheck → test + coverage（ゲート70%）

## ロードマップ

現行インデックスは [`docs/unified-roadmap.md`](docs/unified-roadmap.md) を参照。  
Phase 1-4 の大規模リファクタリングは完了し、実行タスクは `issues/`（open）/`issues/done/`（closed）で管理する。  
旧統合ロードマップ本文は `docs/archive/unified-roadmap-2026-02-10.md` に archive 済み。
