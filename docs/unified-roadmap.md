# trading25 統一ロードマップ

作成日: 2026-02-06
最終更新: 2026-02-09（Phase 4B 方針転換・Phase 4C Step1 着手）
統合元: 5つの個別ロードマップ（[Appendix D](#appendix-d-アーカイブ元ドキュメント) 参照）

---

## Governance Baseline（運用規約）

### 現行ルール

| ルール | 根拠 |
|---|---|
| FastAPI (:3002) が唯一のバックエンド（Phase 3F 完了） | AGENTS.md |
| `apps/bt` は SQLite に直接アクセス（contracts/ スキーマ準拠、SQLAlchemy Core 使用） | ADR-003 |
| Hono サーバー (:3001) は archived（`apps/ts/packages/api` は read-only） | Phase 3F |
| OpenAPI 契約: `bt:sync` で FastAPI スキーマ → TS 型生成 | AGENTS.md |

### 変更承認条件

ルール変更時は以下を必須とする:
1. Decision Log に ADR を追加
2. 影響する AGENTS.md セクションの更新案を明記
3. 関連 Issue の作成

### OpenAPI 更新手順

```bash
bun run --filter @trading25/shared bt:sync   # bt の OpenAPI → TS型生成
```

スキーマ変更時は必ず `bt:sync` を実行し、`contracts/` 配下も更新すること。

---

## Decision Log（意思決定記録）

### ADR-001: モノレポ統合（パターン A 採用）

- **決定日**: 2025-02（初期統合時）
- **決定**: `trading25/` を単一 Git リポジトリとし、`apps/` + `packages/` + `contracts/` 構成を採用
- **API 方向**: パターン A（bt→ts 許可、ts→bt 撤去）
- **履歴**: 履歴破棄（クリーンスタート）
- **元ドキュメント**: `docs/archive/monorepo-integration.md`

### ADR-002: SQLite 維持（Parquet 移行しない）

- **決定日**: 2026-02-06
- **決定**: SQLite を System of Record として維持、Parquet/DuckDB への移行は行わない
- **詳細**: [Appendix A](#appendix-a-sqlite-vs-parquet-決定記録) 参照
- **スナップショット**: dataset 安定境界は SQLite ベースで実装（Parquet/Arrow ではなく）
- **将来の拡張**: 10M 行超の場合のみ Parquet/DuckDB sidecar を検討可能

### ADR-003: DB 管理責務の移行（ts→FastAPI）— 条件付き承認

- **提案日**: 2026-02-06
- **決定日**: 2026-02-06（Phase 2C）
- **ステータス**: 条件付き承認
- **提案**: Phase 3C で FastAPI が market.db / dataset.db / portfolio.db に直接アクセスする
- **前提条件（全て充足）**:
  - [x] Phase 2B（FastAPI 事前調査）完了 — [監査レポート](reports/phase2b-endpoint-audit.md) 参照
  - [x] AGENTS.md の「直接 DB 禁止」ルールの更新（Phase 3C 開始時に実施）
- **元ドキュメント**: `docs/archive/hono-to-fastapi-migration-roadmap.md`

#### リスク評価

| 観点 | 現状（ts 管理） | 提案（FastAPI 管理） | リスク |
|------|----------------|---------------------|--------|
| DB アクセス | Bun SQLite + Drizzle ORM | SQLAlchemy Core（ORM なし） | Medium |
| マイグレーション | Drizzle migrate | 手動 or Alembic | Low |
| トランザクション | WAL mode | WAL mode（同一） | Low |
| JQuants 連携 | TS JQuants client | Python JQuants client（要実装） | **High** |
| テスト | 成熟（tests 多数） | 要新規構築 | Medium |

#### 主要リスク

1. **High**: JQuants Python client の実装コスト（認証・レート制限・ページネーション）
2. **Medium**: Drizzle → sqlite3 のスキーマ互換性維持（`contracts/dataset-db-schema-v2.json` を正とする）
3. **Low**: WAL mode 並行読み取り（同一設定で対応可）

#### 承認条件と制約

- Phase 3C で Python SQLite アクセス層を段階的に構築
- `contracts/dataset-db-schema-v2.json` との整合性検証テスト必須
- AGENTS.md の「直接 DB 禁止」ルールは Phase 3C 開始時に更新（それまでは現行ルール維持）
- JQuants Python client は Phase 3B の JQuants Proxy 移行時に実装

#### AGENTS.md 更新案（Phase 3C 開始時に適用）

ルート AGENTS.md:
- 変更前: `apps/bt` は `apps/ts` API 経由でデータアクセス（直接 DB 禁止）
- 変更後: `apps/bt` は SQLite に直接アクセス（`contracts/` スキーマ準拠）

`apps/bt/AGENTS.md`:
- API クライアント経由から直接 DB アクセスへの移行パスを記載

---

## ステータスダッシュボード

| Phase | 名称 | 状態 | リスク | 見積 |
|---|---|---|---|---|
| 1 | 基盤安定化 | **完了** | Low | 1-2 週 |
| 2 | 契約・データ境界 | **実質完了**（延期項目あり） | Low | 1-2 週 |
| 3 | FastAPI 統一 | **完了**（3F 切替・廃止完了） | **High** | 6-10 週 |
| 4 | パッケージ分離 | **進行中（4A 完了、4C Step1 完了、4B 方針転換済み）** | Medium | 4-6 週 |
| 5 | シグナル・分析拡張 | **未着手** | Low | 2-3 週 |

---

## アーキテクチャ: 現状 vs 目標

### 現状（Phase 3F 完了後）

```
JQUANTS API ──→ FastAPI (:3002) ──→ SQLite (market.db / portfolio.db / datasets)
                     ↓
                  ts/web (:5173)
                  ts/cli
```

- FastAPI が唯一のバックエンド（117 EP: Hono 移行 90 + bt 固有 27）
- Hono サーバー廃止（`apps/ts/packages/api` は archived・read-only）
- Web/CLI は全て FastAPI (:3002) に接続

---

## 完了済み作業アーカイブ

### モノレポ統合 Phase 1（完了）

*元: monorepo-migration-plan.md Phase 1*

- [x] Root CI workflow (`.github/workflows/ci.yml`)
- [x] Root scripts (`scripts/`)
- [x] Root `README.md` 更新

### TA 統合 Phase 0-3.5（完了）

*元: plan-ta-consolidation.md Phase 0-3.5*

- [x] **Phase 0** (2026-02-02): bt-018〜021 解消、API client/CLI 整理
- [x] **Phase 1** (2026-02-02): Indicator API 構築（11 種テクニカル + 3 種信用指標）
- [x] **Phase 2** (2026-02-02): apps/ts/web の API 移行（useBtIndicators/useBtMarginIndicators）
- [x] **Phase 2.5**: 並走検証完了（[検証レポート](reports/phase2_5_verification_report.md)）
- [x] **Phase 3** (2026-02-03): apps/ts/shared/src/ta/ 段階的廃止
  - 削除: sma, ema, rsi, macd, ppo, bollinger, atr, atr-support, n-bar-support, volume-comparison, trading-value-ma, margin-pressure-indicators, margin-volume-ratio
  - 残存: relative/, timeframe/, utils.ts
- [x] **Phase 3.5** (2026-02-03): relative OHLC サポート + margin-volume-ratio 移行

---

## Phase 1: 基盤安定化

**期間**: 1-2 週 | **リスク**: Low

### 1A: TA 回帰監視設定

*元: plan-ta-consolidation.md Phase 2.5 の縮小版*

フル並走検証は不要（Phase 3.5 完了により実質検証済み）。回帰監視のみ設定する。

- [ ] 11 指標 × 代表銘柄 3-5 銘柄の定期差分テスト作成
- [ ] P95 レイテンシ監視（閾値 800ms）— Phase 2.5 の判定基準を定期監視に組み込む
- [ ] 週次サンプル比較スクリプト作成（不一致率 < 0.1%、API エラー率 < 1% を監視）

### 1B: 依存方向監査

*元: monorepo-migration-plan.md Phase 3*

- [x] ts→bt、bt→ts の全呼び出し箇所の洗い出し
- [x] パターン A（bt→ts 許可、ts→bt 撤去）の適用状況確認
- [x] 依存方向違反のチェック機構追加（CI or lint rule）

**成果物**:
- [`docs/reports/dependency-audit-phase1b.md`](reports/dependency-audit-phase1b.md) — 監査レポート（bt→ts 15件準拠、ts→bt 39件分類済み）
- `scripts/dep-direction-allowlist.txt` — 許可ファイル一覧（permanent 3件 + phase3-removal 36件）
- `scripts/check-dep-direction.sh` — CI チェックスクリプト（違反検出 + staleness check）
- `scripts/lint.sh` に統合済み（既存 lint の前に実行）

### 1C: オープン Issue 対応

*元: 各ドキュメントの残タスク*

**bt 系**:
- [x] bt-016: テストカバレッジ 70% 達成（CI ゲート 65%→70% 引き上げ、73%到達済み）
- [x] bt-017: signal registry param key validation（_validate_registry() 追加）
- [x] bt-018: pyright pandas type errors（既にクローズ済み）
- [x] bt-019: resample compatibility test todo（TODOコメント削除、アサーション有効化）
- [x] bt-020: pydantic field example deprecation（json_schema_extra に移行）

**ts 系**:
- [x] ts-121: market endpoint scope clarify（OpenAPI description に消費者スコープ追記）
- [x] ts-122: screening logic single source（分離維持を意思決定、文書化）
- [x] ts-123: remove deprecated fundamentals data service（1,552行削除）

**完了**: 2026-02-06。全 8 件（bt-018 は既にクローズ済み）を解消。Phase 1 全体が完了し、Phase 2 に進行可能。

---

## Phase 2: 契約・データ境界

**期間**: 1-2 週 | **リスク**: Low

### 2A: 契約スキーマ完成

*元: monorepo-migration-plan.md Phase 2 + packages-responsibility-roadmap.md Phase 0-1*

- [x] `contracts/dataset-schema.json` を deprecated 化、v2 を正として明記
- [ ] `packages/contracts` 作成・型生成ルール策定 — **Phase 4 に延期**（`contracts/` 直接管理で十分）
- [x] 契約バージョニングルール策定（additive vs breaking）
- [x] 依存方向ルールのドキュメント化
- [x] 新規スキーマのファイル命名規則決定
- [x] dataset スナップショット出力機能の方針決定（SQLite ベースの安定スキーマ出力 + manifest.json）
- [ ] apps/ts にスナップショット出力機能を実装 — **Phase 3 前提作業に延期**
- [ ] apps/bt にスナップショットリーダー + スキーマバージョン検証を実装 — **Phase 3 前提作業に延期**

### 2B: FastAPI 事前調査

*元: hono-to-fastapi-migration-roadmap.md Phase 0*

- [x] OpenAPI 固定（Hono の openapi.json を `contracts/hono-openapi-baseline.json` として凍結）
- [x] 既存 FastAPI エンドポイント vs Hono エンドポイント監査 — [`docs/reports/phase2b-endpoint-audit.md`](reports/phase2b-endpoint-audit.md)
- [x] 例外レスポンスフォーマット統一（Hono 互換 6 フィールド + correlation ID ミドルウェア）
- [x] FastAPI 側の既存エンドポイント一覧整理、競合パス明確化（重複: `/api/health` のみ）

### 2C: ADR-003 策定（DB 管理責務移行の正式決定）

- [x] DB 管理責務の ts→FastAPI 移行を正式に検討・決定（条件付き承認）
- [x] AGENTS.md 更新案の作成（Phase 3C 開始時に適用）
- [x] 移行に伴うリスク評価（ADR-003 に記載）

**完了**: 2026-02-06。延期項目（snapshot 実装: Phase 3 前提作業、`packages/contracts` workspace 化: Phase 4）を除き実質完了。

**成果物**:
- [`contracts/README.md`](../contracts/README.md) — ガバナンスルール・バージョニング方針・snapshot 方針
- [`contracts/hono-openapi-baseline.json`](../contracts/hono-openapi-baseline.json) — Hono OpenAPI 凍結ベースライン
- [`docs/reports/phase2b-endpoint-audit.md`](reports/phase2b-endpoint-audit.md) — エンドポイント監査レポート（Hono 90 + FastAPI 41）
- 統一エラーレスポンス（Hono 互換 6 フィールド + correlation ID ミドルウェア）
- ts-116 解消（OptimizationHtmlFile 型を bt OpenAPI に追加）
- ADR-003 条件付き承認

---

## Phase 3: FastAPI 統一 — クリティカルパス

**期間**: 6-10 週 | **リスク**: High

各サブフェーズ間に Go/No-Go 判定ゲートを設置。切り戻し範囲はドメイン単位に限定。

### 3A: ミドルウェア・基盤 — **完了** (2026-02-06)

*元: hono-to-fastapi-migration-roadmap.md Phase 1*

- [x] correlation id, エラーハンドリング（Phase 2 で前倒し実施）
- [x] request logging（`RequestLoggerMiddleware`、Hono `httpLogger` 互換フォーマット）
- [x] CORS 統合（Hono 互換オリジン + 明示的 headers/methods + `expose_headers`）
- [x] OpenAPI タグ統一（Hono 10 operation tags + bt 固有 8 タグ = 18 タグ）
- [x] `/openapi.json` を Hono 互換で提供（`info.title`/`contact`/`license`/`servers` 統一）
- [x] ErrorResponse スキーマを OpenAPI に公開（全エンドポイントに 400/404/500 共通注入）
- [x] `/doc` に Swagger UI 配置（`/docs` `/redoc` 無効化）
- [x] テスト基盤（`sync_client`/`async_client` fixture、`respx` dev 依存追加）
- [x] OpenAPI 互換性検証スクリプト（`scripts/verify-openapi-compat.py`）

**Go/No-Go 結果**: 全基準クリア
- 新規テスト 27 件全通過（ミドルウェア 8 + CORS 6 + OpenAPI 13）
- 既存テスト 2186 件全通過（エラーフォーマット 12 + Correlation ID 4 含む）
- ruff check: 0 errors、pyright: 0 errors

**成果物**:

| ファイル | 内容 |
|---------|------|
| `apps/bt/src/server/middleware/request_logger.py` | リクエストロギングミドルウェア（新規） |
| `apps/bt/src/server/openapi_config.py` | OpenAPI 設定集中管理 + ErrorResponse 注入（新規） |
| `apps/bt/tests/unit/server/middleware/test_request_logger.py` | ロガーテスト + ミドルウェア順序テスト（新規） |
| `apps/bt/tests/unit/server/test_cors.py` | CORS テスト（新規） |
| `apps/bt/tests/unit/server/test_openapi.py` | OpenAPI 互換テスト（新規） |
| `scripts/verify-openapi-compat.py` | baseline vs FastAPI 互換性検証スクリプト（新規） |
| `apps/bt/src/server/app.py` | ミドルウェア登録順（LIFO）修正、CORS/OpenAPI 設定適用（変更） |
| `apps/bt/pyproject.toml` | `respx>=0.21.0` dev 依存追加（変更） |
| `apps/bt/tests/conftest.py` | `sync_client`/`async_client` fixture 追加（変更） |

**httpx 活用方針**（Phase 3 全体）:
- 3A: 既存 bt→ts 呼び出し維持。`httpx.ASGITransport` テスト基盤 + `respx` 準備
- 3B: `httpx.AsyncClient` で JQuants API 直接呼び出し（リトライ + トークン管理）
- 3C-E: bt→ts 各クライアント順次不要化
- 3F: `src/api/` パッケージ完全削除

### 3B: 読み取り API 移行 — **完了** (2026-02-07)

*元: hono-to-fastapi-migration-roadmap.md Phase 2*

25 読み取りエンドポイントを 4 サブフェーズで FastAPI に移行完了:

- [x] **3B-1**: JQuants Proxy + Health (12 EP) — JQuantsAsyncClient, RateLimiter, ROE/margin 計算
- [x] **3B-2a**: market.db 直接読み取り (4 EP) — MarketDbReader (sqlite3 read-only URI)
- [x] **3B-2b**: Chart + sector-stocks (6 EP) — JQuants fallback 併用, ChartService
- [x] **3B-3**: Complex Analytics (3 EP) — RankingService, FactorRegressionService, ScreeningService

> `portfolio-factor-regression` は portfolio.db 依存のため Phase 3E に延期。

**Go/No-Go 結果**: 全基準クリア
- 新規テスト 160+ 件全通過（2307→2346 tests）
- 既存テスト全通過（ruff 0 errors, pyright 0 errors）
- ThreadPoolExecutor lifecycle issue 解決（`_get_executor()` パターン）

**成果物**:

| カテゴリ | ファイル |
|---------|---------|
| JQuants Client | `src/server/clients/jquants_client.py`, `rate_limiter.py` |
| DB Reader | `src/server/db/market_reader.py` |
| Routes | `analytics_complex.py`, `analytics_jquants.py`, `chart.py`, `jquants_proxy.py`, `market_data.py` |
| Schemas | `ranking.py`, `factor_regression.py`, `screening.py`, `chart.py`, `jquants.py`, `market_data.py`, `analytics_roe.py`, `analytics_margin.py` |
| Services | `ranking_service.py`, `factor_regression_service.py`, `screening_service.py`, `chart_service.py`, `jquants_proxy_service.py`, `market_data_service.py`, `roe_service.py`, `margin_analytics_service.py` |
| Contract | `contracts/market-db-schema-v1.json` |

**Key Lessons**:
- ThreadPoolExecutor: module-level executors killed by lifespan shutdown → `_get_executor()` recreate pattern
- Factor regression: population variance OLS (N divisor) matches Hono implementation
- Screening: 200+ days data requirement, inlined SMA/EMA to avoid external deps

### 3C: Python SQLite アクセス層 — **完了** (2026-02-07)

*元: hono-to-fastapi-migration-roadmap.md 案A + 新規*

**前提**: ADR-003 承認済み、AGENTS.md 更新済み

SQLAlchemy Core（ORM なし）を採用し、3 データベース・17 テーブルの Python アクセス層を構築:

- [x] `tables.py`: 17 テーブル定義（market_meta 6 + dataset_meta 7 + portfolio_meta 5）
- [x] `base.py`: BaseDbAccess（Engine 管理、StaticPool、PRAGMA event listener）
- [x] `query_helpers.py`: 共通クエリフラグメント（normalize_stock_code 等 6 関数）
- [x] `market_db.py`: MarketDb（read + write、upsert 系メソッド）
- [x] `dataset_db.py`: DatasetDb（read-only、15+ メソッド）
- [x] `portfolio_db.py`: PortfolioDb（CRUD: portfolio/item/watchlist/watchlist_item + summary）
- [x] `contracts/portfolio-db-schema-v1.json`: Portfolio DB 契約（5 テーブル定義）
- [x] `settings.py` + `app.py`: lifespan 拡張（MarketDb / PortfolioDb 初期化・シャットダウン）
- [x] AGENTS.md 更新（「直接 DB 禁止」→「SQLAlchemy Core 直接アクセス」）

**Go/No-Go 結果**: 全基準クリア
- 新規テスト 162 件全通過（2346→2508 tests）
- 契約整合性: 17 テーブル × columns/PK/FK/UNIQUE/INDEX が contracts/ JSON と完全一致
- CRUD: create → read → update → delete + CASCADE 削除 + FK 制約
- PRAGMA: WAL + foreign_keys が接続イベントで確実に設定
- 既存 3B テスト全通過（ruff 0 errors, pyright 0 errors）

**成果物**:

| カテゴリ | ファイル |
|---------|---------|
| テーブル定義 | `src/server/db/tables.py`（17 テーブル、3 MetaData） |
| 基底クラス | `src/server/db/base.py`（Engine 管理、read-only creator パターン） |
| クエリヘルパー | `src/server/db/query_helpers.py`（6 共通関数） |
| MarketDb | `src/server/db/market_db.py`（read + write） |
| DatasetDb | `src/server/db/dataset_db.py`（read-only、15+ メソッド） |
| PortfolioDb | `src/server/db/portfolio_db.py`（CRUD + watchlist + summary） |
| 契約 | `contracts/portfolio-db-schema-v1.json`（5 テーブル） |
| テスト | `test_tables.py`(54), `test_base.py`(7), `test_query_helpers.py`(12), `test_market_db.py`(15), `test_dataset_db.py`(27), `test_portfolio_db.py`(47) |

**Key Lessons**:
- SQLAlchemy `Real` 型は存在しない — `from sqlalchemy.types import REAL` を使用
- `Column("name", Text, unique=True)` は無名 UniqueConstraint を生成 — 明示的 `UniqueConstraint("name", name="...")` が必要
- SQLite read-only: `creator` コールバック + `sqlite3.connect(uri, uri=True)` パターン
- `@event.listens_for(engine, "connect")` は pyright に `reportUnusedFunction` で警告される

### 3D: DB・ジョブ API 移行 — **完了** (2026-02-07)

*元: hono-to-fastapi-migration-roadmap.md Phase 3*

**前提**: 3C 完了（DB 操作は直接アクセスが前提）

30 エンドポイントを 4 サブフェーズで FastAPI に移行完了:

- [x] **3D-1**: Dataset Data + 簡易操作 (20 EP) — DatasetResolver, DatasetDb 拡張, 15 data EP + 5 management EP
- [x] **3D-2**: DB Stats + Validate (2 EP) — MarketDb 拡張 (~15 メソッド追加), db_stats_service, db_validation_service
- [x] **3D-3**: GenericJobManager + Sync + Refresh (4 EP) — 汎用ジョブマネージャ, 3 sync 戦略, stock refresh
- [x] **3D-4**: Dataset Create + Resume + Jobs (4 EP) — DatasetWriter, dataset_builder_service, 9 プリセット

**Go/No-Go 結果**: 全基準クリア
- 新規テスト 120 件全通過（2508→2628 tests）
- ジョブライフサイクル: create→running→complete/cancel/fail 全パス検証済み
- GenericJobManager: asyncio.Lock 排他制御 + asyncio.Event 協調キャンセル
- 既存テスト全通過（ruff 0 errors, pyright 0 errors）

**成果物**:

| カテゴリ | ファイル |
|---------|---------|
| Routes | `db.py`(6 EP), `dataset.py`(9 EP), `dataset_data.py`(15 EP) |
| Schemas | `db.py`, `dataset.py`, `dataset_data.py`, `job.py` |
| Services | `generic_job_manager.py`, `sync_service.py`, `sync_strategies.py`, `stock_refresh_service.py`, `dataset_builder_service.py`, `dataset_presets.py`, `dataset_resolver.py`, `dataset_service.py`, `dataset_data_service.py` |
| DB | `dataset_writer.py`（DatasetWriter: .db ファイル書き込み） |
| DB 拡張 | `market_db.py`(+15 メソッド), `dataset_db.py`(+10 メソッド) |
| テスト | `test_dataset_resolver.py`(12), `test_dataset_db_extended.py`(14), `test_routes_dataset_data.py`(18), `test_routes_dataset.py`(10), `test_routes_db.py`(4), `test_generic_job_manager.py`(15), `test_routes_db_sync.py`(7), `test_dataset_presets.py`(7), `test_dataset_writer.py`(10), `test_dataset_builder_service.py`(12), `test_routes_dataset_jobs.py`(11) |

**Key Lessons**:
- SQLAlchemy Row `.count` attribute conflicts with built-in `count` method — use index access `r[0]`, `r[1]`
- dataset_meta tables have plain names (stocks, stock_data) not prefixed (ds_stocks)
- `asyncio.to_thread()` for blocking DB writes in async context to avoid blocking event loop
- Module-level job managers need `shutdown()` in app lifespan
- GenericJobManager: `asyncio.Lock` for create exclusivity, `asyncio.Event` for cooperative cancellation

### 3E: Portfolio/Watchlist API 移行 — **完了** (2026-02-07)

*元: hono-to-fastapi-migration-roadmap.md Phase 4*

**前提**: 3D 完了（DB 操作は直接アクセスが前提）、PortfolioDb 26 メソッド完備

21 エンドポイントを 2 サブフェーズで FastAPI に移行完了:

- [x] **3E-1**: Portfolio CRUD (11 EP) + Watchlist CRUD (7 EP) — IntegrityError→409, model_fields_set 活用
- [x] **3E-2**: Performance (1 EP) + Prices (1 EP) + Portfolio Factor Regression (1 EP) — OLS 回帰再利用, N+1 回避

**Go/No-Go 結果**: 全基準クリア
- 新規テスト 76 件全通過（2628→2704 tests）
- CRUD 全操作のデータ整合性テスト合格（create→read→update→delete + 409 重複検出）
- 既存テスト全通過（ruff 0 errors, pyright 0 errors）

**成果物**:

| カテゴリ | ファイル |
|---------|---------|
| Routes | `portfolio.py`(12 EP), `watchlist.py`(8 EP), `analytics_complex.py`(+1 EP) |
| Schemas | `portfolio.py`, `watchlist.py`, `portfolio_performance.py`, `portfolio_factor_regression.py` |
| Services | `portfolio_performance_service.py`, `watchlist_prices_service.py`, `portfolio_factor_regression_service.py` |
| DB 拡張 | `portfolio_db.py`(+2 メソッド: `list_portfolio_summaries`, `list_watchlist_summaries`) |
| テスト | `test_routes_portfolio.py`(30), `test_routes_watchlist.py`(19), `test_routes_portfolio_performance.py`(6), `test_routes_watchlist_prices.py`(4), `test_routes_portfolio_factor_regression.py`(6), `test_watchlist_prices_service.py`(5), `test_portfolio_db.py`(+6) |

**Key Lessons**:
- IntegrityError 判定: SQLite は制約名でなく `UNIQUE constraint failed: tablename.column` 形式 → `str(e.orig)` で判定
- `model_fields_set` で Pydantic の "未指定" vs "null送信" を区別（description の null 更新）
- `list[Row[Any]]` は `list[object]` に代入不可（invariant）→ `Sequence[Row[Any]]` を使用
- Portfolio factor regression: `_load_indices_returns()` を N+1 回避版で独自実装（全指数データ一括取得）

### 3F: 切替・廃止 — **完了** (2026-02-07)

*元: hono-to-fastapi-migration-roadmap.md Phase 5*

**前提**: 3A-3E 全完了（117 EP, 2704 テスト）

- [x] **3F-0**: fundamentals GET EP 追加（ブロッカー解消）+ verify-openapi-compat.py 修正
- [x] **3F-1**: Go/No-Go 検証（2709 テスト全通過）
- [x] **3F-2**: Vite proxy 切替（:3001→:3002）+ `/bt` prefix 一括削除（15 ファイル, 98 箇所）
- [x] **3F-3**: CLI + bt API client base URL 変更（:3001→:3002）
- [x] **3F-4**: Hono サーバー停止 + packages/api read-only 化 + CORS/OpenAPI 整理
- [x] **3F-5**: dep-direction-allowlist 整理 + contracts/AGENTS.md/roadmap 更新

**成果物**:

| カテゴリ | ファイル |
|---------|---------|
| Routes | `analytics_jquants.py`(+1 GET EP: fundamentals) |
| Scripts | `verify-openapi-compat.py`(パスパラメータ正規化 + pending→fail) |
| Config | `vite.config.ts`, `settings.py`, `api-client.ts`(URL 切替), `app.py`(CORS), `openapi_config.py`(servers) |
| Docs | `dep-direction-allowlist.txt`, `contracts/README.md`, `AGENTS.md`(root/ts/bt), `unified-roadmap.md` |
| テスト | `test_routes_analytics_fundamentals.py`(5), CORS/settings/client テスト更新 |

### Hono API エンドポイント完全一覧（90）

移行対象の全エンドポイント。Phase 3 各サブフェーズの進捗追跡に使用。

#### Health (1)
| メソッド | パス | 3B |
|---|---|---|
| GET | `/health` | [x] |

#### JQuants Proxy (7)
| メソッド | パス | 3B |
|---|---|---|
| GET | `/api/jquants/auth/status` | [x] |
| GET | `/api/jquants/daily-quotes` | [x] |
| GET | `/api/jquants/indices` | [x] |
| GET | `/api/jquants/listed-info` | [x] |
| GET | `/api/jquants/statements` | [x] |
| GET | `/api/jquants/stocks/{symbol}/margin-interest` | [x] |
| GET | `/api/jquants/topix` | [x] |

#### Chart (5)
| メソッド | パス | 3B |
|---|---|---|
| GET | `/api/chart/indices` | [x] |
| GET | `/api/chart/indices/topix` | [x] |
| GET | `/api/chart/indices/{code}` | [x] |
| GET | `/api/chart/stocks/search` | [x] |
| GET | `/api/chart/stocks/{symbol}` | [x] |

#### Analytics (9)
| メソッド | パス | 3B |
|---|---|---|
| GET | `/api/analytics/factor-regression/{symbol}` | [x] |
| GET | `/api/analytics/fundamentals/{symbol}` | N/A (既存) |
| GET | `/api/analytics/portfolio-factor-regression/{portfolioId}` | [x] |
| GET | `/api/analytics/ranking` | [x] |
| GET | `/api/analytics/roe` | [x] |
| GET | `/api/analytics/screening` | [x] |
| GET | `/api/analytics/sector-stocks` | [x] |
| GET | `/api/analytics/stocks/{symbol}/margin-pressure` | [x] |
| GET | `/api/analytics/stocks/{symbol}/margin-ratio` | [x] |

#### Market Data (4)
| メソッド | パス | 3B |
|---|---|---|
| GET | `/api/market/stocks` | [x] |
| GET | `/api/market/stocks/{code}` | [x] |
| GET | `/api/market/stocks/{code}/ohlcv` | [x] |
| GET | `/api/market/topix` | [x] |

#### Database (6)
| メソッド | パス | 3D |
|---|---|---|
| GET | `/api/db/stats` | [x] |
| POST | `/api/db/stocks/refresh` | [x] |
| POST | `/api/db/sync` | [x] |
| GET | `/api/db/sync/jobs/{jobId}` | [x] |
| DELETE | `/api/db/sync/jobs/{jobId}` | [x] |
| GET | `/api/db/validate` | [x] |

#### Dataset (9)
| メソッド | パス | 3D |
|---|---|---|
| GET | `/api/dataset` | [x] |
| POST | `/api/dataset` | [x] |
| POST | `/api/dataset/resume` | [x] |
| GET | `/api/dataset/jobs/{jobId}` | [x] |
| DELETE | `/api/dataset/jobs/{jobId}` | [x] |
| GET | `/api/dataset/{name}/info` | [x] |
| GET | `/api/dataset/{name}/sample` | [x] |
| GET | `/api/dataset/{name}/search` | [x] |
| DELETE | `/api/dataset/{name}` | [x] |

#### Dataset Data (15)
| メソッド | パス | 3D |
|---|---|---|
| GET | `/api/dataset/{name}/stocks` | [x] |
| GET | `/api/dataset/{name}/stocks/{code}/ohlcv` | [x] |
| GET | `/api/dataset/{name}/stocks/ohlcv/batch` | [x] |
| GET | `/api/dataset/{name}/topix` | [x] |
| GET | `/api/dataset/{name}/indices` | [x] |
| GET | `/api/dataset/{name}/indices/{code}` | [x] |
| GET | `/api/dataset/{name}/margin` | [x] |
| GET | `/api/dataset/{name}/margin/{code}` | [x] |
| GET | `/api/dataset/{name}/margin/batch` | [x] |
| GET | `/api/dataset/{name}/statements/{code}` | [x] |
| GET | `/api/dataset/{name}/statements/batch` | [x] |
| GET | `/api/dataset/{name}/sectors` | [x] |
| GET | `/api/dataset/{name}/sectors/mapping` | [x] |
| GET | `/api/dataset/{name}/sectors/stock-mapping` | [x] |
| GET | `/api/dataset/{name}/sectors/{sectorName}/stocks` | [x] |

#### Portfolio (12)
| メソッド | パス | 3E |
|---|---|---|
| GET | `/api/portfolio` | [x] |
| POST | `/api/portfolio` | [x] |
| GET | `/api/portfolio/{id}` | [x] |
| PUT | `/api/portfolio/{id}` | [x] |
| DELETE | `/api/portfolio/{id}` | [x] |
| POST | `/api/portfolio/{id}/items` | [x] |
| PUT | `/api/portfolio/{id}/items/{itemId}` | [x] |
| DELETE | `/api/portfolio/{id}/items/{itemId}` | [x] |
| GET | `/api/portfolio/{id}/performance` | [x] |
| GET | `/api/portfolio/{name}/codes` | [x] |
| PUT | `/api/portfolio/{portfolioName}/stocks/{code}` | [x] |
| DELETE | `/api/portfolio/{portfolioName}/stocks/{code}` | [x] |

#### Watchlist (8)
| メソッド | パス | 3E |
|---|---|---|
| GET | `/api/watchlist` | [x] |
| POST | `/api/watchlist` | [x] |
| GET | `/api/watchlist/{id}` | [x] |
| PUT | `/api/watchlist/{id}` | [x] |
| DELETE | `/api/watchlist/{id}` | [x] |
| POST | `/api/watchlist/{id}/items` | [x] |
| DELETE | `/api/watchlist/{id}/items/{itemId}` | [x] |
| GET | `/api/watchlist/{id}/prices` | [x] |

### 主要リスクと対策

| リスク | 対策 |
|---|---|
| Bun/Drizzle 依存の移植コスト | 3C で SQLAlchemy Core により段階的に再実装（完了） |
| SQLite スキーマ互換の破壊 | contracts/ スキーマを正とし、Drizzle との読取互換テスト |
| JQuants API の認証・レート制限差異 | Python JQuants クライアントの事前検証（Phase 2B） |
| 長時間ジョブの安定運用 | ジョブライフサイクルの結合テスト（Phase 3D Go/No-Go） |
| 既存クライアントのレスポンス互換性 | 全エンドポイントのレスポンス互換テスト |

### 成功条件

- Hono を停止しても 90 エンドポイント全てが FastAPI で同一動作
- DB ファイルを共有したままデータ互換が維持
- フロントエンドが API URL 変更なしで動作
- OpenAPI 契約の差分がゼロ

---

## Phase 4: パッケージ分離（再ベースライン）

**期間**: 4-6 週 | **リスク**: Medium | **状態**: 進行中（4A 完了、4C Step1 完了、4B 方針転換済み）  
**再ベースライン日**: 2026-02-09

*元: packages-responsibility-roadmap.md Phase 2-5（Phase 3F 後の実装状態に合わせて再編）*

### 前提条件（2026-02-09 現在）

- FastAPI (:3002) が唯一のバックエンド
- `apps/ts/packages/api` は archived・read-only のため Phase 4 の移行対象外
- `contracts/` が bt/ts 間インターフェースの SoT（`packages/contracts` は任意拡張）
- Phase 4 の目的は「機能追加」ではなく責務再配置（挙動互換維持）

### 目標像: Phase 4 完了時の責務

#### TypeScript 側
| 境界（新規/縮小） | 責務 | 現在の主な移行元 |
|---|---|---|
| `clients-ts` | FastAPI クライアントと generated types の公開境界 | `apps/ts/packages/shared/src/clients` |
| `market-db-ts` | market.db 読み取り API | `apps/ts/packages/shared/src/db` |
| `dataset-db-ts` | dataset.db 読み取り API + snapshot/manifest | `apps/ts/packages/shared/src/dataset` |
| `portfolio-db-ts` | portfolio/watchlist DB 操作 | `apps/ts/packages/shared/src/portfolio`, `watchlist` |
| `analytics-ts` | **作成しない**（FastAPI 一本化後のため TS ドメイン実装は削除対象） | `apps/ts/packages/shared/src/factor-regression`, `screening` |
| `market-sync-ts` | **作成しない**（FastAPI 一本化後のため TS ドメイン実装は削除対象） | `apps/ts/packages/shared/src/market-sync` |
| `shared`（縮小） | 互換 re-export と `bt:sync` 関連スクリプト | `apps/ts/packages/shared` |

#### Python 側
| 境界（論理パッケージ） | 責務 | 現在の主な移行元 |
|---|---|---|
| `market-db-py` | market.db / dataset.db / portfolio.db のアクセス境界 | `apps/bt/src/server/db` |
| `dataset-io-py` | snapshot/manifest・dataset 書き込み補助 | `apps/bt/src/server/db/dataset_writer.py`, `apps/bt/src/server/services/dataset_builder_service.py` |
| `indicators-py` | indicator 計算のコアロジック | `apps/bt/src/utils/indicators.py`, `apps/bt/src/server/services` |
| `backtest-core` | backtest 実行エンジン | `apps/bt/src/backtest`, `apps/bt/src/strategies` |
| `strategy-runtime` | strategy config 読み取りと実行 | `apps/bt/src/strategy_config`, `apps/bt/src/strategies` |

> 実装初期は Python 側を `apps/bt/src/lib/*` で分離し、外部配布形式（別 repo/package 化）は Phase 4 完了後に判断する。

### 4A: TS データアクセス + クライアント分離

*元: packages-responsibility-roadmap.md Phase 2（再編）*

- [x] `apps/ts/packages/clients-ts`, `market-db-ts`, `dataset-db-ts`, `portfolio-db-ts` を作成
- [x] `apps/ts/packages/shared/src/db`, `dataset`, `portfolio`, `watchlist`, `clients` から段階移管（実装本体を新パッケージへ移動）
- [x] `apps/ts/packages/web` と `apps/ts/packages/cli` の import を新パッケージへ切替（backtest + dataset/portfolio/watchlist）
- [x] `apps/ts/packages/shared` に互換 re-export を一時配置して breaking change を抑制（`db/dataset/portfolio/watchlist/clients`）

**完了**: 2026-02-09（4A）

**完了条件**:
- `apps/ts/packages/web` と `apps/ts/packages/cli` が `shared/src/*` の深いパスを直接参照しない
- `apps/ts/packages/shared` に DB/HTTP の実装本体を残さない

### 4B: TS ドメインロジック削減（削除中心）

*元: packages-responsibility-roadmap.md Phase 3（再編）*

- [ ] `apps/ts/packages/shared/src/factor-regression`, `screening`, `market-sync` の実装本体を段階削除
- [ ] `apps/ts/packages/web` と `apps/ts/packages/cli` のローカル計算依存を撤去し、FastAPI endpoint + OpenAPI generated types に統一
- [ ] `apps/ts/packages/shared` を「互換 re-export + bt:sync 補助 + 型ファサード」に縮小（`analytics-ts` / `market-sync-ts` は新設しない）

**完了条件**:
- `apps/ts/packages/shared` が再エクスポート/型公開中心の薄いファサードになる
- `apps/ts/packages/web` と `apps/ts/packages/cli` が TS 内の重複ドメイン実装を参照せず、FastAPI を唯一の実行ロジックとして利用する

### 4C: Python ドメインパッケージ分離

*元: packages-responsibility-roadmap.md Phase 4（再編）*

- [x] `apps/bt/src/lib/market_db`, `dataset_io` を作成し `src/server/db` と dataset I/O を再配置（2026-02-09, Step1）
- [ ] `apps/bt/src/lib/indicators`, `backtest_core`, `strategy_runtime` を作成し責務を明確化
- [x] `apps/bt/src/server` と `apps/bt/src/cli_*` の import 先を新境界へ切替（2026-02-09, Step1: server 側完了）
- [x] 既存 API/CLI 挙動との互換回帰テストを維持（2026-02-09, Step1 範囲検証）

**進捗**:
- 2026-02-09: Step1（DB + dataset I/O 分離）完了。`src/server/db` は互換 re-export を維持しつつ、実装本体を `src/lib/*` へ移管。

**完了条件**:
- `apps/bt/src/server/routes` / `services` / `cli_*` が legacy 実装に直接依存しない
- DB アクセス層が `market_db` 境界に集約される

### 4D: 互換レイヤ整理 + CI 段階実行

*元: packages-responsibility-roadmap.md Phase 5（再編）*

- [ ] 一時的な互換 re-export を段階削除
- [ ] `apps/ts/packages/shared` と `apps/bt/src` の重複実装を削除
- [ ] CI を「パッケージ単体テスト」と「apps 結合テスト」に段階化
- [ ] `scripts/check-dep-direction.sh` の allowlist と docs を新境界へ更新

**完了条件**:
- apps 配下に残るのは entrypoint + thin adapter
- CI が分離後の責務境界を検証できる状態になる

### Phase 4 関連 Issue（2026-02-09 再整理）

- [x] `ts-117`: archived API package 前提のためクローズ（`../issues/done/ts-117-coverage-api-75-65.md`）
- [ ] `ts-124`: TS 側責務分離トラッキング（`../issues/ts-124-phase4-ts-package-separation.md`）
- [ ] `bt-026`: Python 側責務分離トラッキング（`../issues/bt-026-phase4-python-domain-package-split.md`）

---

## Phase 5: シグナル・分析拡張

**期間**: 2-3 週 | **リスク**: Low

*元: plan-ta-consolidation.md Phase 4*

> **Note**: Phase 4 と独立して実行可能

### 5A: Signal Overlay API

- [ ] `POST /api/indicators/signals` エンドポイント構築
- [ ] boolean 配列 + トリガー日付リストのレスポンス定義
- [ ] 34 シグナル対応

### 5B: Web UI シグナルマーカー

- [ ] チャート上にシグナル発火点をマーカー表示（▲/▼アイコン）
- [ ] バックテスト戦略のエントリー/エグジットポイントとの連動

### 5C: 新規インジケータ追加

| 優先度 | インジケータ | 既存シグナル |
|---|---|---|
| 高 | セクターローテーション (RRG) | `sector_rotation_phase_signal` |
| 高 | ベータ係数 | `beta_range_signal` |
| 中 | Fibonacci Retracement | `retracement_signal` |
| 中 | Mean Reversion bands | `mean_reversion_combined_signal` |
| 低 | 信用残パーセンタイル | `margin_balance_percentile_signal` |

---

## 依存関係マップ

```
Phase 1 (基盤安定化)
  └──→ Phase 2 (契約・データ境界)
         ├── 2A ──→ Phase 3 (契約スキーマが移行の安全網)
         └── 2C (ADR-003) ──→ 3C (DB管理責務変更が前提)

Phase 3 (FastAPI統一)
  ├── 3A ──→ 3B, 3C
  ├── 3B ‖ 3C (並行可能)
  ├── 3C ──→ 3D (DB操作は直接アクセスが前提)
  ├── 3D ──→ 3E
  └── 3A-3E ──→ 3F (全エンドポイント完了後に切替)

Phase 3 ──→ Phase 4 (API安定後にパッケージ分離)

Phase 4 ‖ Phase 5 (独立して実行可能)
```

---

## Appendix A: SQLite vs Parquet 決定記録

**結論: SQLite 維持（Parquet 移行しない）**

### 理由

1. **データ量が不足**: ~1M 行の OHLCV データは SQLite の性能範囲内
2. **アクセスパターンが行指向**: 銘柄コード単位の OHLCV 取得（列指向の Parquet の利点なし）
3. **CRUD 要件**: portfolio/watchlist に ACID トランザクションが必要（Parquet は追記のみ）
4. **既存契約**: `contracts/dataset-db-schema-v2.json` と Drizzle スキーマが成熟
5. **単一ユーザーシステム**: WAL モードの並行読み取りで十分
6. **移行リスク**: Hono→FastAPI 移行に加えてストレージ移行は過剰

### 将来の拡張オプション

- **SQLite = System of Record（正）** を維持
- 分析用途のみ **Parquet/DuckDB sidecar** を将来的に検討可能
- 検討条件: 10M 行超（インマーケット/ティックデータ）、全銘柄横断ファクター分析

### FastAPI 移行時の DB 実装計画（Phase 3C で実装完了）

- **SQLAlchemy Core**（ORM なし）— クエリビルダ + スキーマ定義、Session 管理不要
- `tables.py` で 17 テーブルを Python コードで定義（Drizzle スキーマと自動照合）
- `contracts/` JSON（market-db-schema-v1, dataset-db-schema-v2, portfolio-db-schema-v1）を正として整合性検証
- WAL モード + foreign_keys は `event.listens_for(engine, "connect")` で接続ごとに設定
- StaticPool + `check_same_thread=False` で FastAPI 非同期環境に対応

---

## Appendix B: オープン Issue マップ

### bt 系

| Issue | 概要 | 関連 Phase | 状態 |
|---|---|---|---|
| bt-016 | テストカバレッジ 70% | 1C | **完了** |
| bt-017 | signal registry param key validation | 1C | **完了** |
| bt-018 | pyright pandas type errors | 1C | **完了** |
| bt-019 | resample compatibility test todo | 1C | **完了** |
| bt-020 | pydantic field example deprecation | 1C | **完了** |

### ts 系

| Issue | 概要 | 関連 Phase |
|---|---|---|
| ts-003 | API auth | — |
| ts-006 | error logging | — |
| ts-011 | ops runbook | — |
| ts-012 | logging metrics | — |
| ts-013 | secret management | — |
| ts-014c | coverage cli 90/90 | — |
| ts-104 | bt HTML sandbox | 5B |
| ts-105 | bt UI progress | 5B |
| ts-106 | bt run params | 5A |
| ts-108 | bt export | — |
| ts-110 | bt env config | — |
| ts-111 | coverage shared 80/80 | — |
| ts-113 | coverage cli 70/70 | — |
| ts-114 | coverage web 45/70 | — |
| ts-116 | bt optimization HTML schema | 2A | **完了** |
| ts-117 | coverage api 75/65 | — |
| ts-118 | fundamentals integration test | 3B |
| ts-119 | lab result runtime validation | — |
| ts-120 | lab results error boundary | — |
| ts-121 | market endpoint scope clarify | 1C | **完了** |
| ts-122 | screening logic single source | 1C | **完了** |
| ts-123 | remove deprecated fundamentals data service | 1C | **完了** |

---

## Appendix C: トレーサビリティ表

各作業項目がどの元ドキュメントから来たかの追跡表。

| 統一ロードマップの項目 | 元ドキュメント | 元の該当セクション |
|---|---|---|
| **Decision Log** | | |
| ADR-001 | monorepo-integration.md | 決定事項 + Claude の見解 |
| ADR-002 | （新規決定） | — |
| ADR-003 | hono-to-fastapi-migration-roadmap.md | 循環依存の解消プラン 案A |
| **Phase 1** | | |
| 1A: TA 回帰監視 | plan-ta-consolidation.md | Phase 2.5 |
| 1B: 依存方向監査 | monorepo-migration-plan.md | Phase 3 |
| 1C: Issue 対応 | 各ドキュメント | — |
| **Phase 2** | | |
| 2A: 契約スキーマ | monorepo-migration-plan.md + packages-responsibility-roadmap.md | Phase 2 + Phase 0-1 |
| 2B: FastAPI 事前調査 | hono-to-fastapi-migration-roadmap.md | Phase 0 |
| 2C: ADR-003 | （新規） | — |
| **Phase 3** | | |
| 3A: ミドルウェア | hono-to-fastapi-migration-roadmap.md | Phase 1 |
| 3B: 読み取り API | hono-to-fastapi-migration-roadmap.md | Phase 2 |
| 3C: SQLite アクセス層 | hono-to-fastapi-migration-roadmap.md | 案A + ADR-003（DB 管理責務移行） |
| 3D: DB・ジョブ API | hono-to-fastapi-migration-roadmap.md | Phase 3 |
| 3E: CRUD | hono-to-fastapi-migration-roadmap.md | Phase 4 |
| 3F: 切替・廃止 | hono-to-fastapi-migration-roadmap.md | Phase 5 |
| **Phase 4** | | |
| 4A: TS データアクセス | packages-responsibility-roadmap.md | Phase 2 |
| 4B: TS ドメインロジック削減 | packages-responsibility-roadmap.md | Phase 3 |
| 4C: Python パッケージ | packages-responsibility-roadmap.md | Phase 4 |
| 4D: クリーンアップ | packages-responsibility-roadmap.md | Phase 5 |
| **Phase 5** | | |
| 5A: Signal Overlay API | plan-ta-consolidation.md | Phase 4 (4-1) |
| 5B: Web UI マーカー | plan-ta-consolidation.md | Phase 4 (4-2) |
| 5C: 新規インジケータ | plan-ta-consolidation.md | Phase 4 (4-3) |
| **Appendix** | | |
| Appendix A | monorepo-integration.md + 新規決定 | dataset スキーマ問題 + SQLite vs Parquet 評価 |

---

## Appendix D: アーカイブ元ドキュメント

統合元の 5 ドキュメントは `docs/archive/` に移動済み:

> Note (2026-02-09): 下表の「状態（統合時点）」はアーカイブ取り込み時のスナップショット。現況は本文のステータスダッシュボードと各 Phase 節を参照。

| ファイル | 内容 | 状態（統合時点） |
|---|---|---|
| `archive/monorepo-integration.md` | モノレポ統合・API 責務分離の検討メモ | 決定事項のみ（完了） |
| `archive/monorepo-migration-plan.md` | Monorepo Migration Plan | Phase 1 完了、2-4 未着手 |
| `archive/hono-to-fastapi-migration-roadmap.md` | Hono→FastAPI 完全一本化ロードマップ | 全 Phase 未着手 |
| `archive/packages-responsibility-roadmap.md` | packages/ 責務分割ロードマップ | 全 Phase 未着手 |
| `archive/plan-ta-consolidation.md` | TA 計算エンジン統合計画 | Phase 0-3.5 完了、4 未着手 |

### 独立レポート（アーカイブ対象外）

- `reports/phase2_5_verification_report.md` — TA 並走検証レポート（Phase 2.5 の成果物）
