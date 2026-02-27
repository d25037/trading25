# trading25 Greenfield Architecture Blueprint

作成日: 2026-02-27
実装チェックリスト: `docs/greenfield-implementation-checklist.md`

## 0. 結論（最強構成の要点）

- **FastAPI を唯一の backend** とする方針は維持（API SoT の一本化）。
- **データ基盤は二層化**:
  - 取引・設定・ジョブ管理: SQLite（将来 Postgres へ差し替え可能）
  - 大容量時系列・分析基盤: DuckDB + Parquet（列指向で高速）
- **契約駆動開発を中核化**:
  - OpenAPI/JSON Schema を SoT にして ts 側型を自動生成
  - Contract test を CI の必須ゲートにする
- **ジョブ実行は「API層」と「Worker層」を分離**:
  - API は orchestration のみ
  - screening/backtest/optimize/dataset build は worker が実行
- **ドメインロジックは `src/domains/*` に集中**し、entrypoint/application は I/O と調停に限定。

---

## 1. 設計原則

1. **Single Source of Truth**
   - API 契約: OpenAPI
   - ドメイン計算: `src/domains/*`
   - データ定義: `contracts/` + DB schema migration
2. **Local-first, Cloud-ready**
   - 単体開発はローカルで完結
   - 必要になった時だけ Postgres/Redis/オブジェクトストレージへ拡張
3. **Immutable data products**
   - dataset snapshot は不変成果物（manifest + checksum 付き）
4. **Idempotent jobs**
   - 同一パラメータ再実行で壊れない
   - cancel/retry/resume を前提に job state machine を設計
5. **Observability by default**
   - correlation ID、構造化ログ、メトリクス、ジョブトレースを標準化

---

## 2. 推奨システム構成（ゼロベース）

```text
                    ┌──────────────────────────────┐
                    │          ts/web (Vite)       │
                    │      ts/cli (Gunshi)         │
                    └──────────────┬───────────────┘
                                   │ /api
                          ┌────────▼────────┐
                          │   FastAPI API   │  :3002
                          │  (orchestrator) │
                          └───────┬─────────┘
                                  │ enqueue/query
                        ┌─────────▼──────────┐
                        │   Worker Runtime    │
                        │ screening/backtest  │
                        │ optimize/dataset    │
                        └─────────┬──────────┘
                                  │
        ┌─────────────────────────┼──────────────────────────┐
        │                         │                          │
┌───────▼────────┐      ┌─────────▼──────────┐      ┌────────▼─────────┐
│ SQLite (OLTP)  │      │ DuckDB + Parquet   │      │ Artifacts Storage │
│ portfolio/jobs │      │ market/features    │      │ html/json/report  │
└────────────────┘      └────────────────────┘      └──────────────────┘
                                  │
                           ┌──────▼───────┐
                           │ J-Quants API │
                           └──────────────┘
```

### なぜこの構成か

- 既存要件（FastAPI 一本化、ts は API consumer）と整合しつつ、時系列分析の性能を最大化できる。
- SQLite 単独より、時系列分析を DuckDB に逃がした方が backtest/screening の再現性と速度が出る。
- worker 分離で API 応答性能と長時間計算を切り離せる。

---

## 3. リポジトリ構成（推奨）

```text
apps/
  bt/
    src/
      domains/                 # 計算ロジック SoT
        analytics/
        fundamentals/
        strategy/
        backtest/
      application/             # use-case orchestration
      infrastructure/          # DB, J-Quants, queue, filesystem adapters
      entrypoints/
        http/                  # FastAPI routers/middleware/openapi
        worker/                # job handlers
        cli/                   # Typer commands
    migrations/                # schema migrations
    tests/
      unit/
      integration/
      contract/
  ts/
    packages/
      web/                     # React/Vite UI
      cli/                     # Gunshi CLI
      shared/                  # OpenAPI generated types + shared utils
      api-clients/             # typed clients
contracts/
  openapi/
  schemas/
docs/
issues/
```

---

## 4. データ設計（最重要）

## 4.1 ストレージ責務分離

- `portfolio.db`（SQLite）:
  - portfolio/watchlist/settings/jobs metadata
- `market`（DuckDB + Parquet）:
  - `stock_data`, `topix_data`, `indices_data`, `statements`, feature tables
- `datasets/`（immutable snapshot）:
  - `manifest.json`（schema version / row counts / checksums / coverage）

## 4.2 取り込みパイプライン

1. `raw fetch`（J-Quants）
2. `normalize`（型・タイムゾーン・市場コード同義語を統一）
3. `validate`（NULL/PK/FK/coverage）
4. `publish`（DuckDB/Parquet + snapshot manifest）
5. `index`（latest pointers と統計情報）

## 4.3 データ品質ゲート

- statements upsert は **非NULL優先 merge**（既存方針を踏襲）
- 欠損 OHLCV は row skip + warning 集約
- `GET /api/dataset/{name}/info` は `snapshot + stats + validation` を SoT 化

---

## 5. API/契約設計

## 5.1 API 方針

- `/api/*` は FastAPI に統一
- 長時間処理は async job API（create/status/cancel/result）を標準
- docs UI は `/doc` のみ公開

## 5.2 Error/Tracing 標準

- 統一エラー形式:
  - `status/error/message/details/timestamp/correlationId`
- `x-correlation-id` を全リクエストで伝播
- middleware 順序は固定（RequestLogger → CorrelationId → CORS）

## 5.3 Contract governance

- OpenAPI 変更時は `bt:sync` を CI 必須化
- additive/breaking を schema version で厳密管理
- ts 生成型と backend schema の差分を PR で可視化

---

## 6. 実行基盤（ジョブ）

## 6.1 Job model

- state: `queued -> running -> succeeded|failed|cancelled`
- すべての job に `timeoutMinutes`, `attempt`, `startedAt`, `finishedAt`, `correlationId`
- result は artifact-first（HTML/JSON）で再解決可能にする

## 6.2 並列化方針

- ingestion は code/day partition 単位で分割
- backtest optimize は trial 単位分散 + early pruning
- CPU-bound 処理は process pool で実行（GIL回避）

---

## 7. フロントエンド/CLI

## 7.1 ts/web

- API state は TanStack Query に集約
- job progress は polling から **SSE 優先**へ（fallback polling）
- heavy table/chart は virtualization を標準化

## 7.2 ts/cli

- web と同じ typed client を使用（重複実装禁止）
- `--wait` / `--json` / `--output` を共通UXに統一
- 非同期ジョブを first-class に扱う（job id で追跡可能）

---

## 8. テスト/品質ゲート

- Unit: domain ロジック中心（純粋関数優先）
- Integration: DB + API + worker 協調
- Contract: OpenAPI と client 生成型の一致検証
- Golden dataset: 回帰検出用固定 fixture
- Performance budget:
  - screening p95
  - backtest median runtime
  - dataset build throughput

CI 必須ジョブ:

1. lint
2. typecheck
3. contract sync check
4. unit/integration tests
5. web e2e smoke
6. coverage gate

---

## 9. セキュリティ/運用

- secret は `.env` ではなく keychain/secret manager 優先（開発時のみ `.env`）
- SQL は SQLAlchemy Core + bind parameters を強制
- 監査ログ: dataset build / strategy update / backtest run を記録
- 障害時 runbook を docs 化（API, DB, J-Quants rate limit, job stuck）

---

## 10. 90日実装ロードマップ（ゼロベース想定）

### Day 1-30: Foundation

- FastAPI skeleton + middleware + error standard
- OpenAPI contract pipeline + ts 型生成
- SQLite jobs/portfolio + DuckDB market baseline
- dataset snapshot manifest v1

### Day 31-60: Core Features

- screening/backtest async jobs
- fundamentals ranking/signal system v1
- web Analysis/Backtest minimal UI
- CLI end-to-end job workflows

### Day 61-90: Hardening

- optimization/pruning + artifacts
- observability（structured logs + metrics）
- e2e/performance/security gate
- migration tools（schema/data backfill）

---

## 11. 追加でやらないこと（意図的な非採用）

- マイクロサービス分割（時期尚早）
- 複数バックエンド併存（SoT 崩壊）
- frontend 独自の計算ロジック肥大化（契約不整合を招く）
- mutable dataset snapshot（再現性崩壊）

---

## 12. この案の判定基準（Done Definition）

- API 変更が OpenAPI diff なしにマージされない
- 同一戦略・同一期間で backtest が再現可能
- dataset build が resume 可能で、validation が常時取得可能
- web/cli が同一レスポンス契約を使い破綻しない
- 主要ジョブで `correlationId` から追跡可能
