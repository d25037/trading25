# Phase 2B: エンドポイント監査レポート

作成日: 2026-02-06
目的: Phase 3（FastAPI 統一）に向けた Hono/FastAPI エンドポイント全数調査

---

## サマリー

| 項目 | Hono (ts/api) | FastAPI (bt/server) |
|------|--------------|---------------------|
| エンドポイント総数 | 90 | 41 |
| レスポンスモデル定義率 | 100% (OpenAPI) | 95% (39/41) |
| 重複パス | 1 (`/api/health` vs `/health`) | |
| SSE ストリーム | 0 | 3 |

### 型カバレッジの課題

FastAPI 側で未型定義のエンドポイント:
- `GET /api/signals/schema` — `dict[str, Any]` を返却（JSON Schema なので許容）
- SSE ストリーム 3 エンドポイント — `EventSourceResponse` は OpenAPI 対象外

---

## Hono エンドポイント一覧（90）

### Health (1)

| Method | Path | Phase 3 |
|--------|------|---------|
| GET | `/health` | 3B |

### JQuants Proxy (7)

| Method | Path | Phase 3 |
|--------|------|---------|
| GET | `/api/jquants/auth/status` | 3B |
| GET | `/api/jquants/daily-quotes` | 3B |
| GET | `/api/jquants/indices` | 3B |
| GET | `/api/jquants/listed-info` | 3B |
| GET | `/api/jquants/statements` | 3B |
| GET | `/api/jquants/stocks/{symbol}/margin-interest` | 3B |
| GET | `/api/jquants/topix` | 3B |

### Chart (5)

| Method | Path | Phase 3 |
|--------|------|---------|
| GET | `/api/chart/indices` | 3B |
| GET | `/api/chart/indices/topix` | 3B |
| GET | `/api/chart/indices/{code}` | 3B |
| GET | `/api/chart/stocks/search` | 3B |
| GET | `/api/chart/stocks/{symbol}` | 3B |

### Analytics (9)

| Method | Path | Phase 3 |
|--------|------|---------|
| GET | `/api/analytics/factor-regression/{symbol}` | 3B |
| GET | `/api/analytics/fundamentals/{symbol}` | 3B |
| GET | `/api/analytics/portfolio-factor-regression/{portfolioId}` | 3B |
| GET | `/api/analytics/ranking` | 3B |
| GET | `/api/analytics/roe` | 3B |
| GET | `/api/analytics/screening` | 3B |
| GET | `/api/analytics/sector-stocks` | 3B |
| GET | `/api/analytics/stocks/{symbol}/margin-pressure` | 3B |
| GET | `/api/analytics/stocks/{symbol}/margin-ratio` | 3B |

### Market Data (3)

| Method | Path | Phase 3 |
|--------|------|---------|
| GET | `/api/market/stocks` | 3B |
| GET | `/api/market/stocks/{code}/ohlcv` | 3B |
| GET | `/api/market/topix` | 3B |

### Database (6)

| Method | Path | Phase 3 |
|--------|------|---------|
| GET | `/api/db/stats` | 3D |
| POST | `/api/db/stocks/refresh` | 3D |
| POST | `/api/db/sync` | 3D |
| GET | `/api/db/sync/jobs/{jobId}` | 3D |
| DELETE | `/api/db/sync/jobs/{jobId}` | 3D |
| GET | `/api/db/validate` | 3D |

### Dataset Management (9)

| Method | Path | Phase 3 |
|--------|------|---------|
| GET | `/api/dataset` | 3D |
| POST | `/api/dataset` | 3D |
| POST | `/api/dataset/resume` | 3D |
| GET | `/api/dataset/jobs/{jobId}` | 3D |
| DELETE | `/api/dataset/jobs/{jobId}` | 3D |
| GET | `/api/dataset/{name}/info` | 3D |
| GET | `/api/dataset/{name}/sample` | 3D |
| GET | `/api/dataset/{name}/search` | 3D |
| DELETE | `/api/dataset/{name}` | 3D |

### Dataset Data (15)

| Method | Path | Phase 3 |
|--------|------|---------|
| GET | `/api/dataset/{name}/stocks` | 3D |
| GET | `/api/dataset/{name}/stocks/{code}/ohlcv` | 3D |
| GET | `/api/dataset/{name}/stocks/ohlcv/batch` | 3D |
| GET | `/api/dataset/{name}/topix` | 3D |
| GET | `/api/dataset/{name}/indices` | 3D |
| GET | `/api/dataset/{name}/indices/{code}` | 3D |
| GET | `/api/dataset/{name}/margin` | 3D |
| GET | `/api/dataset/{name}/margin/{code}` | 3D |
| GET | `/api/dataset/{name}/margin/batch` | 3D |
| GET | `/api/dataset/{name}/statements/{code}` | 3D |
| GET | `/api/dataset/{name}/statements/batch` | 3D |
| GET | `/api/dataset/{name}/sectors` | 3D |
| GET | `/api/dataset/{name}/sectors/mapping` | 3D |
| GET | `/api/dataset/{name}/sectors/stock-mapping` | 3D |
| GET | `/api/dataset/{name}/sectors/{sectorName}/stocks` | 3D |

### Portfolio (12)

| Method | Path | Phase 3 |
|--------|------|---------|
| GET | `/api/portfolio` | 3E |
| POST | `/api/portfolio` | 3E |
| GET | `/api/portfolio/{id}` | 3E |
| PUT | `/api/portfolio/{id}` | 3E |
| DELETE | `/api/portfolio/{id}` | 3E |
| POST | `/api/portfolio/{id}/items` | 3E |
| PUT | `/api/portfolio/{id}/items/{itemId}` | 3E |
| DELETE | `/api/portfolio/{id}/items/{itemId}` | 3E |
| GET | `/api/portfolio/{id}/performance` | 3E |
| GET | `/api/portfolio/{name}/codes` | 3E |
| PUT | `/api/portfolio/{portfolioName}/stocks/{code}` | 3E |
| DELETE | `/api/portfolio/{portfolioName}/stocks/{code}` | 3E |

### Watchlist (8)

| Method | Path | Phase 3 |
|--------|------|---------|
| GET | `/api/watchlist` | 3E |
| POST | `/api/watchlist` | 3E |
| GET | `/api/watchlist/{id}` | 3E |
| PUT | `/api/watchlist/{id}` | 3E |
| DELETE | `/api/watchlist/{id}` | 3E |
| POST | `/api/watchlist/{id}/items` | 3E |
| DELETE | `/api/watchlist/{id}/items/{itemId}` | 3E |
| GET | `/api/watchlist/{id}/prices` | 3E |

---

## FastAPI エンドポイント一覧（41）

### Health (1)

| Method | Path | Response Model |
|--------|------|---------------|
| GET | `/api/health` | `HealthResponse` |

### Backtest (10)

| Method | Path | Response Model |
|--------|------|---------------|
| POST | `/api/backtest/run` | `BacktestJobResponse` |
| GET | `/api/backtest/jobs/{job_id}` | `BacktestJobResponse` |
| GET | `/api/backtest/jobs` | `list[BacktestJobResponse]` |
| POST | `/api/backtest/jobs/{job_id}/cancel` | `BacktestJobResponse` |
| GET | `/api/backtest/result/{job_id}` | `BacktestResultResponse` |
| GET | `/api/backtest/html-files` | `HtmlFileListResponse` |
| GET | `/api/backtest/html-files/{strategy}/{filename}` | `HtmlFileContentResponse` |
| POST | `/api/backtest/html-files/{strategy}/{filename}/rename` | `HtmlFileRenameResponse` |
| DELETE | `/api/backtest/html-files/{strategy}/{filename}` | `HtmlFileDeleteResponse` |
| GET | `/api/backtest/jobs/{job_id}/stream` | `EventSourceResponse` (SSE) |

### Strategies (9)

| Method | Path | Response Model |
|--------|------|---------------|
| GET | `/api/strategies` | `StrategyListResponse` |
| GET | `/api/strategies/{strategy_name}` | `StrategyDetailResponse` |
| POST | `/api/strategies/{strategy_name}/validate` | `StrategyValidationResponse` |
| PUT | `/api/strategies/{strategy_name}` | `StrategyUpdateResponse` |
| DELETE | `/api/strategies/{strategy_name}` | `StrategyDeleteResponse` |
| POST | `/api/strategies/{strategy_name}/duplicate` | `StrategyDuplicateResponse` |
| POST | `/api/strategies/{strategy_name}/rename` | `StrategyRenameResponse` |
| GET | `/api/config/default` | `DefaultConfigResponse` |
| PUT | `/api/config/default` | `DefaultConfigUpdateResponse` |

### Optimization (11)

| Method | Path | Response Model |
|--------|------|---------------|
| POST | `/api/optimize/run` | `OptimizationJobResponse` |
| GET | `/api/optimize/jobs/{job_id}` | `OptimizationJobResponse` |
| GET | `/api/optimize/jobs/{job_id}/stream` | `EventSourceResponse` (SSE) |
| GET | `/api/optimize/grid-configs` | `OptimizationGridListResponse` |
| GET | `/api/optimize/grid-configs/{strategy}` | `OptimizationGridConfig` |
| PUT | `/api/optimize/grid-configs/{strategy}` | `OptimizationGridSaveResponse` |
| DELETE | `/api/optimize/grid-configs/{strategy}` | `OptimizationGridDeleteResponse` |
| GET | `/api/optimize/html-files` | `OptimizationHtmlFileListResponse` |
| GET | `/api/optimize/html-files/{strategy}/{filename}` | `OptimizationHtmlFileContentResponse` |
| POST | `/api/optimize/html-files/{strategy}/{filename}/rename` | `HtmlFileRenameResponse` |
| DELETE | `/api/optimize/html-files/{strategy}/{filename}` | `HtmlFileDeleteResponse` |

### Signal Reference (3)

| Method | Path | Response Model |
|--------|------|---------------|
| GET | `/api/signals/reference` | `SignalReferenceResponse` |
| GET | `/api/signals/schema` | `dict[str, Any]` |
| POST | `/api/signals/compute` | `SignalComputeResponse` |

### Lab (7)

| Method | Path | Response Model |
|--------|------|---------------|
| POST | `/api/lab/generate` | `LabJobResponse` |
| POST | `/api/lab/evolve` | `LabJobResponse` |
| POST | `/api/lab/optimize` | `LabJobResponse` |
| POST | `/api/lab/improve` | `LabJobResponse` |
| GET | `/api/lab/jobs/{job_id}` | `LabJobResponse` |
| GET | `/api/lab/jobs/{job_id}/stream` | `EventSourceResponse` (SSE) |
| POST | `/api/lab/jobs/{job_id}/cancel` | `LabJobResponse` |

### OHLCV (1)

| Method | Path | Response Model |
|--------|------|---------------|
| POST | `/api/ohlcv/resample` | `OHLCVResampleResponse` |

### Indicators (2)

| Method | Path | Response Model |
|--------|------|---------------|
| POST | `/api/indicators/compute` | `IndicatorComputeResponse` |
| POST | `/api/indicators/margin` | `MarginIndicatorResponse` |

### Fundamentals (1)

| Method | Path | Response Model |
|--------|------|---------------|
| POST | `/api/fundamentals/compute` | `FundamentalsComputeResponse` |

---

## 重複・競合分析

### パス重複

| Hono Path | FastAPI Path | 競合度 | 対応方針 |
|-----------|-------------|--------|---------|
| `GET /health` | `GET /api/health` | Low | パス異なる（`/health` vs `/api/health`）。Phase 3A で統一 |

### 機能重複（異なるパス）

FastAPI の backtest/optimize/strategies/lab/signals/indicators/ohlcv/fundamentals エンドポイントは Hono には存在しない。
これらは Phase 3 移行対象ではなく、**そのまま維持**される。

### 移行対象の明確化

Phase 3 で移行が必要なのは **Hono 固有の 90 エンドポイント**のうち、FastAPI に既存のもの（1: health）を除く **89 エンドポイント**。

---

## Phase 3 サブフェーズ割り当て確認

| Phase | カテゴリ | エンドポイント数 | ロードマップ一致 |
|-------|---------|----------------|----------------|
| 3B | Health, Chart, Market, Analytics, JQuants Proxy | 25 | OK |
| 3D | Database, Dataset Management, Dataset Data | 30 | OK |
| 3E | Portfolio, Watchlist | 20 | OK |
| — | bt 固有（Backtest, Strategy, Optimize, Lab, Signals, Indicators, OHLCV, Fundamentals） | 41 | 移行不要（既に FastAPI） |
| **合計** | | **90 + 41** | |

> **Note**: ロードマップでは Hono エンドポイントを「75+」と記載していたが、実際は **90** エンドポイント（JQuants Proxy 内の追加エンドポイント、Dataset Data の batch/sectors 系が想定より多い）。Phase 3 の工数見積りを再確認する必要あり。

---

## FastAPI レスポンスモデル網羅性

| ステータス | 件数 | 備考 |
|-----------|------|------|
| Pydantic モデル定義済み | 38 | OpenAPI スキーマに反映 |
| SSE ストリーム（untyped） | 3 | OpenAPI 対象外（設計上正常） |
| dict 返却 | 0 | ts-116 で解消済み |

**結論**: FastAPI の OpenAPI スキーマ網羅性は十分。`GET /api/signals/schema` は JSON Schema を返すため dict が妥当。

---

## 推奨事項

1. **ロードマップのエンドポイント数を修正**: 75+ → 90 に更新
2. **Phase 3 工数再見積り**: Dataset Data (15) と Dataset Management (9) で計 24 エンドポイントがあり、3D の工数が最大
3. **エラーフォーマット統一**: Wave 3 で対応予定（本レポートの FastAPI エラー形式を入力として使用）
