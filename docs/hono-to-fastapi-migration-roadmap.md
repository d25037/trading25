# Hono -> FastAPI 完全一本化ロードマップ

作成日: 2026-02-05

## 目的
- `apps/ts/packages/api` の Hono サーバー機能を `apps/bt` の FastAPI に移植し、バックエンドを単一サーバーに統合する
- Hono サーバーの運用を終了し、機能・API互換性を維持しつつ移行する

## 現状: Hono サーバーの責務まとめ

### 実行環境・横断機能
- Runtime: Bun + Hono + @hono/zod-openapi
- OpenAPI: `apps/ts/packages/api/openapi.json` / `openapi.yaml` を生成
- Docs UI: `/doc` (Scalar)
- CORS: 開発時に `http://localhost:5173` 等を許可
- ミドルウェア: request logger, correlation id, 統一エラーレスポンス
- Production: `../dist/client` の静的配信 (SPA fallback あり)
- ポート: 3001 (デフォルト)

### データソースとストレージ
- SQLite (Bun: `bun:sqlite`) + Drizzle ORM
- market.db: XDG データディレクトリ配下 `~/.local/share/trading25/market.db`
- portfolio.db (watchlist 兼用): `~/.local/share/trading25/portfolio.db`
- datasets: `~/.local/share/trading25/datasets/*.db`

### 外部連携
- JQuants API v2 (API key)
- `apps/bt` FastAPI へのプロキシ: ファンダメンタル分析は `BacktestClient` 経由で呼び出し

## Hono API エンドポイント一覧 (OpenAPI 生成物より)

### Health
- GET `/health` — Health check

### JQuants Proxy
- GET `/api/jquants/auth/status` — Get JQuants API v2 authentication status
- GET `/api/jquants/daily-quotes` — Get daily stock quotes (raw JQuants format)
- GET `/api/jquants/indices` — Get index data
- GET `/api/jquants/listed-info` — Get listed stock information
- GET `/api/jquants/statements` — Get financial statements (raw JQuants format)
- GET `/api/jquants/stocks/{symbol}/margin-interest` — Get weekly margin interest data
- GET `/api/jquants/topix` — Get TOPIX index data (raw)

### Chart
- GET `/api/chart/indices` — Get list of available indices
- GET `/api/chart/indices/topix` — Get TOPIX index data (cached)
- GET `/api/chart/indices/{code}` — Get index OHLC data
- GET `/api/chart/stocks/search` — Search stocks by code or company name
- GET `/api/chart/stocks/{symbol}` — Get stock chart data

### Analytics
- GET `/api/analytics/factor-regression/{symbol}` — Analyze stock risk factors via OLS regression
- GET `/api/analytics/fundamentals/{symbol}` — Get fundamental analysis metrics for a stock (bt FastAPI へプロキシ)
- GET `/api/analytics/portfolio-factor-regression/{portfolioId}` — Analyze portfolio risk factors via OLS regression
- GET `/api/analytics/ranking` — Get market rankings
- GET `/api/analytics/roe` — Calculate Return on Equity (ROE)
- GET `/api/analytics/screening` — Run stock screening
- GET `/api/analytics/sector-stocks` — Get stocks by sector
- GET `/api/analytics/stocks/{symbol}/margin-pressure` — Get margin pressure indicators
- GET `/api/analytics/stocks/{symbol}/margin-ratio` — Get margin volume ratio

### Market Data
- GET `/api/market/stocks` — Get all stocks data for screening
- GET `/api/market/stocks/{code}/ohlcv` — Get stock OHLCV data from market.db
- GET `/api/market/topix` — Get TOPIX data from market.db

### Database
- GET `/api/db/stats` — Get market database statistics
- POST `/api/db/stocks/refresh` — Refresh historical data for specific stocks
- POST `/api/db/sync` — Start market data synchronization
- GET `/api/db/sync/jobs/{jobId}` — Get sync job status
- DELETE `/api/db/sync/jobs/{jobId}` — Cancel sync job
- GET `/api/db/validate` — Validate market database

### Dataset
- GET `/api/dataset` — List all datasets
- POST `/api/dataset` — Start dataset creation
- POST `/api/dataset/resume` — Resume incomplete dataset
- GET `/api/dataset/jobs/{jobId}` — Get dataset creation job status
- DELETE `/api/dataset/jobs/{jobId}` — Cancel dataset creation job
- GET `/api/dataset/{name}/info` — Get dataset information
- GET `/api/dataset/{name}/sample` — Sample stocks from dataset
- GET `/api/dataset/{name}/search` — Search stocks in dataset
- DELETE `/api/dataset/{name}` — Delete a dataset

### Dataset Data
- GET `/api/dataset/{name}/stocks` — Get stock list
- GET `/api/dataset/{name}/stocks/{code}/ohlcv` — Get stock OHLCV data
- GET `/api/dataset/{name}/stocks/ohlcv/batch` — Get batch stock OHLCV data
- GET `/api/dataset/{name}/topix` — Get TOPIX data
- GET `/api/dataset/{name}/indices` — Get index list
- GET `/api/dataset/{name}/indices/{code}` — Get index data
- GET `/api/dataset/{name}/margin` — Get margin list
- GET `/api/dataset/{name}/margin/{code}` — Get margin data
- GET `/api/dataset/{name}/margin/batch` — Get batch margin data
- GET `/api/dataset/{name}/statements/{code}` — Get financial statements
- GET `/api/dataset/{name}/statements/batch` — Get batch financial statements
- GET `/api/dataset/{name}/sectors` — Get all sectors with stock count
- GET `/api/dataset/{name}/sectors/mapping` — Get sector mapping
- GET `/api/dataset/{name}/sectors/stock-mapping` — Get stock to sector mapping
- GET `/api/dataset/{name}/sectors/{sectorName}/stocks` — Get stocks in a sector

### Portfolio
- GET `/api/portfolio` — List all portfolios
- POST `/api/portfolio` — Create a new portfolio
- GET `/api/portfolio/{id}` — Get portfolio details
- PUT `/api/portfolio/{id}` — Update portfolio
- DELETE `/api/portfolio/{id}` — Delete portfolio
- POST `/api/portfolio/{id}/items` — Add stock to portfolio
- PUT `/api/portfolio/{id}/items/{itemId}` — Update portfolio item by ID
- DELETE `/api/portfolio/{id}/items/{itemId}` — Delete portfolio item by ID
- GET `/api/portfolio/{id}/performance` — Get portfolio performance
- GET `/api/portfolio/{name}/codes` — Get stock codes in portfolio
- PUT `/api/portfolio/{portfolioName}/stocks/{code}` — Update stock in portfolio
- DELETE `/api/portfolio/{portfolioName}/stocks/{code}` — Remove stock from portfolio

### Watchlist
- GET `/api/watchlist` — List all watchlists
- POST `/api/watchlist` — Create a new watchlist
- GET `/api/watchlist/{id}` — Get watchlist details
- PUT `/api/watchlist/{id}` — Update watchlist
- DELETE `/api/watchlist/{id}` — Delete watchlist
- POST `/api/watchlist/{id}/items` — Add stock to watchlist
- DELETE `/api/watchlist/{id}/items/{itemId}` — Remove stock from watchlist
- GET `/api/watchlist/{id}/prices` — Get stock prices for watchlist

## 移行の方針
- API の URL とレスポンス形状は基本互換 (Hono の OpenAPI を基準に FastAPI 側に合わせる)
- データストアの場所は維持 (market.db / portfolio.db / datasets)
- 長時間ジョブは FastAPI 側でも同等の Job API を提供
- 既存フロント/クライアントの変更を最小化し、段階移行と切替を可能にする

## 循環依存の解消プラン (一本化前の最短安定化)

### 現状の循環
- Hono -> bt: `/api/analytics/fundamentals/{symbol}` が bt FastAPI を呼び出し
- bt -> Hono: `apps/bt` が `http://localhost:3001` の Hono API を呼び出し (market/db/jquants/dataset)

### 解消の選択肢

#### 案A: Hono を中心にして循環を切る (推奨: 影響が最小)
- 方針: `Hono -> bt` の依存は残し、`bt -> Hono` を削除
- 目的: bt 側が Hono を経由せずに market.db / datasets / portfolio.db / JQuants に直接アクセス

実施ステップ:
- bt 側の `API_BASE_URL` 依存を撤去
- bt 側に Python で market.db / datasets / portfolio.db 直接アクセス層を実装
- bt 側の `src/api/*` と loader をローカルDB読み取りに切替
- JQuants 呼び出しは bt 側で直接実行 (既存 `JQuantsAPIClient` を利用)
- 既存の Hono 側 API はそのまま維持し、外部からの利用に影響を出さない

メリット:
- TS 側 API と Web の変更が不要
- 既存の Hono 運用を維持しつつ循環を解消

デメリット:
- bt 側に DB アクセス実装が増える

#### 案B: bt を中心にして循環を切る
- 方針: `bt -> Hono` を残し、`Hono -> bt` を削除
- 目的: Hono のファンダメンタル計算を TS 側で再実装するか、Hono を単純化

実施ステップ:
- Hono の `/api/analytics/fundamentals/{symbol}` を bt 呼び出しから TS 実装に切替
- 既存の BacktestClient 依存を撤去
- bt 側は Hono API のまま利用

メリット:
- bt 側の変更が少ない

デメリット:
- TS 側に計算ロジックの移植が必要
- 既存 Python 実装とロジック差分が生まれやすい

### 推奨
- 最短で循環を解消したい場合は 案A
- 一本化を急ぐ場合は 案B (ただし移植コストは高い)

## 移行ロードマップ

### Phase 0: 事前調査と契約固定
- `apps/ts/packages/api/openapi.json` を移行契約として固定し、差分レビューの基準にする
- Hono 依存機能の棚卸し完了 (本ドキュメント)
- FastAPI 側の既存エンドポイント一覧を整理し、競合パスを明確化
- 例外レスポンスの共通フォーマットを定義 (error, message, correlationId)

### Phase 1: 基盤移植 (共通層)
- FastAPI に相当のミドルウェアを実装
- correlation id, request logging, CORS, エラーハンドリング
- OpenAPI のパスを `openapi.json` と同一に合わせる
- `/openapi.json` を Hono 互換で提供
- `/doc` の互換性方針を決定 (FastAPI 標準 or Scalar 導入)

### Phase 2: 読み取り系 API 移行 (低リスク)
- Health
- Chart
- Market Data
- Analytics (計算系は Python 実装 or 既存 ts ロジックを移植)
- JQuants Proxy (Python クライアント化)

### Phase 3: DB 操作とジョブ API 移行 (高リスク)
- Database: sync / validate / stats / refresh
- Dataset: 作成・再開・キャンセル・進捗
- ジョブ管理機構を FastAPI で再構築
- タイムアウト・中断・再開の挙動を Hono と一致させる

### Phase 4: CRUD 系 (Portfolio / Watchlist) 移行
- portfolio.db を Python で直接操作 (schema 互換)
- CRUD・items 操作・prices 取得の整合性確認
- 既存データの読み取り互換テスト

### Phase 5: 切替と廃止
- ルーティング切替 (フロント/クライアントの baseUrl を FastAPI に)
- Hono 側の本番トラフィック停止
- Hono サーバーの起動スクリプト / CI / 依存削除
- 旧 Hono API の退役後に `apps/ts/packages/api` を read-only 化

## 主要リスクと対策
- Bun/Drizzle 依存の移植コスト
- SQLite スキーマ互換の破壊
- JQuants API の認証・レート制限差異
- 長時間ジョブの安定運用 (キャンセル / 監視 / 再開)
- 既存クライアントのレスポンス互換性

## 成功条件
- Hono を停止しても 75 エンドポイント全てが FastAPI で同一動作
- DB ファイルを共有したままデータ互換が維持
- フロントエンドが API URL 変更なしで動作
- OpenAPI 契約の差分がゼロ
