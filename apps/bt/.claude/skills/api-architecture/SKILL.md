# API Architecture

## REST APIクライアントシステム

SQLite直接アクセスからREST API（localhost:3001）経由に移行完了

### 統一APIクライアント

**実装箇所**: `src/api/`パッケージ

#### DatasetAPIClient
- **モジュール**: `dataset_client.py`
- **役割**: 株価・OHLCV・インデックスデータ取得
- **エンドポイント**: `/api/dataset/*`

#### MarketAPIClient
- **モジュール**: `market_client.py`
- **役割**: 市場データ・ランキング・スクリーニング
- **エンドポイント**: `/api/market/*`

#### PortfolioAPIClient
- **モジュール**: `portfolio_client.py`
- **役割**: ポートフォリオデータ管理
- **エンドポイント**: `/api/portfolio/*`

#### JQuantsAPIClient
- **モジュール**: `jquants_client.py`
- **役割**: JQuants財務諸表データ取得（apps/ts/api proxy経由）
- **エンドポイント**: `/api/jquants/*`, `/api/market/stocks/{code}`

### リソース管理

- HTTPセッション管理
- リトライ機構
- タイムアウト設定
- エラーハンドリング

### セクターAPIパターン

セクター関連データを取得するエンドポイント（SectorDataMixin）:
- **セクターマッピング**: `get_sector_mapping()` → `/sectors/mapping`
- **銘柄→セクター名**: `get_stock_sector_mapping()` → `/sectors/stock-mapping`
- **セクター別銘柄**: `get_sector_stocks(name)` → `/sectors/{name}/stocks`（URLエンコード必須）
- **全セクター一覧**: `get_all_sectors()` → `/sectors`（stock_count付き）

### バッチAPIパターン

複数銘柄のデータを一括取得するバッチエンドポイント:
- **OHLCV**: `get_stocks_ohlcv_batch()` → `/stocks/ohlcv/batch`
- **Margin**: `get_margin_batch()` → `/margin/batch`
- **Statements**: `get_statements_batch()` → `/statements/batch`

共通パターン:
- TS: `MAX_BATCH_CODES=100`、内部`BATCH_SIZE=10`で並列処理、`processBatched()`ヘルパー使用
- PY: `MAX_BATCH_SIZE=100`で自動分割、batch API失敗時は個別リクエストにfallback
- レスポンス: `Record<string, Data[]>` / `dict[str, pd.DataFrame]`

### データローダー移行

全loaders（stock/index/margin/statements/portfolio等）がAPI経由アクセスに変更

**移行完了ファイル**:
- `stock_loaders.py`: 株価データローダー
- `index_loaders.py`: インデックスデータローダー
- `margin_loaders.py`: 信用取引データローダー
- `multi_asset_loaders.py`: マルチアセットローダー
- `sector_loaders.py`: セクター別データローダー
- `statements_loaders.py`: 財務データローダー
- `portfolio_loaders.py`: ポートフォリオデータローダー（新規追加）

### 旧実装削除

- `src/data/database.py`完全削除（SQLite直接アクセス廃止）
- `src/strategies/signals/sector.py`からSQLite直接アクセス廃止（`_connect_dataset_db()`削除）

### HTTPクライアント設定例

```python
# タイムアウト設定
DEFAULT_TIMEOUT = 30  # 秒

# リトライ設定
MAX_RETRIES = 3
RETRY_BACKOFF = 1.5  # 指数バックオフ係数
```

### エラーハンドリング

```python
try:
    response = client.get_stock_data(symbol="1301")
except HTTPError as e:
    # HTTP エラー処理
    logger.error(f"API request failed: {e}")
except TimeoutError as e:
    # タイムアウト処理
    logger.error(f"API request timeout: {e}")
```

## FastAPI サーバー（バックテストAPI）

**実装箇所**: `src/server/`

### バックテストエンドポイント
- `POST /api/backtest/run` — バックテストジョブをサブミット
- `GET /api/backtest/jobs/{job_id}` — ジョブステータス取得
- `GET /api/backtest/jobs/{job_id}/stream` — SSEリアルタイム進捗通知
- `GET /api/backtest/jobs` — ジョブ一覧
- `POST /api/backtest/jobs/{job_id}/cancel` — ジョブキャンセル（冪等、COMPLETED/FAILED→409）
- `GET /api/backtest/result/{job_id}` — バックテスト結果取得

### ジョブステータス遷移
- `PENDING` → `RUNNING` → `COMPLETED` / `FAILED` / `CANCELLED`
- terminal状態（COMPLETED/FAILED/CANCELLED）からの巻き戻し不可（`update_job_status`でガード）
- キャンセルは論理キャンセル（`run_in_executor`内スレッドは即停止しない）

### シグナルリファレンスエンドポイント
- `GET /api/signals/reference` — 全シグナル定義のリファレンスデータ返却（フロントエンド向け）

**アーキテクチャ**:
- `routes/signal_reference.py` → `services/signal_reference_service.py` → `SIGNAL_REGISTRY`
- `SignalDefinition`の`category`/`description`/`param_key`フィールドから自動構築
- Pydanticモデルからフィールド情報を自動抽出（型・デフォルト値・選択肢）
- YAMLスニペット自動生成（`enabled: true`上書き）

### 最適化エンドポイント（Grid Search）
- `POST /api/optimize/run` — グリッドサーチ最適化
- `GET /api/optimize/jobs/{job_id}` — ジョブステータス取得
- `GET /api/optimize/jobs/{job_id}/stream` — SSEリアルタイム進捗通知

### Lab エンドポイント（戦略自動生成・進化・最適化・改善）
- `POST /api/lab/generate` — 戦略自動生成（StrategyGenerator + StrategyEvaluator）
- `POST /api/lab/evolve` — GA進化（ParameterEvolver）
- `POST /api/lab/optimize` — Optuna最適化（OptunaOptimizer, TPE/CMA-ES/Random, プログレスコールバック付き）
- `POST /api/lab/improve` — 戦略改善（StrategyImprover）
- `GET /api/lab/jobs/{job_id}` — ジョブステータス取得
- `GET /api/lab/jobs/{job_id}/stream` — SSEリアルタイム進捗通知
- `POST /api/lab/jobs/{job_id}/cancel` — ジョブキャンセル

**アーキテクチャ**:
- `routes/lab.py` → `services/lab_service.py` → Agent modules（`src/agent/`）
- `LabJobResponse(BaseJobResponse)` + discriminated union `result_data`（lab_type別）
- `ThreadPoolExecutor(max_workers=1)` + `JobManager`共有（Semaphore同時実行制限）
- Optuna optimize は `asyncio.run_coroutine_threadsafe` でスレッド→非同期のプログレス通知
- 旧 `/api/optimize/optuna` はLab版に統合・削除済み

### SSE進捗通知アーキテクチャ
- **Pub/Sub**: `JobManager.subscribe()/unsubscribe()` → `asyncio.Queue`
- **SSEManager**: `job_event_generator()` AsyncGenerator（heartbeat 30秒、終了シグナルNone）
- **同期→非同期ブリッジ**: `asyncio.run_coroutine_threadsafe()` でThreadPoolExecutorからSSE通知を発火
- **sse-starlette**: `EventSourceResponse`によるSSEストリーミング

### インジケーターエンドポイント
- `POST /api/indicators/compute` — 複数インジケーター一括計算
  - `output: "indicators"` — インジケーター計算結果を返却（デフォルト）
  - `output: "ohlcv"` — 変換後OHLCVのみを返却（インジケーター計算スキップ）
- `POST /api/indicators/margin` — 信用指標計算（4種: long_pressure, flow_pressure, turnover_days, volume_ratio）

**11種のテクニカルインジケーター**:
SMA, EMA, RSI, MACD, PPO, Bollinger, ATR, ATR Support, N-Bar Support, Volume Comparison, Trading Value MA

**アーキテクチャ**:
- `routes/indicators.py` → `services/indicator_service.py` → `INDICATOR_REGISTRY` (11 compute functions)
- vectorbtデフォルトindicatorは直接呼び出し、自作indicatorは `src/utils/indicators.py` 共通関数経由
- `ThreadPoolExecutor(max_workers=5)` + `asyncio.wait_for(timeout=10s)`
- 共通関数 (`src/utils/indicators.py`) はsignal関数とindicator serviceの両方から呼ばれる
- 信用指標は apps/ts/shared/src/ta/margin-pressure-indicators.ts をpandas rolling操作で再実装

### OHLCVリサンプルエンドポイント
- `POST /api/ohlcv/resample` — OHLCVデータのTimeframe変換

**仕様**: `docs/spec-timeframe-resample.md`

**機能**:
- **Timeframe変換**: 日足→週足/月足リサンプル
- **Relative OHLC**: ベンチマーク（TOPIX）との相対値変換（オプション）

**計算順序**: Relative OHLC計算 → Timeframe Resample

**集約ルール**:
- Open: first（期間最初の始値）
- High: max（期間中の最高値）
- Low: min（期間中の最安値）
- Close: last（期間最後の終値）
- Volume: sum（期間の出来高合計）

**インデックス調整**（仕様に準拠）:
- 週足: 週開始日（月曜）をインデックスとする
- 月足: 月初日（1日）をインデックスとする

**ゼロ除算処理**（`handle_zero_division`オプション）:
- `skip`（デフォルト）: ベンチマークがゼロの日を除外
- `zero`: 相対値を0.0とする
- `null`: 相対値をNaNとする

**アーキテクチャ**:
- `routes/ohlcv.py` → `services/indicator_service.py` → `calculate_relative_ohlcv()` + `resample_timeframe()`
- `ThreadPoolExecutor(max_workers=3)` + `asyncio.wait_for(timeout=10s)`

### Fundamentalsエンドポイント（財務指標計算）
- `POST /api/fundamentals/compute` — 財務指標計算（SSOT: apps/bt/側で演算）

**17種類の財務指標**:
- **Valuation**: PER, PBR
- **Profitability**: ROE, ROA, Operating Margin, Net Margin
- **Per-share**: EPS, BPS, Diluted EPS
- **FCF**: FCF, FCF Yield, FCF Margin
- **Time-series**: Daily PER/PBR valuation
- **Forecast**: Forecast EPS, Forecast Change Rate

**データソース**:
- Financial statements: JQuants API via apps/ts/api proxy
- Stock prices: market.db via apps/ts/api proxy

**アーキテクチャ**:
- `routes/fundamentals.py` → `services/fundamentals_service.py` → `JQuantsAPIClient` + `MarketAPIClient`
- `ThreadPoolExecutor(max_workers=4)` + `asyncio.run_in_executor()`
- シングルトンサービスインスタンス（`fundamentals_service`）
- リソースクリーンアップ: `app.py` lifespan で `close()` 呼び出し

## 関連ファイル

- `src/api/dataset_client.py`
- `src/api/market_client.py`
- `src/api/portfolio_client.py`
- `src/api/jquants_client.py`
- `src/data/loaders/*_loaders.py`
- `src/server/services/sse_manager.py`
- `src/server/services/job_manager.py`
- `src/server/services/optimization_service.py`
- `src/server/services/lab_service.py`
- `src/server/services/fundamentals_service.py`
- `src/server/routes/lab.py`
- `src/server/routes/fundamentals.py`
- `src/server/schemas/lab.py`
- `src/server/schemas/fundamentals.py`
- `src/utils/indicators.py`
- `src/server/services/indicator_service.py`
- `src/server/routes/indicators.py`
- `src/server/routes/ohlcv.py`
- `src/server/schemas/indicators.py`
- `tests/unit/api/`
- `tests/api/test_jquants_client.py`
- `tests/server/test_sse.py`
- `tests/server/routes/test_lab.py`
- `tests/server/routes/test_ohlcv.py`
- `tests/server/routes/test_fundamentals.py`
- `tests/server/services/test_fundamentals_service.py`
- `tests/server/test_resample_compatibility.py`
- `docs/spec-timeframe-resample.md`
