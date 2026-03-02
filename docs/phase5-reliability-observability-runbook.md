# Phase 5 Reliability / Observability Runbook

更新日: 2026-03-02

## 1. 目的

- API / worker / artifact の障害時に、on-call が単独で初動できる状態を維持する。
- `correlationId` を起点に、FastAPI request → job lifecycle → J-Quants fetch/cache を追跡する。

## 2. 標準ログイベント（SoT）

- `request` / `request_error`
  - `correlationId`, `jobId`, `method`, `path`, `status`, `elapsed`, `errorRate`
- `job_lifecycle`
  - `correlationId`, `jobType`, `jobId`, `status`, `durationMs`
- `jquants_fetch`
  - `correlationId`, `endpoint`, `attempt`, `maxRetries`
- `jquants_retry`
  - `correlationId`, `endpoint`, `status`, `attempt`, `backoffSeconds`
- `jquants_proxy_cache`
  - `correlationId`, `endpoint`, `cacheState`, `cacheKey`

## 3. メトリクス採取（process-local）

`src/shared/observability/metrics.py` で以下を採取:

- API latency: `record_request(method, path, status, elapsed_ms)`
- API error rate: `error_rate()`
- Job duration: `record_job_duration(job_type, status, elapsed_ms)`
- J-Quants fetch count: `record_jquants_fetch(endpoint)`
- J-Quants cache state count: `record_jquants_cache_state(endpoint, state)`

## 4. timeout / retry / backoff デフォルト

`src/shared/config/reliability.py` を SoT とする。

- `JQUANTS_RETRY_POLICY.max_retries = 3`
- `JQUANTS_RETRY_POLICY.initial_backoff_seconds = 1.0`（指数バックオフ）
- `SYNC_JOB_TIMEOUT_MINUTES = 35`
- `DATASET_BUILD_TIMEOUT_MINUTES = 35`

## 5. 障害対応プレイブック

### 5.1 API 500 増加

1. `request_error` を `correlationId` で検索。
2. 同一IDの `job_lifecycle` / `jquants_*` を辿り、外部依存か内部例外か判定。
3. DB例外 (`SQLAlchemyError`) の場合は `market.db` / `portfolio.db` ロック競合を確認。

### 5.2 J-Quants 遅延 / 失敗

1. `jquants_retry` の `status` と `backoffSeconds` を確認。
2. 429/5xx が継続する場合、処理を `cancel` して時間帯をずらして再実行。
3. `jquants_proxy_cache` が `hit` にならない場合、同一パラメータ呼び出しになっているか確認。

### 5.3 job stuck（進捗停止）

1. `job_lifecycle` に `running` はあるが `completed/failed/cancelled` が無いか確認。
2. `GET /api/*/jobs/{id}` の `updated_at` と progress を確認。
3. 必要なら `POST .../cancel` → 同一payloadで retry。

### 5.4 artifact 不整合

1. `correlationId` から対象 `jobId` を特定。
2. `~/.local/share/trading25/backtest/results/**` の成果物有無を確認。
3. `result.html` と `*.metrics.json` の両方が無い場合は再実行。

## 6. 運用チェック（毎日）

- API error rate が通常レンジ（目安 < 2%）か。
- screening / backtest / optimize の job duration が急騰していないか。
- J-Quants retry が連続増加していないか。
