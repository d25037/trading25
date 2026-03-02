# trading25 Greenfield 実装チェックリスト（着手順）

作成日: 2026-02-27  
参照: `docs/greenfield-architecture-blueprint.md`

## 使い方

- この順番で上から実施する（前フェーズ完了前に次へ進まない）。
- 各フェーズの `Exit Criteria` を満たしたら次フェーズへ移行する。
- 途中で設計変更が出たら、必ず OpenAPI/契約とテストを先に更新する。

---

## Phase 0: キックオフ（Day 1-3）

### Checklist

- [x] 現行 SoT を固定する（`FastAPI only`, `OpenAPI contract`, `contracts/` ガバナンス）。
- [x] 移行対象機能を確定する（dataset / screening / backtest / optimize / fundamentals）。
- [x] 非機能要件を数値化する（screening p95, backtest runtime, build throughput）。
- [x] 成果物命名規約を確定する（artifact path, manifest schema version）。
- [x] 監視項目を確定する（logs, metrics, correlationId trace）。

### Exit Criteria

- [x] プロジェクト憲章 1ページが合意されている。
- [x] 90日スコープ外の項目が明文化されている（意図的非採用）。

---

## Phase 1: Foundation（Day 4-20）

### Checklist

- [x] `apps/bt` で layers を明確化する（`domains/application/infrastructure/entrypoints`）。
- [x] middleware/order/error format を固定する（`RequestLogger -> CorrelationId -> CORS`）。
- [x] OpenAPI 生成と ts 型同期パイプラインを固定する（`bt:sync` を標準運用化）。
- [x] `jobs` テーブル（queue metadata）を定義する。
- [x] `portfolio/watchlist/settings` と `jobs` の OLTP スキーマを整備する。
- [x] 最小 worker runtime（`enqueue -> run -> status`）を用意する。
- [x] artifact 保存先とメタ情報保存方式を定義する。

### Validation

- [x] `uv run ruff check src/`
- [x] `uv run pyright src/`
- [x] `bun run --filter @trading25/shared bt:sync`
- [x] API サーバ起動で `/doc` に契約が反映される。

### Exit Criteria

- [x] 非同期 job 1本（dummy で可）が create/status/cancel/result まで通る。
- [x] `x-correlation-id` が API/内部呼び出し/ログで追跡できる。

---

## Phase 2: Data Plane（Day 21-40）

### Checklist

- [x] market 時系列の保存先を DuckDB + Parquet に切り分ける。
- [x] portfolio/jobs は SQLite 維持とし、責務境界をコードで固定する。
- [x] ingestion pipeline を `fetch -> normalize -> validate -> publish -> index` に分離する。
- [x] statements upsert の非NULL優先 merge を共通処理化する。
- [x] 欠損 OHLCV の skip + warning 集約を標準化する。
- [x] dataset snapshot manifest v1（counts/checksums/coverage/schemaVersion）を実装する。
- [x] `GET /api/dataset/{name}/info` を `snapshot + stats + validation` SoT に固定する。

### Validation

- [x] dataset create/resume で既存データ再利用が機能する。
- [x] legacy snapshot 読み取りで必須列不足のみ fail し、他は null 補完で継続する。
- [x] 代表銘柄セットで data coverage / fk integrity が取得できる。

### Exit Criteria

- [x] 日次同期を 2回連続実行して整合が崩れない（idempotent）。
- [x] dataset build の再実行で結果再現性が確認できる。

---

## Phase 3: Core Use-cases（Day 41-60）

### Checklist

- [x] screening API を async job SoT に一本化する。
  - 2026-03-02 検証: `GET /api/analytics/screening` は 410 + 移行メッセージを返し、`POST/GET /api/analytics/screening/jobs*` と `GET /api/analytics/screening/result/{job_id}` を SoT とするルートテストを整備。
- [x] backtest API を artifact-first 再解決に統一する。
  - 2026-03-02 検証: `apps/bt/tests/unit/server/routes/test_backtest.py` で `result.html + *.metrics.json` 優先再解決と fallback の挙動を確認。
- [x] optimize job に best/worst params と score を標準返却させる。
  - 2026-03-02 検証: `apps/bt/src/entrypoints/http/routes/optimize.py` / `apps/ts/packages/web/src/hooks/useOptimization.test.tsx` で `best_* / worst_* / total_combinations` を lifecycle テストで確認。
- [x] fundamentals ranking/signal の計算 SoT を `src/domains` 側へ集約する。
  - 2026-03-02 検証: fundamentals ranking は `src/domains/analytics/fundamental_ranking.py` を SoT とし、signal 評価は `src/domains/analytics/screening_requirements.py` / `screening_results.py` / `screening_evaluator.py` へ抽出。`test_screening_*` domain/service テストと `test_ranking_service.py` / `test_analytics_complex.py -k \"fundamental or ranking\"` で回帰確認。
- [x] market filter 同義語（legacy/current）を API 入力境界で統一する。
  - 2026-03-02 継続: `/api/market/stocks` の `market` 入力で `prime/standard/growth` と `0111/0112/0113` を同義受理するよう route 境界を更新し、route/service テストで互換性を検証。
- [x] web/cli で同一 typed client を使うように重複呼び出しを削減する。
  - 2026-02-28 着手: `@trading25/api-clients/analytics` を新設し、screening/fundamental-ranking を web/cli 共通 client へ移行開始。
  - 2026-02-28 継続: `@trading25/api-clients/backtest` を web 利用可能に拡張し、`useOptimization` / `useBtOHLCV` の API 呼び出しを shared client に移行。
  - 2026-02-28 継続: `useBacktest` の core job 系（health/strategies/jobs/result/attribution/cancel）を shared `backtestClient` に移行。
  - 2026-02-28 継続: `useBacktest` の残り（strategy CRUD / html artifacts / default config / signal reference）も shared `backtestClient` に移行。
  - 2026-03-02 継続: CLI `backtest` / `backtest attribution` コマンドの `BacktestClient` 生成を `commands/backtest/client.ts` に集約。
  - 2026-03-02 継続: web hooks の `ranking/factor-regression/portfolio-factor-regression/fundamentals/margin-pressure/margin-ratio/sector-stocks` を shared `analyticsClient` に統一し、`/api/analytics` 直叩きを撤廃。

### Validation

- [x] screening/backtest/optimize で create/status/result が全て通る。
- [x] web 2秒ポーリング（または SSE）で進捗と完了が表示される。
- [x] cli `--wait` で end-to-end が完走する。
  - 2026-03-02 検証追加: `apps/ts/packages/cli/src/commands/analysis/screening.test.ts` で screening create/status/result 呼び出しを明示検証。
  - 2026-03-02 検証追加: `apps/ts/packages/cli/src/commands/backtest/commands.test.ts` に `backtest run --wait` の health→create→status→結果表示までの E2E テストを追加。
  - 2026-03-02 検証追加: `apps/ts/packages/web/src/hooks/useOptimization.test.tsx` に optimize create→status(best/worst params)→result artifact取得の lifecycle テストを追加。
  - 2026-03-02 検証追加: `useBacktest/useScreening/useOptimization` の status hook で `pending/running => refetchInterval=2000ms`, `completed => false` を明示テスト化。

### Exit Criteria

- [x] 主要3ジョブで cancel/retry/resume の挙動が確認できる。
  - 2026-03-02 検証: screening/backtest 既存 cancel テストに加え、optimize に `POST /api/optimize/jobs/{job_id}/cancel` と web hook cancel/retry/resume lifecycle テストを追加。
- [x] 旧エンドポイント廃止時の互換メッセージ（410 等）が仕様通り出る。
  - 2026-03-02 検証: `apps/bt/tests/unit/server/routes/test_analytics_complex.py` で 410 応答の `status/error/message` を移行メッセージまで明示検証。

---

## Phase 4: Frontend/CLI Hardening（Day 61-75）

### Checklist

- [x] web API state を TanStack Query に統一する。
  - 2026-03-02 完了: `useScreening` の job lifecycle を TanStack Query + store 同期で統一し、screening 履歴表示も query/store 経由で管理。
- [x] job history UI を共通コンポーネント化する（screening/backtest/lab）。
  - 2026-03-02 完了: `apps/ts/packages/web/src/components/Jobs/JobHistoryTable.tsx` を追加し、`Backtest/JobsTable` / `Lab/LabJobHistoryTable` / `Screening/ScreeningJobHistoryTable` へ共通適用。
- [x] 重い一覧表示に virtualization を適用する。
  - 2026-03-02 完了: `useVirtualizedRows` を追加し、`ScreeningTable` / `RankingTable` / `FundamentalRankingTable` にしきい値付き仮想化を導入（大量行時のみ有効化）。
- [x] CLI の出力契約を統一する（`--json`, `--output`, `--wait`）。
  - 2026-03-02 完了: `apps/ts/packages/cli/src/utils/job-command-output.ts` を追加し、`analysis screening` / `backtest run` / `backtest attribution run` に `--wait` / `--json` / `--output` を統一適用（`--no-wait` 互換維持）。
- [x] OpenAPI 由来の型に寄せて `any` を削減する。
  - 2026-03-02 維持確認: `apps/ts/packages/web/src` / `apps/ts/packages/cli/src` の production code で `any` 使用なしを確認（`rg -n "\\bany\\b"`）。

### Validation

- [x] `bun run quality:typecheck`
- [x] `bun run quality:lint`
- [x] `bun run workspace:test`
- [x] `bun run --filter @trading25/web e2e:smoke`
  - 2026-03-02 補足: `PLAYWRIGHT_WEB_PORT=47831` を指定し、`uv run bt server --port 3002` 起動状態で smoke 4件通過。

### Exit Criteria

- [x] web/cli の主要ワークフローで手動確認チェックリストを全通過。
  - 2026-03-02 検証: web (`Analysis/Backtest/Lab`) の履歴表示と仮想化対象、cli (`analysis screening` / `backtest run` / `backtest attribution run`) の `--wait --json --output` をテストで確認。
- [x] API 契約変更時に ts 側ビルドが自動で破綻検知できる。
  - 2026-03-02 検証: `quality:typecheck` 実行時に `@trading25/shared bt:generate-types`（OpenAPI生成）を経由し、ts workspace typecheck を通過。

---

## Phase 5: Reliability/Observability（Day 76-85）

### Checklist

- [x] structured logging（event名, correlationId, jobId）を統一する。
  - 2026-03-02 実装: `RequestLoggerMiddleware` と `ScreeningJobService` で `request/request_error/job_lifecycle` の構造化キーを統一。
- [x] metrics（latency/error rate/job duration）を採取する。
  - 2026-03-02 実装: `src/shared/observability/metrics.py` を追加し、request/job/J-Quants の process-local メトリクス集計を導入。
- [x] J-Quants proxy cache/singleflight の計測を標準化する。
  - 2026-03-02 実装: `jquants_proxy_cache` ログに加えて cache state カウンタを追加し、`jquants_fetch/jquants_retry` も共通キーで記録。
- [x] timeout/retry/backoff のデフォルトを機能別に定義する。
  - 2026-03-02 実装: `src/shared/config/reliability.py` に J-Quants retry/backoff・sync/dataset timeout の SoT を新設。
- [x] 障害 runbook を `docs/` に整備する（API/DB/J-Quants/job stuck）。
  - 2026-03-02 実装: `docs/phase5-reliability-observability-runbook.md` を追加。

### Validation

- [x] 疑似障害でタイムアウト/再試行/キャンセルが想定通り動作する。
  - 2026-03-02 検証: `test_jquants_client.py` の retry/timeout と `test_screening_job_service.py` の cancel/failed を回帰確認。
- [x] correlationId から API->worker->artifact まで追跡できる。
  - 2026-03-02 検証: request/job/jquants ログに `correlationId` を付与し runbook に追跡手順を明記。

### Exit Criteria

- [x] 運用手順なしでも on-call が初動可能な状態になっている。
  - 2026-03-02 検証: Phase5 runbook（API/DB/J-Quants/job stuck）を整備。

---

## Phase 6: Release Gate（Day 86-90）

### Checklist

- [ ] contract tests を CI 必須にする。
- [ ] Golden dataset 回帰テストを CI 必須にする。
- [ ] coverage gate（bt 70%+, ts 既存基準）を満たす。
- [ ] performance baseline（screening/backtest/build）を記録する。
- [ ] 移行完了ドキュメントを作成する（差分、制約、次フェーズ課題）。

### Validation

- [ ] `.github/workflows/ci.yml` の required checks を全緑にする。
- [ ] 本番相当データ量の smoke run を 1サイクル通す。

### Exit Criteria

- [ ] 「同一入力で同一結果」が主要ユースケースで再現できる。
- [ ] 既知制約と次アクションが `issues/` に登録済み。

---

## 優先順位つき着手順（最短ルート）

1. Phase 1 の `job骨格 + OpenAPI同期 + middleware固定`
2. Phase 2 の `dataset manifest + idempotent ingestion`
3. Phase 3 の `screening/backtest async job統一`
4. Phase 4 の `web/cli typed client統一`
5. Phase 5 の `observability + runbook`
6. Phase 6 の `CI gate固定 + release`

---

## ブロッカー早見表（先に潰す）

- [ ] DB SoT が曖昧（SQLite vs DuckDB の責務未定義）
- [ ] OpenAPI 更新フローが PR で自動チェックされない
- [ ] worker の cancel/retry 実装が後回し
- [ ] Golden dataset が未整備で回帰検知不能
- [ ] web/cli で API 呼び出し実装が二重管理
