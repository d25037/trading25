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

- [ ] 現行 SoT を固定する（`FastAPI only`, `OpenAPI contract`, `contracts/` ガバナンス）。
- [ ] 移行対象機能を確定する（dataset / screening / backtest / optimize / fundamentals）。
- [ ] 非機能要件を数値化する（screening p95, backtest runtime, build throughput）。
- [ ] 成果物命名規約を確定する（artifact path, manifest schema version）。
- [ ] 監視項目を確定する（logs, metrics, correlationId trace）。

### Exit Criteria

- [ ] プロジェクト憲章 1ページが合意されている。
- [ ] 90日スコープ外の項目が明文化されている（意図的非採用）。

---

## Phase 1: Foundation（Day 4-20）

### Checklist

- [ ] `apps/bt` で layers を明確化する（`domains/application/infrastructure/entrypoints`）。
- [ ] middleware/order/error format を固定する（`RequestLogger -> CorrelationId -> CORS`）。
- [ ] OpenAPI 生成と ts 型同期パイプラインを固定する（`bt:sync` を標準運用化）。
- [ ] `jobs` テーブル（queue metadata）を定義する。
- [ ] `portfolio/watchlist/settings` と `jobs` の OLTP スキーマを整備する。
- [ ] 最小 worker runtime（`enqueue -> run -> status`）を用意する。
- [ ] artifact 保存先とメタ情報保存方式を定義する。

### Validation

- [ ] `uv run ruff check src/`
- [ ] `uv run pyright src/`
- [ ] `bun run --filter @trading25/shared bt:sync`
- [ ] API サーバ起動で `/doc` に契約が反映される。

### Exit Criteria

- [ ] 非同期 job 1本（dummy で可）が create/status/cancel/result まで通る。
- [ ] `x-correlation-id` が API/内部呼び出し/ログで追跡できる。

---

## Phase 2: Data Plane（Day 21-40）

### Checklist

- [ ] market 時系列の保存先を DuckDB + Parquet に切り分ける。
- [ ] portfolio/jobs は SQLite 維持とし、責務境界をコードで固定する。
- [ ] ingestion pipeline を `fetch -> normalize -> validate -> publish -> index` に分離する。
- [ ] statements upsert の非NULL優先 merge を共通処理化する。
- [ ] 欠損 OHLCV の skip + warning 集約を標準化する。
- [ ] dataset snapshot manifest v1（counts/checksums/coverage/schemaVersion）を実装する。
- [ ] `GET /api/dataset/{name}/info` を `snapshot + stats + validation` SoT に固定する。

### Validation

- [ ] dataset create/resume で既存データ再利用が機能する。
- [ ] legacy snapshot 読み取りで必須列不足のみ fail し、他は null 補完で継続する。
- [ ] 代表銘柄セットで data coverage / fk integrity が取得できる。

### Exit Criteria

- [ ] 日次同期を 2回連続実行して整合が崩れない（idempotent）。
- [ ] dataset build の再実行で結果再現性が確認できる。

---

## Phase 3: Core Use-cases（Day 41-60）

### Checklist

- [ ] screening API を async job SoT に一本化する。
- [ ] backtest API を artifact-first 再解決に統一する。
- [ ] optimize job に best/worst params と score を標準返却させる。
- [ ] fundamentals ranking/signal の計算 SoT を `src/domains` 側へ集約する。
- [ ] market filter 同義語（legacy/current）を API 入力境界で統一する。
- [ ] web/cli で同一 typed client を使うように重複呼び出しを削減する。

### Validation

- [ ] screening/backtest/optimize で create/status/result が全て通る。
- [ ] web 2秒ポーリング（または SSE）で進捗と完了が表示される。
- [ ] cli `--wait` で end-to-end が完走する。

### Exit Criteria

- [ ] 主要3ジョブで cancel/retry/resume の挙動が確認できる。
- [ ] 旧エンドポイント廃止時の互換メッセージ（410 等）が仕様通り出る。

---

## Phase 4: Frontend/CLI Hardening（Day 61-75）

### Checklist

- [ ] web API state を TanStack Query に統一する。
- [ ] job history UI を共通コンポーネント化する（screening/backtest/lab）。
- [ ] 重い一覧表示に virtualization を適用する。
- [ ] CLI の出力契約を統一する（`--json`, `--output`, `--wait`）。
- [ ] OpenAPI 由来の型に寄せて `any` を削減する。

### Validation

- [ ] `bun run quality:typecheck`
- [ ] `bun run quality:lint`
- [ ] `bun run workspace:test`
- [ ] `bun run --filter @trading25/web e2e:smoke`

### Exit Criteria

- [ ] web/cli の主要ワークフローで手動確認チェックリストを全通過。
- [ ] API 契約変更時に ts 側ビルドが自動で破綻検知できる。

---

## Phase 5: Reliability/Observability（Day 76-85）

### Checklist

- [ ] structured logging（event名, correlationId, jobId）を統一する。
- [ ] metrics（latency/error rate/job duration）を採取する。
- [ ] J-Quants proxy cache/singleflight の計測を標準化する。
- [ ] timeout/retry/backoff のデフォルトを機能別に定義する。
- [ ] 障害 runbook を `docs/` に整備する（API/DB/J-Quants/job stuck）。

### Validation

- [ ] 疑似障害でタイムアウト/再試行/キャンセルが想定通り動作する。
- [ ] correlationId から API->worker->artifact まで追跡できる。

### Exit Criteria

- [ ] 運用手順なしでも on-call が初動可能な状態になっている。

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

