# trading25 Greenfield プロジェクト憲章（90日）

作成日: 2026-02-27  
参照: `docs/greenfield-architecture-blueprint.md`, `docs/greenfield-implementation-checklist.md`

## 1. 目的

greenfield 方針で `apps/bt`（FastAPI + worker）と `apps/ts`（web/cli）を再編し、同一入力で同一結果を再現できる実行基盤を 90 日で完成させる。

## 2. 固定する SoT（Phase 0 合意）

- Backend SoT: **FastAPI のみ**（`:3002`）。
- API 契約 SoT: **OpenAPI**（変更時は `bt:sync` と契約更新を必須化）。
- 安定契約 SoT: **`contracts/` ガバナンス**（additive/breaking を version で管理）。
- ドメイン計算 SoT: **`apps/bt/src/domains/*`**。
- Error/Tracing SoT: 統一エラー形式 + `x-correlation-id` 伝播。

## 3. 90日スコープ（対象）

- dataset
- screening
- backtest
- optimize
- fundamentals

上記に加えて、job orchestration（create/status/cancel/result）、artifact-first 再解決、typed client 同期を実装対象に含める。

## 4. 90日スコープ外（意図的非採用）

- マイクロサービス分割
- 複数 backend 併存運用
- frontend 側の独自計算ロジック肥大化
- mutable dataset snapshot（不変性を崩す運用）
- 本番前提のインフラ拡張（Postgres/Redis/Object Storage への即時移行）

## 5. 非機能要件（数値目標・暫定）

| 項目 | 目標 |
|---|---|
| Screening runtime p95 | <= 120 秒（代表 universe + 標準戦略） |
| Backtest runtime median | <= 180 秒（5年日足、標準戦略） |
| Dataset build throughput | >= 50,000 rows/分（OHLCV + benchmark） |
| API status/result p95 | <= 300 ms（job status/result endpoint） |

注記: 上記は Phase 0 の暫定受入基準。Phase 6 で実測 baseline を記録し、必要に応じて閾値を改訂する。

## 6. 成果物命名規約（artifact + manifest）

- 共有ルート: `~/.local/share/trading25/`
- 命名原則:
  - job 系: `{domain}/{jobId}/`
  - dataset 系: `datasets/{datasetName}/...`
  - manifest: `manifest.v1.json`
  - metrics: `*.metrics.json`
- schemaVersion:
  - manifest は `schemaVersion: 1` を必須とする
  - 将来 breaking 変更時は `manifest.v2.json` を新設し並存期間を設ける

## 7. 監視項目（logs/metrics/trace）

- Structured logs（必須キー）:
  - `event`, `correlationId`, `jobId`, `status`, `durationMs`
- Metrics（最小セット）:
  - API latency（p50/p95）, error rate
  - job duration（type/state 別）, queue depth, timeout/cancel/retry 件数
- Trace:
  - `x-correlation-id` を API -> internal client -> worker -> artifact metadata まで伝播

## 8. 合意運用

- 本憲章を greenfield 実装の判断基準とする。
- 変更時は PR で本ドキュメントと `docs/greenfield-implementation-checklist.md` を同時更新する。
