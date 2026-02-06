# Phase 1B 依存方向監査レポート

**日付**: 2026-02-06
**目的**: ADR-001 パターン A（bt→ts 許可、ts→bt 撤去）の適用状況を監査し、全呼び出し箇所を記録する。

---

## 1. 監査基準

### パターン A（ADR-001）

| 方向 | 判定 | メカニズム |
|------|------|-----------|
| bt→ts | **許可** | REST HTTP 経由（localhost:3001） |
| ts→bt | **撤去対象** | Phase 3 で段階的に解消 |
| ts→bt（ツーリング） | **恒久許可** | bt:sync 用スクリプトのみ |

### 検出パターン

ts→bt 参照の検出に使用したパターン:

```
localhost:3002, 127.0.0.1:3002, BT_API_URL, /bt/api/,
BacktestClient, @trading25/shared/clients/backtest,
bt-api-types, backtest/generated
```

---

## 2. bt→ts（準拠）

全て REST HTTP 経由（localhost:3001）。直接コード import なし。

### API クライアント層

| ファイル | 対象 | メカニズム |
|---------|------|-----------|
| `apps/bt/src/config/settings.py` | api_base_url 設定 | `http://localhost:3001` デフォルト |
| `apps/bt/src/api/client.py` | BaseAPIClient | httpx HTTP クライアント |
| `apps/bt/src/api/market_client.py` | MarketAPIClient | `/api/market/*` |
| `apps/bt/src/api/portfolio_client.py` | PortfolioAPIClient | `/api/portfolio/*` |
| `apps/bt/src/api/dataset/base.py` | DatasetAPIClient | `/api/dataset/*` |
| `apps/bt/src/api/jquants_client.py` | JQuantsAPIClient | `/api/jquants/*` |

### データローダー（API クライアント経由）

| ファイル | 取得データ |
|---------|-----------|
| `apps/bt/src/data/loaders/stock_loaders.py` | 株価 OHLCV |
| `apps/bt/src/data/loaders/index_loaders.py` | 指数データ |
| `apps/bt/src/data/loaders/margin_loaders.py` | 信用取引データ |
| `apps/bt/src/data/loaders/portfolio_loaders.py` | ポートフォリオ |
| `apps/bt/src/data/loaders/sector_loaders.py` | セクターデータ |
| `apps/bt/src/data/loaders/statements_loaders.py` | 財務諸表 |

### サーバー層

| ファイル | 用途 |
|---------|------|
| `apps/bt/src/server/app.py` | CORS origins に localhost:3001 を許可 |
| `apps/bt/src/server/services/fundamentals_service.py` | API クライアント経由でデータ取得 |
| `apps/bt/src/server/routes/fundamentals.py` | ファンダメンタルズ API ルート |

### 判定: **全て準拠**

直接 DB アクセスなし、直接コード import なし。全て REST HTTP 経由。

---

## 3. ts→bt（違反 — Phase 3 で解消予定）

全て REST HTTP 経由（localhost:3002）。直接コード import なし。

### 3.1 恒久許可（ツーリング）— 3 ファイル

bt:sync ワークフロー用。FastAPI の OpenAPI スキーマから TS 型を生成するために必要。

| ファイル | 用途 |
|---------|------|
| `apps/ts/packages/shared/scripts/fetch-bt-openapi.ts` | OpenAPI スキーマ取得 |
| `apps/ts/packages/shared/scripts/check-bt-types.ts` | 生成型の整合性チェック |
| `apps/ts/packages/shared/src/clients/backtest/generated/type-compatibility-check.ts` | 型互換性検証 |

### 3.2 Web UI hooks — 15 ファイル（解消: Phase 3F）

| ファイル | マッチパターン |
|---------|--------------|
| `apps/ts/packages/web/src/hooks/useBacktest.ts` | `/bt/api/` |
| `apps/ts/packages/web/src/hooks/useBacktest.test.tsx` | `/bt/api/` |
| `apps/ts/packages/web/src/hooks/useBtOHLCV.ts` | `/bt/api/` |
| `apps/ts/packages/web/src/hooks/useBtOHLCV.test.tsx` | `/bt/api/` |
| `apps/ts/packages/web/src/hooks/useBtIndicators.ts` | `/bt/api/` |
| `apps/ts/packages/web/src/hooks/useBtIndicators.test.ts` | `/bt/api/` |
| `apps/ts/packages/web/src/hooks/useBtMarginIndicators.ts` | `/bt/api/` |
| `apps/ts/packages/web/src/hooks/useBtMarginIndicators.test.ts` | `/bt/api/` |
| `apps/ts/packages/web/src/hooks/useBtSignals.ts` | `/bt/api/` |
| `apps/ts/packages/web/src/hooks/useLab.ts` | `/bt/api/` |
| `apps/ts/packages/web/src/hooks/useLab.test.tsx` | `/bt/api/` |
| `apps/ts/packages/web/src/hooks/useLabSSE.ts` | `/bt/api/` |
| `apps/ts/packages/web/src/hooks/useLabSSE.test.tsx` | `/bt/api/` |
| `apps/ts/packages/web/src/hooks/useOptimization.ts` | `/bt/api/` |
| `apps/ts/packages/web/src/hooks/useOptimization.test.tsx` | `/bt/api/` |

### 3.3 Web コンポーネント — 2 ファイル（解消: Phase 3F）

| ファイル | マッチパターン |
|---------|--------------|
| `apps/ts/packages/web/src/components/Lab/LabResultSection.tsx` | `@trading25/shared/clients/backtest` |
| `apps/ts/packages/web/src/types/backtest.ts` | `@trading25/shared/clients/backtest` |

### 3.4 CLI backtest コマンド — 7 ファイル（解消: Phase 3F）

| ファイル | マッチパターン |
|---------|--------------|
| `apps/ts/packages/cli/src/commands/backtest/run.ts` | `BT_API_URL` |
| `apps/ts/packages/cli/src/commands/backtest/cancel.ts` | `BT_API_URL` |
| `apps/ts/packages/cli/src/commands/backtest/list.ts` | `BT_API_URL` |
| `apps/ts/packages/cli/src/commands/backtest/results.ts` | `BT_API_URL` |
| `apps/ts/packages/cli/src/commands/backtest/status.ts` | `BT_API_URL` |
| `apps/ts/packages/cli/src/commands/backtest/validate.ts` | `BT_API_URL` |
| `apps/ts/packages/cli/src/commands/backtest/error-handler.ts` | `BT_API_URL` |

### 3.5 API サーバープロキシ — 5 ファイル（解消: Phase 3B/3D）

| ファイル | マッチパターン |
|---------|--------------|
| `apps/ts/packages/api/src/routes/analytics/fundamentals.ts` | `/bt/api/` |
| `apps/ts/packages/api/src/routes/__tests__/fundamentals.test.ts` | `/bt/api/` |
| `apps/ts/packages/api/src/routes/jquants/statements.ts` | `/bt/api/`（コメント内） |
| `apps/ts/packages/api/src/services/dataset/dataset-data-service.ts` | `BT_API_URL` |
| `apps/ts/packages/api/src/services/dataset/dataset-data-service.test.ts` | `BT_API_URL` |

### 3.6 API サーバー（deprecated）— 2 ファイル（解消: ts-123）

| ファイル | マッチパターン |
|---------|--------------|
| `apps/ts/packages/api/src/services/fundamentals-data.ts` | `BT_API_URL` |
| `apps/ts/packages/api/src/services/stock-data.ts` | `BT_API_URL` |

### 3.7 Shared クライアントライブラリ — 5 ファイル（解消: Phase 3F 後廃止）

| ファイル | マッチパターン |
|---------|--------------|
| `apps/ts/packages/shared/src/clients/backtest/BacktestClient.ts` | `BT_API_URL`, `BacktestClient` |
| `apps/ts/packages/shared/src/clients/backtest/BacktestClient.test.ts` | `BacktestClient` |
| `apps/ts/packages/shared/src/clients/backtest/index.ts` | `BacktestClient` |
| `apps/ts/packages/shared/src/clients/backtest/types.ts` | `bt-api-types` |

### 3.8 Vite プロキシ設定 — 1 ファイル（解消: Phase 3F）

| ファイル | マッチパターン |
|---------|--------------|
| `apps/ts/packages/web/vite.config.ts` | `localhost:3002` |

### 未知の違反: 0 件

---

## 4. サマリー

| カテゴリ | ファイル数 | 方向 | 判定 | 解消予定 |
|---------|-----------|------|------|---------|
| bt API クライアント | 6 | bt→ts | 準拠 | — |
| bt データローダー | 6 | bt→ts | 準拠 | — |
| bt サーバー層 | 3 | bt→ts | 準拠 | — |
| ts ツーリング（bt:sync） | 3 | ts→bt | **恒久許可** | — |
| ts Web UI hooks | 15 | ts→bt | 違反 | Phase 3F |
| ts Web コンポーネント | 2 | ts→bt | 違反 | Phase 3F |
| ts CLI backtest | 7 | ts→bt | 違反 | Phase 3F |
| ts API プロキシ | 5 | ts→bt | 違反 | Phase 3B/3D |
| ts API deprecated | 2 | ts→bt | 違反 | ts-123 |
| ts Shared クライアント | 4 | ts→bt | 違反 | Phase 3F 後廃止 |
| ts Vite プロキシ | 1 | ts→bt | 違反 | Phase 3F |

**合計**: bt→ts 15 ファイル（準拠）、ts→bt 39 ファイル（うち 3 恒久許可、36 撤去予定）

---

## 5. CI チェック機構

### 実装

- **スクリプト**: `scripts/check-dep-direction.sh`
- **allowlist**: `scripts/dep-direction-allowlist.txt`
- **統合先**: `scripts/lint.sh`（既存 lint の前に実行）

### 機能

1. `apps/ts/` 配下の `.ts`/`.tsx` ファイルを検出パターンで grep
2. マッチしたファイルが allowlist に含まれるか確認
3. **新規違反検出**: allowlist 外のマッチ → CI 失敗
4. **staleness check**: allowlist にあるがマッチしない → CI 失敗（不要エントリの排除）

---

## 6. 次アクション

1. **Phase 3B/3D**: API プロキシ 5 ファイルの bt 参照を FastAPI 統合で解消
2. **Phase 3F**: Web/CLI/Shared の 28 ファイルをルーティング切替で解消
3. **ts-123**: deprecated ファイル 2 件を削除
4. **継続監視**: CI の dep-direction check で新規違反を自動防止
