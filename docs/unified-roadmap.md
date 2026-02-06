# trading25 統一ロードマップ

作成日: 2026-02-06
統合元: 5つの個別ロードマップ（[Appendix D](#appendix-d-アーカイブ元ドキュメント) 参照）

---

## Governance Baseline（運用規約）

### 現行ルール

| ルール | 根拠 |
|---|---|
| `apps/ts/packages/api` が唯一の JQuants API 窓口 | AGENTS.md |
| `apps/ts` が DB 管理者（market.db / portfolio.db / datasets） | AGENTS.md |
| `apps/bt` は `apps/ts` API 経由でデータアクセス（直接 DB 禁止） | AGENTS.md |
| API 呼び出し方向: パターン A（bt→ts 許可、ts→bt 撤去） | ADR-001 |
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

### ADR-003: DB 管理責務の移行（ts→FastAPI）— 未承認

- **提案日**: 2026-02-06
- **提案**: Phase 3C で FastAPI が market.db / dataset.db / portfolio.db に直接アクセスする
- **前提条件**:
  - Phase 2B（FastAPI 事前調査）完了
  - AGENTS.md の「直接 DB 禁止」ルールの更新
- **承認条件**: Phase 2C で正式決定
- **元ドキュメント**: `docs/archive/hono-to-fastapi-migration-roadmap.md`

---

## ステータスダッシュボード

| Phase | 名称 | 状態 | リスク | 見積 |
|---|---|---|---|---|
| 1 | 基盤安定化 | **未着手** | Low | 1-2 週 |
| 2 | 契約・データ境界 | **未着手** | Low | 1-2 週 |
| 3 | FastAPI 統一 | **未着手** | **High** | 6-10 週 |
| 4 | パッケージ分離 | **未着手** | Medium | 4-6 週 |
| 5 | シグナル・分析拡張 | **未着手** | Low | 2-3 週 |

---

## アーキテクチャ: 現状 vs 目標

### 現状

```
JQUANTS API ──→ ts/api (:3001, Hono) ──→ bt (REST APIクライアント)
                  ↑                          ↓
               ts/shared                  bt/server (:3002, FastAPI)
                  ↑                          ↓
               ts/web (:5173) ←──── /bt proxy ──→ bt/server
```

- ts/api が JQuants 窓口 + DB 管理者
- bt は ts/api 経由でデータアクセス（直接 DB 禁止）
- 2 つのバックエンドサーバーが稼働

### 目標（Phase 3 完了後）

```
JQUANTS API ──→ FastAPI (:3002) ──→ SQLite (market.db / portfolio.db / datasets)
                     ↓
                  ts/web (:5173)
```

- FastAPI が唯一のバックエンド
- Hono サーバー廃止
- 75+ エンドポイントを FastAPI に統合

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

- [ ] ts→bt、bt→ts の全呼び出し箇所の洗い出し
- [ ] パターン A（bt→ts 許可、ts→bt 撤去）の適用状況確認
- [ ] 依存方向違反のチェック機構追加（CI or lint rule）

### 1C: オープン Issue 対応

*元: 各ドキュメントの残タスク*

**bt 系**:
- [ ] bt-016: テストカバレッジ 70% 達成
- [ ] bt-017: signal registry param key validation
- [ ] bt-018: pyright pandas type errors
- [ ] bt-019: resample compatibility test todo
- [ ] bt-020: pydantic field example deprecation

**ts 系**:
- [ ] ts-121: market endpoint scope clarify
- [ ] ts-122: screening logic single source
- [ ] ts-123: remove deprecated fundamentals data service

---

## Phase 2: 契約・データ境界

**期間**: 1-2 週 | **リスク**: Low

### 2A: 契約スキーマ完成

*元: monorepo-migration-plan.md Phase 2 + packages-responsibility-roadmap.md Phase 0-1*

- [ ] `contracts/dataset-schema.json` を実データに基づいて拡張
- [ ] `packages/contracts` 作成・型生成ルール策定
- [ ] 契約バージョニングルール策定（additive vs breaking）
- [ ] 依存方向ルールのドキュメント化
- [ ] 新規 packages 作成時のテンプレート決定
- [ ] dataset スナップショット出力機能の方針決定（SQLite ベースの安定スキーマ出力 + manifest.json）
- [ ] apps/ts にスナップショット出力機能を実装
- [ ] apps/bt にスナップショットリーダー + スキーマバージョン検証を実装

### 2B: FastAPI 事前調査

*元: hono-to-fastapi-migration-roadmap.md Phase 0*

- [ ] OpenAPI 固定（Hono の openapi.json を移行契約として確定）
- [ ] 既存 FastAPI エンドポイント vs Hono エンドポイント監査
- [ ] 例外レスポンスフォーマット定義（error, message, correlationId）
- [ ] FastAPI 側の既存エンドポイント一覧整理、競合パス明確化

### 2C: ADR-003 策定（DB 管理責務移行の正式決定）

- [ ] DB 管理責務の ts→FastAPI 移行を正式に検討・決定
- [ ] AGENTS.md 更新案の作成
- [ ] 移行に伴うリスク評価

---

## Phase 3: FastAPI 統一 — クリティカルパス

**期間**: 6-10 週 | **リスク**: High

各サブフェーズ間に Go/No-Go 判定ゲートを設置。切り戻し範囲はドメイン単位に限定。

### 3A: ミドルウェア・基盤

*元: hono-to-fastapi-migration-roadmap.md Phase 1*

- [ ] correlation id, request logging, CORS, エラーハンドリング
- [ ] OpenAPI パス互換（Hono と同一パス提供）
- [ ] `/openapi.json` を Hono 互換で提供
- [ ] `/doc` の互換性方針決定（FastAPI 標準 or Scalar 導入）

**Go/No-Go**: ミドルウェアテスト全通過、OpenAPI パス互換確認

### 3B: 読み取り API 移行

*元: hono-to-fastapi-migration-roadmap.md Phase 2*

- [ ] Health (`GET /health`)
- [ ] Chart (5 エンドポイント)
- [ ] Market Data (3 エンドポイント)
- [ ] Analytics (9 エンドポイント)
- [ ] JQuants Proxy (7 エンドポイント)

**Go/No-Go**: 読取 API 全エンドポイントのレスポンス互換テスト合格

### 3C: Python SQLite アクセス層

*元: hono-to-fastapi-migration-roadmap.md 案A + 新規*

**前提**: ADR-003 承認済み、AGENTS.md 更新済み

- [ ] market.db / dataset.db / portfolio.db の Python 直接アクセス層
- [ ] sqlite3 + Pydantic モデルによるリポジトリパターン
- [ ] contracts/ スキーマとの整合性検証
- [ ] WAL モード pragma で読み取り並行性確保

**Go/No-Go**: 既存 Drizzle スキーマとの読取互換テスト合格

> **Note**: 3B と 3C は並行実施可能

### 3D: DB・ジョブ API 移行

*元: hono-to-fastapi-migration-roadmap.md Phase 3*

**前提**: 3C 完了（DB 操作は直接アクセスが前提）

- [ ] Database: sync, validate, stats, refresh
- [ ] Dataset: 作成・再開・キャンセル・進捗
- [ ] ジョブ管理機構の FastAPI 再構築
- [ ] タイムアウト・中断・再開の挙動を Hono と一致

**Go/No-Go**: ジョブライフサイクル（作成→実行→完了/キャンセル）テスト合格

### 3E: CRUD 移行

*元: hono-to-fastapi-migration-roadmap.md Phase 4*

- [ ] Portfolio CRUD (12 エンドポイント)
- [ ] Watchlist CRUD (8 エンドポイント)
- [ ] 既存データの読み取り互換テスト

**Go/No-Go**: CRUD 全操作のデータ整合性テスト合格

### 3F: 切替・廃止

*元: hono-to-fastapi-migration-roadmap.md Phase 5*

**前提**: 3A-3E 全完了

- [ ] ルーティング切替（フロント/クライアントの baseUrl を FastAPI に）
- [ ] Hono サーバー停止
- [ ] CI / 依存削除
- [ ] `apps/ts/packages/api` を read-only 化

**Go/No-Go**: 全 75 エンドポイントの結合テスト合格、OpenAPI 契約差分ゼロ

### Hono API エンドポイント完全一覧（75+）

移行対象の全エンドポイント。Phase 3 各サブフェーズの進捗追跡に使用。

#### Health (1)
| メソッド | パス | 3B |
|---|---|---|
| GET | `/health` | [ ] |

#### JQuants Proxy (7)
| メソッド | パス | 3B |
|---|---|---|
| GET | `/api/jquants/auth/status` | [ ] |
| GET | `/api/jquants/daily-quotes` | [ ] |
| GET | `/api/jquants/indices` | [ ] |
| GET | `/api/jquants/listed-info` | [ ] |
| GET | `/api/jquants/statements` | [ ] |
| GET | `/api/jquants/stocks/{symbol}/margin-interest` | [ ] |
| GET | `/api/jquants/topix` | [ ] |

#### Chart (5)
| メソッド | パス | 3B |
|---|---|---|
| GET | `/api/chart/indices` | [ ] |
| GET | `/api/chart/indices/topix` | [ ] |
| GET | `/api/chart/indices/{code}` | [ ] |
| GET | `/api/chart/stocks/search` | [ ] |
| GET | `/api/chart/stocks/{symbol}` | [ ] |

#### Analytics (9)
| メソッド | パス | 3B |
|---|---|---|
| GET | `/api/analytics/factor-regression/{symbol}` | [ ] |
| GET | `/api/analytics/fundamentals/{symbol}` | [ ] |
| GET | `/api/analytics/portfolio-factor-regression/{portfolioId}` | [ ] |
| GET | `/api/analytics/ranking` | [ ] |
| GET | `/api/analytics/roe` | [ ] |
| GET | `/api/analytics/screening` | [ ] |
| GET | `/api/analytics/sector-stocks` | [ ] |
| GET | `/api/analytics/stocks/{symbol}/margin-pressure` | [ ] |
| GET | `/api/analytics/stocks/{symbol}/margin-ratio` | [ ] |

#### Market Data (3)
| メソッド | パス | 3B |
|---|---|---|
| GET | `/api/market/stocks` | [ ] |
| GET | `/api/market/stocks/{code}/ohlcv` | [ ] |
| GET | `/api/market/topix` | [ ] |

#### Database (6)
| メソッド | パス | 3D |
|---|---|---|
| GET | `/api/db/stats` | [ ] |
| POST | `/api/db/stocks/refresh` | [ ] |
| POST | `/api/db/sync` | [ ] |
| GET | `/api/db/sync/jobs/{jobId}` | [ ] |
| DELETE | `/api/db/sync/jobs/{jobId}` | [ ] |
| GET | `/api/db/validate` | [ ] |

#### Dataset (9)
| メソッド | パス | 3D |
|---|---|---|
| GET | `/api/dataset` | [ ] |
| POST | `/api/dataset` | [ ] |
| POST | `/api/dataset/resume` | [ ] |
| GET | `/api/dataset/jobs/{jobId}` | [ ] |
| DELETE | `/api/dataset/jobs/{jobId}` | [ ] |
| GET | `/api/dataset/{name}/info` | [ ] |
| GET | `/api/dataset/{name}/sample` | [ ] |
| GET | `/api/dataset/{name}/search` | [ ] |
| DELETE | `/api/dataset/{name}` | [ ] |

#### Dataset Data (15)
| メソッド | パス | 3D |
|---|---|---|
| GET | `/api/dataset/{name}/stocks` | [ ] |
| GET | `/api/dataset/{name}/stocks/{code}/ohlcv` | [ ] |
| GET | `/api/dataset/{name}/stocks/ohlcv/batch` | [ ] |
| GET | `/api/dataset/{name}/topix` | [ ] |
| GET | `/api/dataset/{name}/indices` | [ ] |
| GET | `/api/dataset/{name}/indices/{code}` | [ ] |
| GET | `/api/dataset/{name}/margin` | [ ] |
| GET | `/api/dataset/{name}/margin/{code}` | [ ] |
| GET | `/api/dataset/{name}/margin/batch` | [ ] |
| GET | `/api/dataset/{name}/statements/{code}` | [ ] |
| GET | `/api/dataset/{name}/statements/batch` | [ ] |
| GET | `/api/dataset/{name}/sectors` | [ ] |
| GET | `/api/dataset/{name}/sectors/mapping` | [ ] |
| GET | `/api/dataset/{name}/sectors/stock-mapping` | [ ] |
| GET | `/api/dataset/{name}/sectors/{sectorName}/stocks` | [ ] |

#### Portfolio (12)
| メソッド | パス | 3E |
|---|---|---|
| GET | `/api/portfolio` | [ ] |
| POST | `/api/portfolio` | [ ] |
| GET | `/api/portfolio/{id}` | [ ] |
| PUT | `/api/portfolio/{id}` | [ ] |
| DELETE | `/api/portfolio/{id}` | [ ] |
| POST | `/api/portfolio/{id}/items` | [ ] |
| PUT | `/api/portfolio/{id}/items/{itemId}` | [ ] |
| DELETE | `/api/portfolio/{id}/items/{itemId}` | [ ] |
| GET | `/api/portfolio/{id}/performance` | [ ] |
| GET | `/api/portfolio/{name}/codes` | [ ] |
| PUT | `/api/portfolio/{portfolioName}/stocks/{code}` | [ ] |
| DELETE | `/api/portfolio/{portfolioName}/stocks/{code}` | [ ] |

#### Watchlist (8)
| メソッド | パス | 3E |
|---|---|---|
| GET | `/api/watchlist` | [ ] |
| POST | `/api/watchlist` | [ ] |
| GET | `/api/watchlist/{id}` | [ ] |
| PUT | `/api/watchlist/{id}` | [ ] |
| DELETE | `/api/watchlist/{id}` | [ ] |
| POST | `/api/watchlist/{id}/items` | [ ] |
| DELETE | `/api/watchlist/{id}/items/{itemId}` | [ ] |
| GET | `/api/watchlist/{id}/prices` | [ ] |

### 主要リスクと対策

| リスク | 対策 |
|---|---|
| Bun/Drizzle 依存の移植コスト | 3C で sqlite3 + Pydantic で段階的に再実装 |
| SQLite スキーマ互換の破壊 | contracts/ スキーマを正とし、Drizzle との読取互換テスト |
| JQuants API の認証・レート制限差異 | Python JQuants クライアントの事前検証（Phase 2B） |
| 長時間ジョブの安定運用 | ジョブライフサイクルの結合テスト（Phase 3D Go/No-Go） |
| 既存クライアントのレスポンス互換性 | 全エンドポイントのレスポンス互換テスト |

### 成功条件

- Hono を停止しても 75 エンドポイント全てが FastAPI で同一動作
- DB ファイルを共有したままデータ互換が維持
- フロントエンドが API URL 変更なしで動作
- OpenAPI 契約の差分がゼロ

---

## Phase 4: パッケージ分離

**期間**: 4-6 週 | **リスク**: Medium

*元: packages-responsibility-roadmap.md Phase 2-5*

### 目標像: パッケージ責務テーブル

#### コア契約と型
| パッケージ | 責務 | 移行元 |
|---|---|---|
| `packages/contracts` | JSON Schema / OpenAPI からの型生成、バージョニング | `contracts/`, `apps/ts/packages/shared`, `apps/bt/src/models` |
| `packages/strategy-config` | strategy-config の読み書き・検証 | `contracts/strategy-config-v1.schema.json`, `apps/bt/src/strategy_config` |

#### データアクセス (TS)
| パッケージ | 責務 | 移行元 |
|---|---|---|
| `packages/market-db-ts` | market.db 読み取り API | `apps/ts/packages/shared/src/db` |
| `packages/dataset-db-ts` | dataset.db 読み取り API + snapshot/manifest | `apps/ts/packages/shared/src/dataset` |
| `packages/portfolio-db-ts` | portfolio/watchlist DB 操作 | `apps/ts/packages/shared/src/portfolio`, `watchlist` |

#### ドメインロジック (TS)
| パッケージ | 責務 | 移行元 |
|---|---|---|
| `packages/analytics-ts` | factor-regression / screening / ranking | `apps/ts/packages/shared/src/factor-regression`, `screening`, `services` |
| `packages/market-sync-ts` | market 同期・検証・ジョブ制御 | `apps/ts/packages/shared/src/market-sync` |
| `packages/clients-ts` | bt/ts API クライアント | `apps/ts/packages/shared/src/clients` |

#### データアクセス (Python)
| パッケージ | 責務 | 移行元 |
|---|---|---|
| `packages/market-db-py` | market.db / dataset.db の読み取り | `apps/bt/src/data`, `apps/bt/src/api` |
| `packages/dataset-io-py` | snapshot/manifest の読み書き | `apps/bt/src/data` |

#### ドメインロジック (Python)
| パッケージ | 責務 | 移行元 |
|---|---|---|
| `packages/indicators-py` | indicator 計算のコアロジック | `apps/bt/src/utils/indicators.py`, `apps/bt/src/server/services` |
| `packages/backtest-core` | backtest 実行エンジン | `apps/bt/src/backtest`, `apps/bt/src/strategies` |
| `packages/strategy-runtime` | strategy config 読み取りと実行 | `apps/bt/src/strategy_config`, `apps/bt/src/strategies` |

### 4A: TS データアクセス層

*元: packages-responsibility-roadmap.md Phase 2*

- [ ] `market-db-ts`, `dataset-db-ts`, `portfolio-db-ts` を作成
- [ ] `apps/ts/packages/shared/src/db`, `dataset`, `portfolio`, `watchlist` のロジック移動
- [ ] `apps/ts/packages/api` が packages 経由で DB にアクセスするよう変更

**完了条件**: `apps/ts/packages/api` が DB 直接実装を持たない

### 4B: TS ドメインロジック

*元: packages-responsibility-roadmap.md Phase 3*

- [ ] `analytics-ts`, `market-sync-ts`, `clients-ts` を作成
- [ ] `apps/ts/packages/shared` を薄いファサードに縮小
- [ ] `apps/ts/packages/cli` と `apps/ts/packages/api` の依存先を packages に切替

**完了条件**: `apps/ts/packages/shared` が再エクスポート中心

### 4C: Python パッケージ

*元: packages-responsibility-roadmap.md Phase 4*

- [ ] `market-db-py`, `dataset-io-py` を作成し `apps/bt/src/data` を分割
- [ ] `indicators-py` を作成し indicator 計算を集約
- [ ] `backtest-core` を作成し backtest 実行系を切り出し

**完了条件**: `apps/bt/src/server` と `apps/bt/src/cli_*` が packages 経由で計算

### 4D: クリーンアップ

*元: packages-responsibility-roadmap.md Phase 5*

- [ ] `apps/ts/packages/shared` の不要モジュール削除
- [ ] `apps/bt/src` の重複ロジック削減
- [ ] CI で packages 単体テストと apps 結合テストの段階実行

**完了条件**: apps/ 配下に残るのは entrypoint と thin adapter のみ

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

### FastAPI 移行時の DB 実装計画

- Python 標準の `sqlite3`（または async 対応の `aiosqlite`）
- Pydantic モデルで行バリデーション（既存資産活用）
- `contracts/dataset-db-schema-v2.json` を正として整合性検証
- WAL モード pragma で読み取り並行性確保

---

## Appendix B: オープン Issue マップ

### bt 系

| Issue | 概要 | 関連 Phase |
|---|---|---|
| bt-016 | テストカバレッジ 70% | 1C |
| bt-017 | signal registry param key validation | 1C |
| bt-018 | pyright pandas type errors | 1C |
| bt-019 | resample compatibility test todo | 1C |
| bt-020 | pydantic field example deprecation | 1C |

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
| ts-116 | bt optimization HTML schema | — |
| ts-117 | coverage api 75/65 | — |
| ts-118 | fundamentals integration test | 3B |
| ts-119 | lab result runtime validation | — |
| ts-120 | lab results error boundary | — |
| ts-121 | market endpoint scope clarify | 1C |
| ts-122 | screening logic single source | 1C |
| ts-123 | remove deprecated fundamentals data service | 1C |

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
| 4B: TS ドメインロジック | packages-responsibility-roadmap.md | Phase 3 |
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

| ファイル | 内容 | 状態（統合時点） |
|---|---|---|
| `archive/monorepo-integration.md` | モノレポ統合・API 責務分離の検討メモ | 決定事項のみ（完了） |
| `archive/monorepo-migration-plan.md` | Monorepo Migration Plan | Phase 1 完了、2-4 未着手 |
| `archive/hono-to-fastapi-migration-roadmap.md` | Hono→FastAPI 完全一本化ロードマップ | 全 Phase 未着手 |
| `archive/packages-responsibility-roadmap.md` | packages/ 責務分割ロードマップ | 全 Phase 未着手 |
| `archive/plan-ta-consolidation.md` | TA 計算エンジン統合計画 | Phase 0-3.5 完了、4 未着手 |

### 独立レポート（アーカイブ対象外）

- `reports/phase2_5_verification_report.md` — TA 並走検証レポート（Phase 2.5 の成果物）
