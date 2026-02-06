# packages/ 責務分割ロードマップ

作成日: 2026-02-05

## 背景
モノレポ化により `apps/ts` と `apps/bt` は同一リポジトリに統合されたが、機能の大半が `apps/` 配下に集約されている。  
再利用性・変更影響の局所化・責務の明確化のため、`packages/` に責任分割する段階的なロードマップを定める。

## 参照ドキュメント
- `docs/monorepo-integration.md`
- `docs/monorepo-migration-plan.md`
- `docs/plan-ta-consolidation.md`
- `docs/hono-to-fastapi-migration-roadmap.md`

## 基本方針
- `apps/` は「実行・デプロイ単位」、`packages/` は「再利用可能なライブラリ単位」。
- 依存方向は `packages -> packages` と `apps -> packages` のみを許可し、`packages -> apps` を禁止する。
- JQuants API は `apps/ts` が唯一の窓口である方針を維持する。
- dataset 境界はスナップショット形式を前提とし、スキーマは `contracts/` を単一の真実として扱う。
- API 呼び出し方向は `docs/monorepo-integration.md` のパターンAを維持する。

## 目標像 (責務の再配置)

### コア契約と型
| パッケージ | 責務 | 主な移行元 / 利用先 |
|---|---|---|
| `packages/contracts` | JSON Schema / OpenAPI からの型生成、バージョニング | `contracts/`, `apps/ts/packages/shared`, `apps/bt/src/models` |
| `packages/strategy-config` | strategy-config の読み書き・検証 | `contracts/strategy-config-v1.schema.json`, `apps/bt/src/strategy_config` |

### データアクセス (TS)
| パッケージ | 責務 | 主な移行元 / 利用先 |
|---|---|---|
| `packages/market-db-ts` | market.db 読み取り API | `apps/ts/packages/shared/src/db` |
| `packages/dataset-db-ts` | dataset.db 読み取り API + snapshot/manifest | `apps/ts/packages/shared/src/dataset` |
| `packages/portfolio-db-ts` | portfolio/watchlist DB 操作 | `apps/ts/packages/shared/src/portfolio`, `watchlist` |

### ドメインロジック (TS)
| パッケージ | 責務 | 主な移行元 / 利用先 |
|---|---|---|
| `packages/analytics-ts` | factor-regression / screening / ranking | `apps/ts/packages/shared/src/factor-regression`, `screening`, `services` |
| `packages/market-sync-ts` | market 同期・検証・ジョブ制御 | `apps/ts/packages/shared/src/market-sync` |
| `packages/clients-ts` | bt/ts API クライアント | `apps/ts/packages/shared/src/clients` |

### データアクセス (Python)
| パッケージ | 責務 | 主な移行元 / 利用先 |
|---|---|---|
| `packages/market-db-py` | market.db / dataset.db の読み取り | `apps/bt/src/data`, `apps/bt/src/api` |
| `packages/dataset-io-py` | snapshot/manifest の読み書き | `apps/bt/src/data` |

### ドメインロジック (Python)
| パッケージ | 責務 | 主な移行元 / 利用先 |
|---|---|---|
| `packages/indicators-py` | indicator 計算のコアロジック | `apps/bt/src/utils/indicators.py`, `apps/bt/src/server/services` |
| `packages/backtest-core` | backtest 実行エンジン | `apps/bt/src/backtest`, `apps/bt/src/strategies` |
| `packages/strategy-runtime` | strategy config 読み取りと実行 | `apps/bt/src/strategy_config`, `apps/bt/src/strategies` |

## ロードマップ

### Phase 0: 境界定義と最小ツール整備
目的: packages 追加の前提を整える。
- `packages/README.md` に命名規則・依存規則・公開ポリシーを記載
- 依存関係の可視化 (apps ↔ packages の簡易図)
- 型生成とスキーマ検証の方針を固定

完了条件:
- 依存方向ルールがドキュメント化されている
- 新規 packages 作成時のテンプレートが決まっている

### Phase 1: 契約と型の切り出し (低リスク)
目的: 共有境界を先に固定する。
- `packages/contracts` を作成し、`contracts/` の JSON Schema を参照する
- TS と Python の型生成 or 手動同期のルールを決定
- `apps/ts` と `apps/bt` の型参照先を `packages/contracts` に寄せる

完了条件:
- dataset / strategy-config の型定義が `packages/contracts` に一本化されている
- `contracts/` と型生成物の差分チェックが実行できる

### Phase 2: TS データアクセス層の分離
目的: Hono/FastAPI の移行と独立に DB 操作を分離する。
- `market-db-ts`, `dataset-db-ts`, `portfolio-db-ts` を作成
- `apps/ts/packages/shared/src/db`, `dataset`, `portfolio`, `watchlist` のロジックを移動
- `apps/ts/packages/api` は packages を経由して DB にアクセス

完了条件:
- `apps/ts/packages/api` が DB 直接実装を持たない
- DB 操作に関するユニットテストが packages 側で完結する

### Phase 3: TS ドメインロジックの分離
目的: CLI/API/Web から共有ロジックを再利用可能にする。
- `analytics-ts`, `market-sync-ts`, `clients-ts` を作成
- `apps/ts/packages/shared` を薄いファサードに縮小
- `apps/ts/packages/cli` と `apps/ts/packages/api` の依存先を packages に切替

完了条件:
- `apps/ts/packages/shared` が再エクスポート中心になっている
- analytics/screening/factor-regression のテストが packages 側に移行している

### Phase 4: Python 側の分離
目的: bt の CLI / API / backtest の責務分割を明確化する。
- `market-db-py`, `dataset-io-py` を作成し `apps/bt/src/data` を分割
- `indicators-py` を作成し indicator の計算を集約
- `backtest-core` を作成し backtest 実行系を切り出し

完了条件:
- `apps/bt/src/server` と `apps/bt/src/cli_*` が packages を経由して計算を行う
- indicator 計算の単一実装が packages 化されている

### Phase 5: クリーンアップと互換性維持
目的: packages 移行後の整理と互換性の担保。
- `apps/ts/packages/shared` の不要モジュール削除
- `apps/bt/src` の重複ロジック削減
- CI で packages 単体テストと apps 結合テストの段階実行

完了条件:
- apps/ 配下に残るのは entrypoint と thin adapter のみ
- packages 単体で回るテストが CI に組み込まれている

## 非対象 (このロードマップに含めない)
- Hono -> FastAPI 一本化の具体実装手順 (別ドキュメントにて管理)
- JQuants API のアクセス窓口の変更
- API パス互換性の全面変更

## 進捗管理のルール
- Phase ごとに Issue ラベルを統一する (`pkg-0x`, `pkg-1x` など)
- packages 追加は必ず README と依存関係図を更新する
- 破壊的変更は `contracts/` のバージョンを必須で更新する
