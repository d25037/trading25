---
id: bt-031
title: "apps/bt/src 再編: 機能境界ベースのパッケージ構造へ移行"
status: done
priority: medium
labels: [architecture, refactor, bt]
project: bt
created: 2026-02-23
updated: 2026-02-24
depends_on: []
blocks: []
parent: null
---

# bt-031 apps/bt/src 再編: 機能境界ベースのパッケージ構造へ移行

## 背景（現状整理）
`apps/bt/src` は機能別のまとまりが部分的に存在する一方で、
- **レイヤと責務が混在したトップレベル構成**（`server/`, `api/`, `data/`, `lib/`, `models/`, `strategies/` が横並び）
- **同種責務の分散**（例: `api/models.py`, `models/`, `server/schemas/` に型定義が分散）
- **命名規則の不統一**（`server/*` vs `cli_bt/*` vs `strategy_config/*`）
- **境界の曖昧さ**（`data/` と `lib/` の違いがディレクトリ名だけでは読み取りづらい）
があり、参入時の探索コスト・変更時の影響範囲判断コストが高い。

参考観測（2026-02-23 時点）:
- ディレクトリ別 Python ファイル数は `server=102`, `strategies=38`, `lib=25`, `data=19`, `agent=19` などで、特に `server` 側に機能横断コードが集中。
- 実行入口（CLI/API）とドメインロジックがトップレベルで同一階層に並び、責務境界が読み取りにくい。

## 問題判定
**実際に問題あり（要リファクタリング）** と判定する。

理由:
1. 機能追加時に「どこへ置くべきか」の判断コストが高い。
2. 型・スキーマ・DTO の重複/分散により、仕様変更時に更新漏れが起きやすい。
3. API/CLI/Batch など入口の違いとドメイン責務が混ざり、テスト粒度の最適化が難しい。
4. `server` の肥大化が進むと、FastAPI 層の変更がドメイン変更に波及しやすい。

## 目的
- `src` を **機能境界（bounded context）優先** で再編し、
  - 入口層（entrypoints）
  - アプリケーション層（usecase/service）
  - ドメイン層（strategy/backtest/analytics）
  - インフラ層（db/api/client/repository）
  を明示化する。
- 既存 API 契約と CLI UX を破壊せず、段階移行可能な形で整理する。

## 提案アーキテクチャ（目標）
例（最終像）:

- `src/entrypoints/`
  - `http/`（現 `server` の router/app/middleware）
  - `cli/`（現 `cli_bt`）
- `src/domains/`
  - `backtest/`
  - `strategy/`
  - `optimization/`
  - `analytics/`
  - `lab_agent/`
- `src/infrastructure/`
  - `db/`（market/dataset/portfolio への access）
  - `external_api/`（JQuants/FastAPI client）
  - `repository/`
- `src/shared/`
  - `models/`（共通型）
  - `config/` / `paths/` / `utils/`

> 注: 一括移動ではなく、import 互換を保つ shim を使った段階移行を前提とする。

## 実施計画（段階的）
### Phase 0: 可視化とガードレール
- 依存関係（import graph）と循環参照を可視化。
- パッケージ境界ルール（例: `entrypoints -> domains -> infrastructure/shared`）を定義。
- PR ごとの移動上限（ファイル数）を設定。

### Phase 1: 入口層の分離
- `server` を `entrypoints/http` に集約（まずは移設 + re-export）。
- `cli_bt` を `entrypoints/cli` に移設。
- 実行コマンド (`uv run bt ...`) の import 互換を維持。

### Phase 2: 型とスキーマの統合
- `models/`, `api/models.py`, `server/schemas/` の責務を再定義。
- 共通 DTO / API schema / domain model の3分類に整理。
- OpenAPI 生成/契約更新フローを CI で固定。

### Phase 3: data/lib/api の再編
- `data/` と `lib/` の責務を棚卸しし、
  - domain ロジックは `domains/*`
  - I/O は `infrastructure/*`
 へ移す。
- `api/` は external client と internal app service を分離。

### Phase 4: 完了処理
- 旧 import パスの shim を削除。
- ドキュメント（構成図・追加時ガイド）更新。
- 代表ユースケース（backtest, optimize, screening）で回帰確認。

## 受け入れ条件
- [x] `src` のトップレベルが「責務名ベース」の固定セットに収束している。
- [x] 新規開発者向けに「どこに何を書くか」ガイドが docs に追加されている。
- [x] API/CLI の既存挙動が E2E / integration テストで維持される。
- [x] import 循環が現状より減少（またはゼロ維持）している。
- [x] OpenAPI 契約と `contracts/` の整合が CI で確認できる。

## リスクと対策
- **リスク**: 大規模移動で差分レビューが困難。
  - **対策**: フェーズごとに PR 分割、移動とロジック変更を同一PRで混在させない。
- **リスク**: import path 破壊。
  - **対策**: 非推奨 shim + 段階削除、リリースノートで告知。
- **リスク**: テスト実行時間増。
  - **対策**: 影響範囲別テストセット（smoke/full）を整備。

## 備考
- 本Issueは「全面書き換え」ではなく、**互換性を維持した漸進リファクタリング**を目的とする。

## 結果
- `apps/bt/src` を `entrypoints / application / domains / infrastructure / shared` の5層へ全面移行し、旧トップレベル境界（`server`, `cli_bt`, `lib`, `api`, `data`, `backtest`, `strategy_config` など）を削除。
- 互換 shim は導入せず、リポジトリ内 import を新パスへ一括更新。
- アーキテクチャガードレールとして `tests/unit/architecture/test_layer_boundaries.py` と `tests/unit/architecture/test_legacy_imports_removed.py` を追加。
- OpenAPI 出力の historical key 整合（`DateRange` / `IndexMatch` / `OHLCVRecord`）を `entrypoints/http/openapi_config.py` で安定化し、baseline 差分 0 を再現可能に修正。
- `bt` CLI エントリポイント、`/doc` 表示、3002表記、strategy default config path 解決を新構成に合わせて更新。
- 検証: `uv run --project apps/bt pytest apps/bt/tests -q`（3605 passed）, `uv run --project apps/bt pyright apps/bt/src`（0 errors/0 warnings）, OpenAPI baseline diff 0 を確認。
