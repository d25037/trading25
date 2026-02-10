# Phase 4 パッケージ構造レビュー

作成日: 2026-02-09

## 概要

Phase 4「パッケージ分離」の実装が、元の計画（`packages-responsibility-roadmap.md`）と異なる場所に作成されている。本ドキュメントは現状を整理し、今後の方針判断の材料とする。

---

## 元の計画 vs 実態

### 元の計画（`docs/archive/packages-responsibility-roadmap.md`）

基本原則: **`apps/` はデプロイ単位、`packages/` は再利用可能なライブラリ単位**

```
packages/
  contracts/          # JSON Schema / OpenAPI 型生成
  strategy-config/    # strategy-config 読み書き・検証
  market-db-ts/       # market.db 読み取り API (TS)
  dataset-db-ts/      # dataset.db 読み取り API (TS)
  portfolio-db-ts/    # portfolio/watchlist DB 操作 (TS)
  analytics-ts/       # factor-regression / screening (TS)
  market-sync-ts/     # market 同期・検証 (TS)
  clients-ts/         # bt/ts API クライアント (TS)
  market-db-py/       # market.db / dataset.db 読み取り (Python)
  dataset-io-py/      # snapshot/manifest 読み書き (Python)
  indicators-py/      # indicator 計算コアロジック (Python)
  backtest-core/      # backtest 実行エンジン (Python)
  strategy-runtime/   # strategy config 読み取りと実行 (Python)
```

依存方向: `apps -> packages`、`packages -> packages` のみ許可。`packages -> apps` は禁止。

### 実態（2026-02-09 時点）

| 計画上のパス | 実際のパス | 状態 |
|---|---|---|
| `packages/contracts` | `contracts/`（直接管理） | Phase 2A で延期決定 |
| `packages/strategy-config` | 未着手 | — |
| `packages/market-db-ts` | `apps/ts/packages/market-db-ts` | 4A で作成 |
| `packages/dataset-db-ts` | `apps/ts/packages/dataset-db-ts` | 4A で作成 |
| `packages/portfolio-db-ts` | `apps/ts/packages/portfolio-db-ts` | 4A で作成 |
| `packages/analytics-ts` | 作成しない（削除方針） | 4B で方針転換 |
| `packages/market-sync-ts` | 作成しない（削除方針） | 4B で方針転換 |
| `packages/clients-ts` | `apps/ts/packages/clients-ts` | 4A で作成 |
| `packages/market-db-py` | `apps/bt/src/lib/market_db` | 4C Step1 で作成 |
| `packages/dataset-io-py` | `apps/bt/src/lib/dataset_io` | 4C Step1 で作成 |
| `packages/indicators-py` | 未着手 | 4C Step2 対象 |
| `packages/backtest-core` | 未着手 | 4C Step2 対象 |
| `packages/strategy-runtime` | 未着手 | 4C Step2 対象 |

ルートの `packages/` は README のみで空。

---

## 乖離の原因

`unified-roadmap.md` の Phase 4 再ベースライン（2026-02-09）で意図的に変更された:

> 実装初期は Python 側を `apps/bt/src/lib/*` で分離し、外部配布形式（別 repo/package 化）は Phase 4 完了後に判断する。

TS 側も明示的な説明なく `apps/ts/packages/` 内に作成された。

### 背景: Phase 3 完了による前提の変化

元の計画（Phase 3 開始前）:
- Hono と FastAPI が併存 → bt/ts 間でコード共有の可能性あり
- `packages/` にルートレベルで置くことに意義があった

Phase 3F 完了後の現実:
- **FastAPI が唯一のバックエンド** — bt/ts 間のコード共有は OpenAPI / `contracts/` 経由のみ
- TS から Python コードを直接使う場面がない
- Python から TS コードを直接使う場面がない

→ ルート `packages/` に言語横断で配置する本来の意義が消失。

---

## 各配置の評価

### TS 側: `apps/ts/packages/*`

**実害は小さい。**

- bun workspace packages として正常に機能（`@trading25/market-db-ts` 等で参照可能）
- `package.json`、独自テスト、明確な公開境界（`__init__.ts` / `index.ts`）を持つ
- `apps/ts/packages/web` と `cli` から workspace 依存として参照されている
- ルート `packages/` に移動しても、bun workspace の設定変更以外の実質的な差はない

**ただし**: 元の計画の「apps の外に出す」という原則には違反。

### Python 側: `apps/bt/src/lib/*`

**パッケージ分離としては不十分。**

- 独立した `pyproject.toml` がない（`apps/bt` の一部としてのみ存在）
- 独立したテストスイートがない（`apps/bt/tests/` に混在）
- バージョニング不可（独立配布できない）
- 実質的には「ディレクトリ移動 + import パス変更」の内部リファクタリング

**ただし**: Python にはbun workspace のようなネイティブなモノレポパッケージ管理の仕組みがない。`uv workspace` は存在するが、このプロジェクトでは未採用。

---

## 選択肢

### A. 現状追認（計画を実態に合わせる）

ルート `packages/` の計画を撤回し、各 app 内でのパッケージ分離を正式な方針とする。

**メリット**:
- 移動コストなし
- 現状の workspace 設定が崩れない
- bt/ts 間のコード共有が不要な現実に即している

**デメリット**:
- 元の「再利用可能なライブラリ」という設計原則を放棄
- Python 側の分離が「ディレクトリ整理」レベルにとどまる

**必要な作業**:
1. `packages/README.md` の更新（「move here over time」の撤回）
2. `unified-roadmap.md` で方針変更の明記
3. `archive/packages-responsibility-roadmap.md` にアーカイブ注記

### B. 元の計画に戻す（ルート `packages/` に移動）

TS パッケージを `packages/` に、Python パッケージも `packages/` に独立パッケージとして切り出す。

**メリット**:
- 元の設計原則に忠実
- パッケージの独立性が高まる（個別テスト、個別バージョニング）
- 将来の別リポジトリ分離が容易

**デメリット**:
- 移動コストが大きい（workspace 設定、import パス、CI 全て要更新）
- Python 側は `uv workspace` 等の追加ツール導入が必要
- 現時点で bt/ts 横断の再利用ニーズがない

**必要な作業**:
1. TS: `apps/ts/packages/{clients,market-db,dataset-db,portfolio-db}-ts` → `packages/` に移動
2. TS: `apps/ts/bun.lockb`、`apps/ts/package.json` の workspace 設定更新
3. Python: `apps/bt/src/lib/*` → `packages/` に独立 `pyproject.toml` 付きで移動
4. Python: `uv workspace` 導入 or パス依存設定
5. CI 更新

### C. ハイブリッド（TS は現状維持、Python を改善）

TS 側は `apps/ts/packages/` で機能しているため現状維持。Python 側は `apps/bt/src/lib/` のままだが、独立テストと明確な公開境界を追加して「論理パッケージ」としての品質を上げる。

**メリット**:
- 実利とコストのバランスが良い
- TS 側の動作中の workspace を壊さない
- Python 側の分離品質は向上

**デメリット**:
- ルート `packages/` は使わないことになる
- 中途半端な状態が残る

---

## 推奨: A（現状追認）

理由:
1. **FastAPI 一本化後、bt/ts 間でコードを直接共有する場面がない** — ルート `packages/` に置く最大の意義が消失
2. **TS workspace packages は `apps/ts/packages/` で正常に機能している** — 移動は cosmetic change
3. **Python にはモノレポパッケージの成熟した仕組みがない** — 無理にルートに出すよりも `src/lib/` で十分
4. **移動コストに対して得られるものが少ない** — Phase 4 の残タスク（4B 削除、4C Step2、4D クリーンアップ）に集中すべき

ルート `packages/` は将来的に本当に言語横断の共有ニーズが生じた場合にのみ使用する。
