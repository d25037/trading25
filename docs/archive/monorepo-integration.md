# モノレポ統合とAPI責務分離の検討メモ

## 背景
- 現状は `apps/ts/` と `apps/bt/` が別リポジトリで強く相互依存している。
- Hono(3001) と FastAPI(3002) が相互に呼び合う循環が発生している。
- dataset のスキーマ変更が頻繁にあり、bt 側への追随コストが高い。

## 結論サマリ
- モノレポ統合は有力（同時変更・契約変更の管理が容易）。
- Hono サーバー削除の一本化は現時点では非推奨（TS資産の再実装コストが大きい）。
- 循環は「呼び出し方向の固定」で解消可能。
- dataset は“安定境界”を用意しない限り、bt 追随コストが高い。

## モノレポ化の方向性
### 1) 本来のモノレポ（採用）
- `trading25/` を単一 Git リポジトリとして管理。
- `apps/bt/` と `apps/ts/` の `.git` は削除し、普通のサブディレクトリとして扱う。

### 2) 採用した構造（apps/packages）
```
/
├── apps/
│   ├── ts/
│   └── bt/
├── contracts/
├── packages/
├── docs/
└── README.md
```

**apps と packages の意味**
- apps: サーバー/CLIなどの実行・デプロイ単位
- packages: 共有ロジック・契約・SDKなど再利用基盤

## Hono削除の一本化について
- 現時点では非推奨。
- apps/ts 側の shared/cli/DB 運用ロジックが大きく、FastAPI への移譲コストが高い。
- まず循環を止める方が効果的。

## 循環依存の解消（呼び出し方向固定）
### パターンA（最小変更）
- front/CLI → apps/ts（データ系）
- front/CLI → apps/bt（バックテスト・計算系）
- apps/bt → apps/ts（dataset 読み込み）
- apps/ts → apps/bt を撤去（analytics proxy などを削る）

### パターンB（apps/ts を唯一の入口にする）
- front/CLI → apps/ts → apps/bt
- apps/bt → apps/ts を禁止
- apps/bt は dataset を直接読む必要あり（スナップショット or 共有ストレージ）

## dataset スキーマ変更が頻繁な場合の対応
### A) 安定API契約（短期で現実的）
- `/api/dataset/v1` の固定契約を用意。
- apps/ts 内部の変更は v1 互換アダプタで吸収。
- 破壊的変更は v2 として分離。

### B) スナップショット形式（中期で安定）
- apps/ts が apps/bt 用に安定スキーマで出力（SQLite/Parquet/Arrow + manifest）。
- apps/bt は出力フォーマットだけに依存。

### C) bt 側でバージョン別アダプタ
- dataset に `schema_version` を持たせ、apps/bt が読み分ける。
- コストは下がるが、apps/bt 側の変更は残る。

## 名前の議論（bt → py 変更）
- AI理解の観点では rename は不要。
- `py` は意味が広く曖昧化しやすい。
- 変えるなら `backtest` が最も誤解が少ない。

## 決定事項
- モノレポ統合方法: 履歴破棄（クリーンスタート）
- 構成: `apps/` + `packages/` + `contracts/`
- API 呼び出し方向の固定: パターン A
- dataset 境界の方式: スナップショット形式

---

## Claude の見解（2025-02-04）

### 総評
Codex の分析は妥当。以下に補足と推奨を記載する。

### 1. モノレポ統合：賛成
- 現状の `trading25/` 構造は事実上のモノレポ。正式統合は自然な流れ。
- **推奨**: 個人プロジェクトであれば履歴破棄（クリーンスタート）で十分。

### 2. Hono 削除：Codex に同意、ただし条件付き
- 「現時点では非推奨」に同意。
- 理由: `apps/ts/shared/` の DB 操作、JQUANTS-API 連携、CLI 運用ロジックは TS で書かれており、Python 移植コストが高い。
- **ただし長期的には一本化が望ましい**。2 つの API server 維持は運用負荷が高く、循環の根本原因になりやすい。

### 3. 循環解消：パターン A を推奨
```
front/CLI → apps/ts（データ系）
front/CLI → apps/bt（バックテスト系）
apps/bt → apps/ts（dataset 読み込み）
apps/ts → apps/bt を撤去
```
- 最小変更で効果的。
- パターン B は ts がボトルネック化するリスクがある。バックテスト系は直接 bt を呼ぶ方がシンプル。

### 4. dataset スキーマ問題：スナップショット形式（B 案）を推奨
```
apps/ts → Parquet/Arrow 出力 → apps/bt が読む
```
- API 契約（A 案）は結局 ts 側の変更が bt に伝播しやすい。
- スナップショット形式なら bt は「ファイルフォーマット」だけに依存でき、ts 内部の変更から隔離される。
- Python（pandas/polars）は Parquet/Arrow の扱いが得意。

### 5. 追加提案：contracts/ ディレクトリの活用
```
contracts/
├── dataset-schema.json    # bt が期待するスキーマ定義
├── api-types.ts           # 共有型定義
└── api-types.py           # 自動生成 or 手動同期
```
- スキーマ変更が「契約の変更」として明示化され、意図しない破壊的変更を防げる。

### 推奨まとめ
| 項目 | 推奨 |
|------|------|
| モノレポ化 | Yes（履歴破棄で OK） |
| Hono 削除 | 現時点では No、長期的には Yes |
| 循環解消 | パターン A（ts→bt 撤去） |
| dataset 境界 | スナップショット形式（Parquet/Arrow） |

### 次のアクション
1. `apps/ts → apps/bt` の呼び出し箇所を洗い出し、削除可能か確認
2. contracts/ ディレクトリを作成し、スキーマ定義を開始
3. dataset のスナップショット出力機能を apps/ts 側に実装
