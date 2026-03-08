---
id: bt-038
title: "Dataset snapshot SoT を dataset.duckdb + parquet へ移行"
status: open
priority: high
labels: [dataset, duckdb, parquet, migration, bt]
project: bt
created: 2026-03-08
updated: 2026-03-08
depends_on: [bt-028]
blocks: [bt-043]
parent: bt-037
---

# bt-038 Dataset snapshot SoT を dataset.duckdb + parquet へ移行

## 目的
- dataset snapshot の SoT を legacy `dataset.db` から `dataset.duckdb + parquet + manifest` へ移す。
- market plane と dataset plane の物理フォーマットと query model を揃える。

## 受け入れ条件
- [ ] dataset create/resume が `datasets/{name}/dataset.duckdb` と `parquet/*.parquet` を出力する。
- [ ] manifest に snapshot schema/version/count/checksum/source 情報が揃う。
- [ ] 既存 `DatasetDb` 読み出し経路は移行期間中 compatibility artifact として継続できる。
- [ ] backtest / dataset API / validation テストが新 snapshot 形式で通る。

## 実施内容
- [ ] dataset writer を DuckDB + Parquet 出力へ拡張する。
- [ ] `apps/bt/src/infrastructure/db/market/dataset_db.py` の後継 reader を追加する。
- [ ] 旧 `dataset.db` との compatibility policy を定義する。
- [ ] manifest reader/validator と連携し、異常時は早期失敗させる。

## 結果
- 未着手

## 補足
- `bt-028` の manifest reader / schema validation を前提に進める。
- 参照: `docs/backtest-greenfield-rebuild.md` Section 4.3, 10

