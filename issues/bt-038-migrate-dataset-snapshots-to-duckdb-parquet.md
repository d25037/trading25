---
id: bt-038
title: "Dataset snapshot SoT を dataset.duckdb + parquet へ移行"
status: done
priority: high
labels: [dataset, duckdb, parquet, migration, bt]
project: bt
created: 2026-03-08
updated: 2026-03-09
depends_on: [bt-028]
blocks: [bt-043]
parent: bt-037
---

# bt-038 Dataset snapshot SoT を dataset.duckdb + parquet へ移行

## 目的
- dataset snapshot の SoT を legacy `dataset.db` から `dataset.duckdb + parquet + manifest` へ移す。
- market plane と dataset plane の物理フォーマットと query model を揃える。

## 受け入れ条件
- [x] dataset create/resume が `datasets/{name}/dataset.duckdb` と `parquet/*.parquet` を出力する。
- [x] manifest に snapshot schema/version/count/checksum/source 情報が揃う。
- [x] 既存 `DatasetDb` 読み出し経路は移行期間中 compatibility artifact として継続できる。
- [x] backtest / dataset API / validation テストが新 snapshot 形式で通る。

## 実施内容
- [x] dataset writer を DuckDB + Parquet 出力へ拡張する。
- [x] `apps/bt/src/infrastructure/db/market/dataset_db.py` の後継 reader を追加する。
- [x] 旧 `dataset.db` との compatibility policy を定義する。
- [x] manifest reader/validator と連携し、異常時は早期失敗させる。

## 結果
- `DatasetWriter` は `dataset.duckdb + parquet` を SoT としつつ、`dataset.db` を compatibility artifact として同時出力するように変更した。
- `DatasetResolver` / direct access client は snapshot directory を優先解決し、legacy flat `*.db` を fallback にした。
- `DatasetSnapshotReader` を追加し、manifest / checksum 検証を通した上で DuckDB を直接 query する read path へ置換した。
- manifest shape は strict Pydantic validation と `contracts/dataset-snapshot-manifest-v1.schema.json` で固定した。
- dataset API / builder / validation / direct mode の targeted tests は新 snapshot 形式で通過した。

## 補足
- `bt-028` の manifest reader / schema validation を前提に進める。
- 参照: `docs/backtest-greenfield-rebuild.md` Section 4.3, 10
