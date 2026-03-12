---
id: bt-043
title: "market / dataset の snapshot resolver を共通化"
status: done
priority: high
labels: [snapshot, market, dataset, resolver, bt]
project: bt
created: 2026-03-08
updated: 2026-03-09
depends_on: [bt-038, bt-039]
blocks: [bt-044, bt-045]
parent: bt-037
---

# bt-043 market / dataset の snapshot resolver を共通化

## 目的
- market plane と dataset plane の入力解決を 1 つの snapshot resolver へ寄せる。
- screening / backtest / optimize / lab が同じ input snapshot contract を共有できるようにする。

## 受け入れ条件
- [x] market latest / market snapshot / dataset snapshot を同一 API で解決できる。
- [x] `dataset.db` 直参照や loader 個別分岐が snapshot resolver 経由へ置き換わる。
- [x] run metadata に解決済み snapshot ID を保存できる。
- [x] 既存 direct mode loader の主要経路に回帰テストがある。

## 実施内容
- [x] snapshot resolver domain/service を追加する。
- [x] `apps/bt/src/infrastructure/data_access/clients.py` と loaders を順次置き換える。
- [x] market / dataset の resolver policy と fallback ルールを定義する。
- [x] docs と settings 説明を更新する。

## 結果
- 2026-03-09: `snapshot_resolver` を追加し、market latest / dataset snapshot を同一 service API (`resolve`) で解決できるようにした。
- 2026-03-09: `infrastructure/data_access/clients.py` の direct mode path 解決を resolver 経由へ統一した。
- 2026-03-09: `run_contracts` の `dataset_snapshot_id` 解決を resolver helper 経由へ寄せた。
- 2026-03-09: `RunSpec` / `RunMetadata` / `CanonicalExecutionResult` に `market_snapshot_id` を追加し、OpenAPI / TS 型まで同期した。
- 2026-03-09: `DirectMarketClient.get_stock_ohlcv()` を追加し、`indicator_service` / `signal_service` / `portfolio_loaders` の market read path を factory 経由へ統一した。
- 2026-03-09: `backtest_attribution_service` の保存メタで market path を resolver 経由に変更し、snapshot ID を保存するようにした。
- 2026-03-09: `GET /api/snapshots/resolve` を追加し、market latest / dataset snapshot を 1 つの HTTP 契約で解決できるようにした。
- 2026-03-09: `indicators/ohlcv` request schema の `source` を snapshot-aware な `str` に揃え、dataset snapshot 名をそのまま受け付けるようにした。
- 2026-03-09: `docs/backtest-greenfield-rebuild.md` と `shared/config/settings.py` に resolver policy / fallback / root path の説明を追記した。

## 補足
- 参照: `docs/backtest-greenfield-rebuild.md` Section 4.2, 4.3, 10
