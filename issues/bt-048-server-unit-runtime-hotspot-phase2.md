---
id: bt-048
title: "server unit test の残 hotspot を phase2 で削減"
status: in-progress
priority: medium
labels: [bt, testing, performance]
project: bt
created: 2026-03-24
updated: 2026-03-24
depends_on: [bt-047]
blocks: []
parent: null
---

# bt-048 server unit test の残 hotspot を phase2 で削減

## 目的
- `bt-047` で導入した fast 実行導線と `slow` 運用を前提に、`tests/unit/server/**` の残る起動コスト hotspot をさらに削減する。
- FastAPI app / DuckDB / resolver 初期化が不要なテストを、共有 fixture または service/domain 直呼びへ段階的に寄せる。

## 背景
- `bt-047` で `test_analytics_complex.py`、`test_openapi.py`、`test_routes_portfolio_performance.py` などの明確な hotspot は削減できた。
- 一方で `tests/unit/server` 全体にはまだ `create_app()` と `TestClient` の繰り返し初期化に起因する setup-heavy なファイルが残っている。
- この環境では `BT_PYTEST_FAST=1 ./scripts/bt-pytest.sh tests/unit/server` の広域計測が無音のまま長引くため、次フェーズでは chunk ごとの durations 計測で詰める方が安全である。

## 受け入れ条件
- [x] `tests/unit/server` の残 hotspot を chunk 単位で棚卸しし、上位ファイルと主因を issue に記録する。
- [x] 少なくとも 1 つ以上の残 hotspot ファイルで、共有 fixture 化または service/domain 直呼びへの置換を実施する。
- [x] 変更対象の before/after runtime をファイル単位または subset 単位で比較し、回帰確認を残す。
- [x] `bt-047` で整備した `fast` / `slow` / xdist 保留方針と矛盾しない形で進める。

## 実施内容
- [x] `tests/unit/server` を route/service/db などの chunk に分けて durations を再計測する。
- [x] `create_app()` / `TestClient` / 一時 DB 作成を毎テストで繰り返している fixture を候補抽出する。
- [x] 不変な app/schema/market fixture を module 共有に寄せられる箇所から順に最適化する。
- [ ] HTTP 起動が不要なテストは service/domain 直呼びへ移せるかを確認する。
- [ ] 必要なら `apps/bt/docs/test-runtime.md` に新しい hotspot 知見を追記する。

## 結果
- 2026-03-24: chunk ごとの durations を再計測した。`tests/unit/server/routes` は `293 passed in 27.14s`、`tests/unit/server/services` は `606 passed in 10.36s`、`tests/unit/server/db -m 'not slow'` は `248 passed, 1 deselected in 5.16s`、`tests/unit/server/test_*.py` は `480 passed in 42.50s` で、残 hotspot は route/top-level 側に偏っていることを確認した。
- 2026-03-24: `tests/unit/server/test_routes_dataset_data.py` を最適化対象として選び、read-only な dataset snapshot bundle と `TestClient` を module 共有 fixture に寄せた。
- 2026-03-24: `tests/unit/server/test_routes_dataset_data.py` の実行時間は `26 passed in 5.59s` から `26 passed in 0.51s` まで短縮し、重い setup がほぼ 1 回分に集約されることを確認した。
- 2026-03-24: `tests/unit/server/routes/test_jquants_proxy.py` も最適化対象として選び、J-Quants env と `TestClient` 初期化を module 共有 fixture に寄せた。
- 2026-03-24: `tests/unit/server/routes/test_jquants_proxy.py` の実行時間は `18 passed in 3.52s` から `18 passed in 0.32s` まで短縮した。あわせて `test_jquants_proxy.py + test_routes_dataset_data.py` の subset で `44 passed in 0.59s` を確認した。
- 2026-03-24: `tests/unit/server/test_routes_db_sync.py` は mutable DB を共有しない方針を維持し、market DB を毎回生成する代わりに module-scope template DB を 1 回作成して各テストでコピーする形へ変更した。
- 2026-03-24: `tests/unit/server/test_routes_db_sync.py` の実行時間は `25 passed in 3.09s` から `25 passed in 2.62s` へ短縮した。`test_jquants_proxy.py + test_routes_dataset_data.py + test_routes_db_sync.py` の subset では `69 passed in 3.13s` を確認した。
- 2026-03-24: `tests/unit/server/routes/test_market_data.py` は read-only な market DuckDB と env/app 初期化を module 共有 fixture に置き換えた。共通 fixture への依存をやめ、必要最小限の test data をファイル内 helper で閉じた。
- 2026-03-24: `tests/unit/server/routes/test_market_data.py` の実行時間は `22 passed in 5.82s` から `22 passed in 0.23s` へ短縮した。`test_market_data.py + test_jquants_proxy.py + test_routes_dataset_data.py + test_routes_db_sync.py` の subset では `91 passed in 1.97s` を確認した。
- 2026-03-24: pure route テストで app state を持たない `[test_backtest.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/routes/test_backtest.py)`、`[test_optimize.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/routes/test_optimize.py)`、`[test_routes_analytics_fundamentals.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_routes_analytics_fundamentals.py)` を module 共有 `TestClient` に寄せた。
- 2026-03-24: それぞれの実行時間は `test_backtest.py: 37 passed in 1.96s -> 0.27s`、`test_optimize.py: 25 passed in 1.20s -> 0.72s`、`test_routes_analytics_fundamentals.py: 14 passed in 0.59s -> 0.20s` まで短縮した。3 ファイル combined subset では `76 passed in 0.80s` を確認した。
- 2026-03-24: top-level CRUD/portfolio 系の `[test_routes_portfolio.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_routes_portfolio.py)`、`[test_routes_watchlist.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_routes_watchlist.py)`、`[test_routes_watchlist_prices.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_routes_watchlist_prices.py)` を shared app client + per-test DB 注入へ寄せた。watchlist prices は market DB を template-copy 化し、reader の初期化コストも下げた。
- 2026-03-24: `test_routes_portfolio.py` は `30 passed in 1.91s -> 0.61s`、`test_routes_watchlist.py` は `19 passed` を `0.42s`、`test_routes_watchlist_prices.py` は `4 passed` を `0.26s` で確認した。3 ファイル combined subset では `53 passed in 3.40s -> 0.85s` となった。
- 2026-03-24: `[test_routes_portfolio_performance.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_routes_portfolio_performance.py)` と `[test_routes_portfolio_factor_regression.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_routes_portfolio_factor_regression.py)` も shared app client に揃え、前者は `6 passed in 1.59s -> 0.37s`、後者は `6 passed in 1.11s -> 0.45s` まで短縮した。5 ファイル combined subset では `65 passed in 1.40s` を確認した。
- 2026-03-24: `[test_error_format.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_error_format.py)` は `_make_test_app()` を毎回呼んでいた class setup を shared client fixture へ置き換え、`no_db` 系だけは lifespan を走らせない plain `TestClient` を維持した。`26 passed in 0.28s` を確認した。
- 2026-03-24: `[test_routes_dataset.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_routes_dataset.py)` は dataset snapshot を毎回 build する代わりに module-scope template bundle を 1 回作成して各テストで copy する形にし、shared app client と組み合わせた。create job の end-to-end テストだけは専用 app を維持しつつ、market source DuckDB を template-copy 化した。`12 passed in 1.35s -> 0.57s` を確認した。
- 2026-03-24: `[test_routes_dataset_jobs.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_routes_dataset_jobs.py)` は `create_app()` を毎回立ち上げる代わりに shared app client + per-test mock 注入へ変更し、`13 passed in 0.77s -> 0.23s` まで短縮した。2 ファイル combined subset では `25 passed in 0.69s` を確認した。
- 2026-03-24: `[test_backtest_worker.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_backtest_worker.py)`、`[test_lab_worker.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_lab_worker.py)`、`[test_optimization_worker.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_optimization_worker.py)` の timeout 回帰テストは、heartbeat loop 本体の専用テストを残したまま run-worker 側を fake timeout に寄せた。3 ファイル combined subset は `41 passed in 0.42s` を確認した。
- 2026-03-24: `[test_routes_db.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_routes_db.py)` は market DuckDB の毎回生成を template-copy 化し、shared app client に対して per-test で `MarketDb` を差し替える形に変更した。`4 passed in 0.73s -> 0.45s` を確認した。
- 2026-03-24: stateless/fixture-injected な route テストの shared client を `with TestClient(...)` から plain `TestClient(...)` + explicit close へ揃えた。対象は `[test_error_format.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_error_format.py)`、`[test_routes_dataset.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_routes_dataset.py)`、`[test_routes_db_sync.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_routes_db_sync.py)`、`[test_routes_portfolio.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_routes_portfolio.py)`、`[test_routes_watchlist.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_routes_watchlist.py)`、`[test_routes_portfolio_factor_regression.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_routes_portfolio_factor_regression.py)`、`[test_routes_portfolio_performance.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_routes_portfolio_performance.py)`。
- 2026-03-24: `[test_openapi.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_openapi.py)` は schema 検証を `/openapi.json` HTTP 呼び出しではなく `app.openapi()` へ寄せ、Doc UI だけ shared client で検証する形に整理した。`[test_cors.py](/Users/shinjiroaso/.codex/worktrees/3e53/trading25/apps/bt/tests/unit/server/test_cors.py)` も per-test client 初期化をやめて shared client を使う形に変更した。関連 subset は `34 passed in 1.30s` を確認した。
- 2026-03-24: 再計測の結果、`tests/unit/server/test_*.py -m 'not slow'` は `480 passed in 42.50s -> 5.79s` まで短縮した。残る上位 hotspot は `test_openapi.py` の schema build、`test_routes_dataset.py` の end-to-end create job、`test_routes_portfolio_factor_regression.py` / `test_routes_portfolio_performance.py` の market fixture 構築、`test_routes_db.py` の isolated no-db path へ寄っている。

## 補足
- `pytest-xdist` 導入可否の再検討は、この issue の主目的ではない。
- まずは測定しやすい局所 hotspot を落とし、full suite の wall-clock を段階的に削る。
