---
id: bt-024
title: cli_market/ のranking/screeningをapps/ts/cliに一本化
status: done
priority: medium
labels: [refactor, cli, api-integration]
project: bt
created: 2026-02-02
updated: 2026-02-02
depends_on: []
blocks: []
parent: null
---

# bt-024 cli_market/ のranking/screeningをapps/ts/cliに一本化

## 目的
apps/bt/とapps/ts/で重複するマーケット分析CLIコマンドを整理し、apps/bt/をバックテスト専用エンジンとしての責務に集中させる。

## 受け入れ条件
- apps/bt/ の `cli_market/ranking.py` と `cli_market/screening.py` が削除されていること
- apps/ts/cli の `analysis ranking` / `analysis screening` で同等機能が利用可能であること（既に存在）
- apps/bt/ 固有の `signal_screening.py`（シグナルベーススクリーニング）は維持されていること
- MarketAPIClient の ranking/screening メソッドの削除も検討されていること

## 実施内容
- 削除対象:
  - `src/cli_market/ranking.py` — apps/ts/cli `analysis ranking` と重複
  - `src/cli_market/screening.py` — apps/ts/cli `analysis screening` と重複
  - `src/data/market_analysis.py` の ranking 関連関数（他で使われていなければ）
- MarketAPIClient から以下メソッドの削除を検討:
  - `get_trading_value_ranking()`
  - `get_daily_change_ranking()`
  - `get_all_rankings()`
- apps/bt/内で市場ランキング/スクリーニングを使う他のコードがないか確認が必要

## 結果
- `src/cli_market/` ディレクトリ全体を削除
- `tests/unit/cli_market/` ディレクトリ全体を削除
- pyproject.toml から `market` エントリポイントと coverage omit を削除
- MarketAPIClient からranking系メソッド3つ（get_trading_value_ranking, get_daily_change_ranking, get_all_rankings）を削除
- `market_analysis.py` からranking関数3つ + screening wrapper関数2つ（get_stock_data_for_screening, perform_screening）を削除、re-exportモジュールに簡素化
- `src/api/models.py` から `RankingItem` と `ScreeningResult` モデルを削除
- テスト更新: ranking系テスト・ErrorHandlingテスト・wrapper系テストを削除、signal_screening re-export経由のテストは維持
- ドキュメント更新: CLAUDE.md, AGENTS.md, README.md, docs/commands.md からmarket CLI記述を削除
- apps/bt/が引き続き使用するmarket APIエンドポイント: `get_stock_ohlcv`, `get_stock_data_for_screening`, `get_topix`
- 全1774テスト通過、ruff/pyright通過確認済み

## 補足
- apps/ts/cli が JQUANTS→market.db→分析 の一気通貫パイプラインを持つ
- apps/bt/ がこれを別経路で呼ぶのはアーキテクチャ上の責務違反
- ただし、apps/bt/のシグナルスクリーニング（独自ロジック）は残すべき
- `cli_market/` ディレクトリ自体が空になる場合は削除
