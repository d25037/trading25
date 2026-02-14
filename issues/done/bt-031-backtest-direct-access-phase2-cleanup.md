---
id: bt-031
title: Backtest direct access Phase2 cleanup
status: done
priority: medium
labels: [backtest, architecture, maintenance]
project: bt
created: 2026-02-12
updated: 2026-02-14
depends_on: []
blocks: []
parent: null
---

# bt-031 Backtest direct access Phase2 cleanup

## 目的
backtest 実行経路で導入した `http/direct` 二重モードを整理し、保守コストを下げる。

## 受け入れ条件
- backtest 実行経路で `direct` をデフォルト化し、`http` 分岐を削除または限定利用に縮小する。
- `src/data/access/clients.py` の変換処理を共通化し、loader 側で重複した整形ロジックを持たない。
- backtest 実行中に `BaseAPIClient._request` が呼ばれないことをテストで継続保証する。
- 不要化した互換コード（コメント/legacy記述）を整理する。

## 実施内容
- `src/data/access/*` の API を最小公開面に再設計する。
- loader から見たインターフェースを確定し、移行完了後に一時互換コードを削除する。
- direct 経路のエラー型とメッセージを統一し、監視ログの可読性を上げる。
- テストを `mode依存` から `振る舞い依存` に寄せる。

## 結果
- `BacktestRunner.execute()` のデフォルトを `direct` に変更し、`BT_DATA_ACCESS_MODE` を常時明示注入する形へ整理した。
- `BacktestService` は mode を個別指定せず Runner のデフォルトを利用するようにし、`http` 指定は明示オーバーライド時のみの限定利用に縮小した。
- `src/data/access/clients.py` の DB行→DataFrame 変換を `src/api/dataset/helpers.py` の共通変換関数に寄せ、変換責務を一本化した。
- `get_dataset_client()` / `get_market_client()` から `http_client_factory` 引数を削除し、loader/strategy 側の呼び出しを最小インターフェースへ統一した。
- backtest 実行中の HTTP 非利用を担保する回帰テスト（`test_backtest_runner_default_direct_mode_bypasses_http_requests`）を追加した。
- 旧来の `localhost:3001` など legacy 記述を関連 loader で整理した。
- 追加テストにより、対象モジュールの line/branch coverage を 80% 以上で担保した（`backtest.py` 96%, `clients.py` 98%, `runner.py` 95%, `backtest_service.py` 98%）。

## 補足
- Phase1 実装: backtest 実行時のみ `BT_DATA_ACCESS_MODE=direct` を有効化済み。
- この Issue は「移行完了後の恒久化/簡素化」を扱う。
