---
id: bt-031
title: Backtest direct access Phase2 cleanup
status: open
priority: medium
labels: [backtest, architecture, maintenance]
project: bt
created: 2026-02-12
updated: 2026-02-12
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
未着手

## 補足
- Phase1 実装: backtest 実行時のみ `BT_DATA_ACCESS_MODE=direct` を有効化済み。
- この Issue は「移行完了後の恒久化/簡素化」を扱う。
