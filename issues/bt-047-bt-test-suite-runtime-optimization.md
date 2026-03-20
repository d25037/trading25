---
id: bt-047
title: "bt フルスイートテストの実行時間最適化"
status: open
priority: medium
labels: [bt, testing, performance, ci]
project: bt
created: 2026-03-20
updated: 2026-03-20
depends_on: []
blocks: []
parent: null
---

# bt-047 bt フルスイートテストの実行時間最適化

## 目的
- `apps/bt` のフルスイートテストがローカルで約 6 分かかる状況に対して、通常開発時の待ち時間を削減する。
- フルスイートの信頼性は維持しつつ、ローカル実行と CI 実行の役割を整理する。

## 背景
- `apps/bt/tests` は unit/server/strategy/agent/data/backtest など広い範囲を含み、テストファイル数も多い。
- `apps/bt/pyproject.toml` の pytest 設定では `slow` marker は定義されているが、現状は slow テストの明示的な運用が弱い。
- `scripts/test-packages.sh` と `scripts/test-apps.sh` はどちらも素の `pytest` 実行を前提としており、ローカル高速化オプションがない。
- `apps/bt/tests/conftest.py` には `create_app()` / `TestClient` / `httpx.AsyncClient` / DuckDB / vectorbt 系 fixture があり、一部テストは unit でも初期化コストが高い可能性がある。

## 受け入れ条件
- [ ] 遅いテスト群を特定できるように、ローカルで durations 計測または同等の可視化手段が提供される。
- [ ] `slow` marker の付与基準が決まり、通常開発時に slow を除外した実行手段が用意される。
- [ ] ローカル向けに任意の並列実行オプション（例: `pytest-xdist`）を導入するか、導入しない理由を文書化する。
- [ ] FastAPI app 初期化や DuckDB 初期化が不要な unit テストを service/domain 直呼びへ寄せる整理方針が定義される。

## 実施内容
- [ ] `apps/bt/tests` のうち遅いファイル・重い fixture 利用箇所を棚卸しする。
- [ ] `scripts/test-packages.sh` / `scripts/test-apps.sh` にローカル診断用 durations 出力や高速モードの導入可否を検討する。
- [ ] `pytest-xdist` 導入時の並列安全性（tmp_path, env, XDG path, shared state）を確認する。
- [ ] `apps/bt/tests/conftest.py` と `tests/unit/server/**` を中心に、HTTP 起動不要のテストを軽量化する候補を抽出する。
- [ ] README または docs に「通常フルスイート」「高速ローカル実行」「slow 含む完全確認」の使い分けを記載する。

## 結果
- 未着手

## 補足
- 現時点では約 6 分は異常ではないが、開発ループ短縮の観点では改善余地がある。
- CI はすでに `package-unit-tests` と `app-integration-tests` に分割されているため、まずはローカル体験の改善を優先する。
