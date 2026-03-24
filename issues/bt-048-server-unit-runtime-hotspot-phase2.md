---
id: bt-048
title: "server unit test の残 hotspot を phase2 で削減"
status: open
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
- [ ] `tests/unit/server` の残 hotspot を chunk 単位で棚卸しし、上位ファイルと主因を issue に記録する。
- [ ] 少なくとも 1 つ以上の残 hotspot ファイルで、共有 fixture 化または service/domain 直呼びへの置換を実施する。
- [ ] 変更対象の before/after runtime をファイル単位または subset 単位で比較し、回帰確認を残す。
- [ ] `bt-047` で整備した `fast` / `slow` / xdist 保留方針と矛盾しない形で進める。

## 実施内容
- [ ] `tests/unit/server` を route/service/db などの chunk に分けて durations を再計測する。
- [ ] `create_app()` / `TestClient` / 一時 DB 作成を毎テストで繰り返している fixture を候補抽出する。
- [ ] 不変な app/schema/market fixture を module 共有に寄せられる箇所から順に最適化する。
- [ ] HTTP 起動が不要なテストは service/domain 直呼びへ移せるかを確認する。
- [ ] 必要なら `apps/bt/docs/test-runtime.md` に新しい hotspot 知見を追記する。

## 結果
- 未着手

## 補足
- `pytest-xdist` 導入可否の再検討は、この issue の主目的ではない。
- まずは測定しやすい局所 hotspot を落とし、full suite の wall-clock を段階的に削る。
