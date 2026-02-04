---
id: bt-023
title: PortfolioAPIClient を read-only に縮小
status: done
priority: low
labels: [refactor, dead-code, api-integration]
project: bt
created: 2026-02-02
updated: 2026-02-02
depends_on: []
blocks: []
parent: null
---

# bt-023 PortfolioAPIClient を read-only に縮小

## 目的
apps/bt/で未使用の Portfolio write系メソッドを削除し、apps/ts/ との責務境界を明確にする。

## 受け入れ条件
- 以下の未使用メソッドが削除されていること:
  - `create_portfolio()`
  - `update_portfolio()`
  - `delete_portfolio()`
  - `add_portfolio_item()`
  - `update_portfolio_item()`
  - `delete_portfolio_item()`
- read系メソッドは維持されていること:
  - `get_portfolio_list()`
  - `get_portfolio()`
  - `get_portfolio_by_name()`
  - `get_portfolio_items()`
  - `get_portfolio_codes()`
  - `get_portfolio_summary()`
- apps/bt/の既存テスト・機能に影響がないこと

## 実施内容
- ファイル: `src/api/portfolio_client.py`
- Portfolio CRUD はapps/ts/ (web/cli) の責務
- apps/bt/はポートフォリオの読み取り（銘柄コード取得→バックテスト/分析）のみ必要
- クライアント名を `PortfolioReadClient` にリネームすることも検討

## 結果
- 6つのwriteメソッド（create/update/delete_portfolio, add/update/delete_portfolio_item）を削除
- docstringからPOST/PUT/DELETEエンドポイント記述を削除
- 未使用の `Optional` importを削除
- 対応する6つのテストを削除
- クラス名は `PortfolioAPIClient` のまま維持（リネームは影響範囲が広いため）
- 全テスト通過確認済み

## 補足
- apps/ts/ の `/api/portfolio/*` エンドポイント自体は変更不要
- apps/ts/web と apps/ts/cli が write操作の唯一のコンシューマーとなる
