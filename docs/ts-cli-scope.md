# ts CLI Scope

`apps/ts/packages/cli` は廃止済みです。  
TypeScript workspace は `web / contracts / domain / utils / api-clients` を管理します。

## Current Policy

- 運用・デバッグの CLI は `apps/bt` の `bt` コマンドを使用する
- 日常操作（Portfolio/Watchlist、Database Sync、Analysis、Backtest）は `apps/ts/packages/web` を正規導線とする
- API 契約は FastAPI OpenAPI を SoT とし、`@trading25/contracts` / `@trading25/api-clients` を通じて利用する
