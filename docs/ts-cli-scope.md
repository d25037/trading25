# ts CLI Scope

`apps/ts/packages/cli` は FastAPI (`:3002`) の操作クライアントとして、  
Web UI では置き換えにくい運用・自動化ユースケースに限定する。

## Current Scope

### Keep (Operations / Automation)
- `db`: `sync`, `validate`, `stats`, `refresh`
- `dataset`: `create`, `info`, `sample`, `search`
- `jquants`: `auth status`, `fetch *`
- `backtest`: run/status/results/cancel/attribution
- `analysis`: screening/ranking/factor-regression/portfolio-factor-regression/roe

### Removed (Migrated to Web)
- `portfolio` command group
- `watchlist` command group

`portfolio/watchlist` の CRUD は `apps/ts/packages/web` の Portfolio タブを正規導線とする。

## Guardrails

- 新規 CLI 機能は「ヘッドレス実行」「JSON/CSV 出力」「運用スクリプト統合」のいずれかを満たす場合のみ追加する。
- 対話的な日常操作（CRUD/画面で十分な機能）は Web へ実装し、CLI には追加しない。
- API 契約は OpenAPI を SoT とし、CLI 側の独自型で乖離させない。
