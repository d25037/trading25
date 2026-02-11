---
name: bt-financial-analysis
description: bt FastAPI 側の financial-analysis 実装を扱うスキル。ROE/ranking/screening/factor regression/portfolio factor regression/fundamentals/margin/sector-stocks のロジック変更・契約更新・テスト時に使用する。
---

# bt-financial-analysis

financial-analysis の実装責務は `apps/bt` に集約する。

## Scope

- `/api/analytics/roe`
- `/api/analytics/ranking`
- `/api/analytics/screening`
- `/api/analytics/factor-regression/{symbol}`
- `/api/analytics/portfolio-factor-regression/{portfolioId}`
- `/api/analytics/fundamentals/{symbol}`
- `/api/analytics/stocks/{symbol}/margin-pressure`
- `/api/analytics/stocks/{symbol}/margin-ratio`
- `/api/analytics/sector-stocks`

## Source of Truth

- Route wiring: `apps/bt/src/server/app.py`
- Analytics routes: `apps/bt/src/server/routes/analytics_complex.py`, `analytics_jquants.py`, `chart.py`
- Core services: `apps/bt/src/server/services/ranking_service.py`, `screening_service.py`, `factor_regression_service.py`, `portfolio_factor_regression_service.py`, `roe_service.py`, `fundamentals_service.py`, `margin_analytics_service.py`
- Schemas: `apps/bt/src/server/schemas/`

## Workflow

1. 変更対象 endpoint の route → service → schema を特定する。
2. service で計算ロジックを実装/修正する（route には集約しない）。
3. response schema と OpenAPI を整合させる。
4. 統一エラーフォーマットと correlation ID 伝播を維持する。
5. 必要な pytest を実行して回帰を確認する。
6. 契約変更があれば ts 側で `bt:sync` が必要なことを明示する。

## Guardrails

- financial-analysis ロジックを `apps/ts` に戻さない。
- 既存の統一エラーレスポンス形式を崩さない。
- market filter は legacy (`prime/standard/growth`) と current (`0111/0112/0113`) を同義として扱う。
- `/doc` を正とした OpenAPI 契約を維持する。

## Verification

- 変更箇所に対応する unit/integration テストを優先実行する。
- 最低限 `apps/bt` で型・lint・テストのいずれかを回し、未実行項目は明記する。
