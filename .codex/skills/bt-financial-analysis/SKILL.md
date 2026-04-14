---
name: bt-financial-analysis
description: bt FastAPI 側の financial-analysis 実装を扱うスキル。ROE、ranking、screening、factor regression、fundamentals、margin analytics のロジック変更・契約更新・テスト時に使用する。
---

# bt-financial-analysis

## When to use

- ranking、screening、fundamentals、factor regression、margin analytics の計算や API 契約を変更するとき。
- `apps/bt` 側の分析ロジックを見直し、ts 側 consumer ではなく backend SoT を触るとき。

## Source of Truth

- `apps/bt/src/domains/analytics`
- `apps/bt/src/domains/fundamentals`
- `apps/bt/src/application/services`
- `apps/bt/src/entrypoints/http/routes`
- `apps/bt/src/entrypoints/http/schemas`

## Workflow

1. 変更対象 endpoint の route -> service -> domain -> schema を特定する。
2. 計算ロジックは `domains/*` と `application/services/*` に寄せ、route には I/O だけを残す。
3. ranking / fundamentals / screening の snapshot は必ず target date / as-of date で切り、開示・universe・latest row selection の順序が PIT stable か確認する。
4. screening job など async SoT を壊さないか確認する。
5. response schema、OpenAPI、ts 側の `bt:sync` 要否を同時に確認する。

## Guardrails

- financial-analysis の実装責務は `apps/bt` に集約する。
- financial-analysis ロジックを `apps/ts` に戻さない。
- future leak / point-in-time contamination を最優先で疑う。開示行や snapshot row は `apps/bt/src/shared/utils/pit_guard.py` の helper を優先して切る。
- `latest per code` や `latest per issuer` を取る前に、必ず as-of filtering を済ませる。
- ranking や fundamentals の仕様変更では、PIT stability test または future-row exclusion test を追加する。
- 既存の統一エラーレスポンス形式を崩さない。
- market filter は legacy (`prime/standard/growth`) と current (`0111/0112/0113`) を同義として扱う。

## Verification

- `uv run --project apps/bt pytest tests/unit/server/routes/test_routes_analytics_fundamentals.py tests/unit/server/services`
- `uv run --project apps/bt ruff check src/domains/analytics src/domains/fundamentals src/application/services src/entrypoints/http/routes`
- `uv run --project apps/bt pyright src/domains/analytics src/domains/fundamentals src/application/services`
