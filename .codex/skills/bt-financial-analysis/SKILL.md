---
name: bt-financial-analysis
description: Use when bt FastAPI の ROE、ranking、screening、factor regression、fundamentals、または margin analytics を変更・テストするとき。
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
   Fundamentals GET/POSTでは `to` をknowledge cutoff、`from`をdisplay lower boundとして扱い、同じMarket v4 PIT bundle readerとstrict date/error semanticsを使う。
4. analytics read は schema v4 / `local_projection_v2_event_time` の既存 materialization だけを読み、欠損 basis を GET/POST 内で生成しないことを確認する。
5. screening job など async SoT を壊さないか確認する。旧 `GET /api/analytics/screening` は 410 で、jobs/result API だけを実行 SoT とする。
6. response schema、OpenAPI、ts 側の `bt:sync` 要否を同時に確認する。

## Guardrails

- financial-analysis の実装責務は `apps/bt` に集約する。
- financial-analysis ロジックを `apps/ts` に戻さない。
- future leak / point-in-time contamination を最優先で疑う。開示行や snapshot row は `apps/bt/src/shared/utils/pit_guard.py` の helper を優先して切る。
- `latest per code` や `latest per issuer` を取る前に、必ず as-of filtering を済ませる。
- cutoff-aware Fundamentals / Prime liquidityは`stock_data`、`stocks_latest`、current/latest basis fallback、service-local adjustmentを使わない。`stock_adjustment_bases` / segmentsから選んだready basisと同basisの`statement_metrics_adjusted` / `daily_valuation`だけを使う。
- schema v3以前や adjustment mode 不一致を analytics-local fallback で隠さない。Market v4/event-time incompatibilityまたは materialization欠損は typed recovery error として返す。
- missing/inconsistent basisまたはexact `stock_master_daily` snapshotの欠損をunsupported/emptyへdowngradeしない。409 recoveryは`adjusted_metrics_pit` stageを案内し、`repair`を案内しない。
- ranking や fundamentals の仕様変更では、PIT stability test または future-row exclusion test を追加する。
- 既存の統一エラーレスポンス形式を崩さない。
- market filter は legacy (`prime/standard/growth`) と current (`0111/0112/0113`) を同義として扱う。

## Verification

```bash
uv run --directory apps/bt pytest tests/unit/server/test_routes_analytics_fundamentals.py tests/unit/server/services
uv run --directory apps/bt ruff check src/domains/analytics src/domains/fundamentals src/application/services src/entrypoints/http/routes
uv run --directory apps/bt pyright src/domains/analytics src/domains/fundamentals src/application/services
```
