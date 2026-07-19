# PR #480 Final Review Blockers Design

## Purpose

PR #480 の最終レビューで見つかった2件の blocker を解消する。Technical Fit の forward outcome session を signal feature と同じ valid raw-bar universe に揃え、`scripts/prepush-ci.sh --research` が docs-only 差分でも公称どおり research checks を強制するようにする。既存 PR #480 の branch を更新し、重複 PR は作成しない。

## Scope

対象は次の2点に限定する。

1. `ranking_technical_fit_price_projection.py` の signal feature と forward outcome が、同一の valid raw-bar predicate で session を数える。
2. `scripts/prepush-ci.sh --research` が docs-only scope の早期終了を抑止し、必要な command/dependency preparation を含めて research suite を実行する。

Production Ranking API、OpenAPI、ts/web、strategy contract、研究の decision gate は変更しない。canonical research publication は、現行 Market v4 data に invalid raw bar が存在して結果または provenance digest が変わる場合だけ再生成する。

## Valid Raw-Session Contract

`stock_data_raw` から正規化した physical row の audit count は保持する。一方、signal feature window と forward completion horizon の session universe は、次の predicate を共有する。

```sql
open > 0
AND high > 0
AND low > 0
AND close > 0
AND volume >= 0
```

`stock_adjustment_basis_segments.cumulative_factor` は既存 integrity check により finite positive なので、raw price の正負と projected price の正負は一致する。したがって `ranking_technical_fit_raw_sessions` の `lead()` 入力をこの predicate で絞れば、signal feature と outcome の session counting が一致する。

invalid raw bar は canonical physical audit から削除しない。`canonical_raw_row_count` は従来どおり全正規化 row を数える。invalid bar は feature/outcome session を消費せず、N-session completion date は次の valid bar へ進む。completion basis、segment、return、hash は解決後の valid completion date を使う。

fail-closed basis/segment integrity、event-time completion basis、TOPIX endpoint の扱いは変更しない。

## Forced Research Pre-push Contract

`--research` は changed-file taxonomy にかかわらず local research checks を強制する。そのため `research_ci || include_research` を次の3境界で一貫して扱う。

1. command validation: docs-only + `--research` でも `uv` を必須にする。
2. dependency preparation: `--skip-install` が無い場合は bt dependencies を準備する。
3. execution/early return: docs-only early return は `include_research=false` の場合だけ許可し、指定時は `run_research_suite` まで到達する。

plain docs-only で追加 flag が無い場合の早期 PASS、`--security` / `--web-e2e` / `--full` の既存 semantics は維持する。

## Test Design

TDD で次の regression を先に追加し、現行 head で期待理由により失敗することを確認する。

1. price projection fixture に signal date、invalid intermediate bar、valid next bar を作る。horizon 1 の completion date が invalid bar ではなく valid next bar になり、return が valid endpoints から算出されることを検証する。
2. `canonical_raw_row_count` は invalid physical row を含む一方、completed outcome は valid completion date を持つことを検証する。
3. pre-push script の docs-only + `--research --skip-install` execution contract を command stubs 付きの isolated temporary repository で実行し、docs-only early return せず research guardrail/fast test path に到達することを検証する。
4. plain docs-only execution は従来どおり早期 return し、research path を実行しないことを検証する。
5. source contract として command/dependency/execution の全 research selection boundary が `include_research` を扱うことを検証する。

実行型 pre-push test が過度に repository internals と結合する場合でも、単なる文字列1箇所の assertion へ縮小せず、少なくとも docs-only + forced research の分岐結果を subprocess で証明する。

## Verification

実装後に以下を実行する。

- 新規 regression tests の RED → GREEN
- `test_ranking_technical_fit_price_projection_contract.py`
- `test_ci_workflow.py` と pre-push execution regression module
- mapped fast research contract tests
- PR #480 の mapped heavy research suite
- Ruff、Pyright、research guardrails、strict skill audit
- `git diff --check`
- current Market v4 raw data の invalid-bar inspection。該当 row があれば3 publication runnerの再実行要否を artifact/digest差分で判定する

検証後、対象ファイルだけを commit/push し、既存 PR #480 を open のまま再レビュー可能にする。既存 inline blocker が outdated または解決済みになったことと、最新 head の CI state を確認する。
