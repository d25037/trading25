# PR #480 Review Fixes Design

## Purpose

PR #480 の研究結果から future-outcome availability による選定時の先読みを除去し、Market v4 event-time basis 検証を fail-closed にする。同時に Technical Fit の実質的に flat な expectancy を 0..1 score 全域へ増幅しないようにする。CI は軽量 contract tests に限定し、重い experiment tests と publication rerun は local workflow に残す。

## Scope

対象は次の4点に限定する。

1. Fixed Return Priority、Trend Acceleration、Technical Fit の top-k 選定を signal-time information だけで確定する。
2. signal/completion price projection basis に `adjustment_through_date = valid_from` を要求する。
3. Technical Fit の expectancy spread が 0.01 percentage point（1 bp）以下なら flat と扱う。
4. research test/fixture taxonomy と local mapped-test routing を補完し、必須CIには専用の軽量 contract tests だけを追加する。

Production Ranking API、materialization、sort key、badge、UI field は変更しない。

## Selection and Outcome Semantics

各 top-k builder は次の順序を共通 contract とする。

1. signal date 時点で利用可能な priority/fit score を持つ候補だけを signal-time candidate universe とする。
2. code を deterministic tie-breaker として候補を順位付けする。
3. top-k membership を確定する。この時点では forward outcome の有無を参照しない。
4. membership 確定後に candidate universe と selected basket の outcome coverage を評価する。
5. candidate universe または selected basket に outcome 欠損が1件でもあれば、rank 6/11 などによる補充を行わない。
6. 不完全な行も audit row として保持するが、return、lift、win-rate、quantile、severe-loss などの outcome-derived metrics は `NaN` にする。
7. stability、bootstrap、adoption/decision gate は complete rows のみを使用する。

各 top-k table に以下を加える。

- `candidate_outcome_count`
- `candidate_outcome_coverage_pct`
- `selected_outcome_count`
- `selected_outcome_coverage_pct`
- `outcome_status`: `complete` または `incomplete_outcomes`

既存の `candidate_count` は outcome filter 前の signal-time candidate count とする。Technical Fit の `topk_operational_lift` bundle schema も同じ定義へ更新する。

この設計では欠損 outcome を severe loss として補完せず、観測済み銘柄だけで basket size を縮小して評価することもしない。どちらも別の estimand を導入するためである。

## Market v4 Basis Integrity

`ranking_technical_fit_price_projection.py` の signal basis と completion basis の ready/materialized selection に、次の条件を追加する。

```sql
CAST(basis.adjustment_through_date AS DATE)
    = CAST(basis.valid_from AS DATE)
```

interval cardinality check は basis interval の一意性確認として維持し、上記は integrity/readiness filter に置く。条件不一致時は既存の request-count comparison が失敗し、signal と completion それぞれの fail-closed error を返す。latest/current basis fallback や service-local recomputation は追加しない。

## Technical Fit Flatness

`DEFAULT_FLAT_EXPECTANCY_TOLERANCE_PCT = 0.01` を frozen research parameter とする。`classify_shape()` は finite expectancy の `max - min` がこの値以下なら `flat` を返し、walk-forward mapping は全binを `technical_fit_score = 0.5` とする。

閾値は percentage point 単位であり、0.01 percentage point は 1 basis point に相当する。負の override は `ValueError` とする。閾値を超える明確な monotonic/shape response は従来どおり min-max mapping を使う。

このparameterと変更後の選定contractは研究結果へ影響しうるため、3つのrunnerをlocalで再実行し、canonical README、digest、bundle provenanceを更新する。過去のbundleはimmutable artifactとして扱い、上書きせず新しいversionを発行する。

## CI and Local Verification Boundary

「重いものはlocal」を維持する。

必須CIは以下だけを実行する。

- 既存の fast research guardrail/core tests
- 新しい軽量 price-projection contract module
- selection-first、missing-outcome fail-closed、flat tolerance の小さなpure-dataframe contract tests
- taxonomy/target-routing unit tests

フルの Fixed Return Priority、Trend Acceleration、Technical Fit experiment suites、live XDG bundle verification、runner publication rerunは必須CIへ追加しない。

`test_taxonomy.py` は以下を research scope と認識する。

- `apps/bt/tests/fixtures/research/**`
- analytics research module に対応する `apps/bt/tests/unit/domains/analytics/test_*.py`

`research-test-targets.py` は changed research test をそれ自身へ、published digest fixture をそのconsumer testへmapする。このmapped suiteは `scripts/prepush-ci.sh` のlocal research pathで実行する。GitHub Actions の `bt-research-tests` は引き続き `--mode fast-pytest` のみを使う。

## Tests

TDDで次のregressionを先に追加し、各testが既存実装に対して期待理由で失敗することを確認する。

1. 各top-k builderで最高score候補のoutcomeを欠損させてもmembershipを補充しない。
2. `candidate_count` はsignal-time universe sizeを保持し、coverage/count/statusを正しく記録する。
3. incomplete rowのoutcome-derived metricsが`NaN`で、downstream gate/bootstrapから除外される。
4. signal basisの`adjustment_through_date != valid_from`を拒否する。
5. completion basisの`adjustment_through_date != valid_from`を拒否する。
6. expectancy spreadが1bp以下ならflat、1bp超なら従来mappingとなる。
7. research test/fixture pathがresearch scopeへ分類され、local mapped targetへ解決される。
8. GitHub Actions workflowがheavy mapped suitesを呼ばず、fast curated targetsだけを呼ぶ。

## Verification and Publication

実装後は次を実行する。

- focused RED/GREEN tests
- affected 3 experiment test modulesとprice projection contract tests
- research taxonomy/target tests
- ruff、pyright、research guardrails、strict skill audit
- 3 runnerの`--help`
- 3 research runnersのlocal rerunとcanonical artifact/readout検証
- local `scripts/prepush-ci.sh --research --skip-install`

PR #480 のbranchへcommit/pushし、DraftをReady for reviewへ変更する。既存PRを閉じたり新規PR番号を作成したりしない。
