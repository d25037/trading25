# PR #480 Outcome and Publication Integrity Design

## Purpose

PR #480 の final whole-branch review で見つかった、event-time stock completion date と benchmark endpoint の不整合を解消する。Fixed Return Priority と Trend Acceleration が upstream の authoritative outcome relation を実際に消費するようにし、N225 sensitivity も同一 completion date へ揃える。数値または provenance が無効な publication は immutable new version として再発行し、Research catalog/index を canonical publication と同期させる。

## Root Cause

`create_event_time_price_relations()` は valid stock session で horizon completion date を決め、completion-date basis を両 stock endpoint に適用し、signal date と実 completion date の TOPIX close から `forward_close_excess_return_*` を構築する。

しかし `ranking_color_evidence._create_observation_panel()` の external-price branch は `forward_close_return_*` だけを synthetic future close に戻し、次を捨てている。

- `forward_outcome_completion_date_*`
- authoritative `forward_close_excess_return_*`

その後、signal date から TOPIX/N225 を独立 `lead(horizon)` して excess を再計算するため、stock suspension / missing session で benchmark endpoint がずれる。Fixed と Trend はこの panel outcome を primary metric として消費している。Technical Fit の primary TOPIX outcome は direct relation join により正しいが、N225 sensitivity は同じ nominal-lead 問題を持つ。

## Authoritative Outcome Contract

`price_feature_relation` / `price_outcome_relation` を指定した Daily Ranking panel は、各 horizon について次を authoritative relation から透過する。

- `forward_outcome_completion_date_{horizon}d`
- `forward_close_return_{horizon}d_pct`
- `forward_close_excess_return_{horizon}d_pct`

external-price branch では TOPIX nominal lead から primary excess を再計算しない。stock return と TOPIX excess の naming/schema は既存 consumer contract を維持する。

legacy `stock_data` branch は既存 behavior を維持する。external/legacy の分岐は SQL expression を明示的に切り替え、同名 column の衝突や暗黙上書きを避ける。

## Benchmark Endpoint Alignment

N225 sensitivity は、各 horizon の authoritative stock completion date を使って signal/completion endpoint の N225 close を join する。nominal `lead(horizon)` は external-price branch の N225 excess に使わない。

対象は次の3研究である。

- Fixed Return Priority: observation / sensitivity tables
- Trend Acceleration: observation / sensitivity tables
- Technical Fit: N225 candidate sensitivity

TOPIX primary outcome は upstream relation の値をそのまま使う。N225 aligned return は panel または consumer の一箇所で構築し、consumer ごとに別の endpoint rule を実装しない。

## Consumer and Bundle Contract

Fixed と Trend の最終 observation rows / `observation_sample` は各 horizon の completion date、stock return、TOPIX excess を保持する。これにより bundle から outcome endpoint を監査できる。

aggregate table names と decision gate schema は維持する。修正後の aggregate、bootstrap、gate は再計算する。OpenAPI、production Ranking API、ts/web、strategy contract は変更しない。

upstream price projection hash は保持するが、consumer-level regression と persisted completion date により「正しい relation が実際に消費された」ことを検証する。新しい manifest field は、既存 bundle writer が output table digests を既に記録していない場合に限り追加し、独自の重複 provenance format は導入しない。

## Current Data Impact and Publication Versions

current Market v4 の Prime exact-date universe では invalid raw bar は0件だが、stock-specific missing sessions により nominal TOPIX completion と stock completion が異なる。

- 5D: 1,044 / 4,502,049 complete keys
- 20D: 2,509 / 4,473,964 complete keys
- 60D: 4,431 / 4,399,245 complete keys

Trend v5 sample では code `7916` の4 signal datesで60D endpoint mismatch が実在し、published primary excess に最大約1.10 percentage point の差がある。Trend は新しい immutable v6を必須とする。

Fixed v9 の全4,762 observationsでは completion mismatch が偶然0件だったが、manifest が宣言した authoritative outcome relation を consumer が使っていない provenance gap がある。Fixed は contract-correctな immutable v10を発行し、v9をsupersedeする。

Technical Fit v8 primary TOPIX は正しい。N225 alignment 修正後に全 observation を監査し、published table/digestが変わる場合だけ immutable v9を発行する。差が無ければ v8をcanonicalのまま維持し、N225 audit evidenceだけをtest/readoutへ追記する。

既存 bundle は上書き・削除しない。

## Canonical Catalog Registry

`research-catalog-metadata.toml` の3 experiment entry に次の structured fields を追加する。

- `canonicalRunId`
- `canonicalDecision`
- `supersededRunIds`

`decision` prose も final canonical version/run lineage と一致させる。Technical Fit は既存 committed digest の `published_run_id` / `decision` と structured fields を一致させる。Trend/Fixed には hermetic committed publication registry fixture を追加するか、3研究共通の小さな registry fixture に統合する。

catalog/index consistency test は README prose のversion文字列だけをscrapeせず、structured TOML と committed registry/digest を比較する。live XDG manifest は通常CIの必須依存にしない。

`apps/bt/docs/experiments/README.md` と各 canonical README の Source Artifacts / supersession lineage を final version に更新する。

## Test Design

TDDで以下を先にREDにする。

1. price outcome fixtureで nominal TOPIX completion と stock completion のTOPIX closeを異なる値にし、upstream completion/stock return/TOPIX excessをassertする。
2. Daily Ranking external-price panelで authoritative completion/return/excessがrelationと一致し、nominal TOPIX lead resultと不一致になる sparse-session regressionを追加する。
3. Fixed/Trendの最終 `observation_sample_df` とbundle round-tripでcompletion date/return/excessが保持されることを検証する。
4. Fixed/Trend/TechnicalのN225 sensitivityがauthoritative completion date endpointを使う regressionを追加する。
5. structured catalog registryがcanonical run ID、decision、superseded IDsで一致する hermetic testを追加する。

既存 fast-CI/heavy-local boundary は維持する。consumer boundary の小さい contract tests は fast対象へ追加可能だが、full experiment rerunはlocal publication workflowに残す。

## Verification and Publication

実装後は affected unit suites、Ruff、Pyright、research guardrails、privacy/skill audit、mapped fast/heavy suitesを実行する。current Market v4で consumer endpoint mismatch/delta audit を再実行し、Trend v6、Fixed v10、必要時のみTechnical v9を既存runnerから発行する。

各新bundleについて manifest、results.duckdb、summary.md、canonical README、decision/gate、output counts/hashを検証する。publication docs/catalog/digestをcommitし、full `scripts/prepush-ci.sh --research --skip-install` を完走させる。

既存 PR #480 branch を更新し、新しいPR番号は作らない。final reviewerのCritical/Important findingsを根拠付きでreply/resolveし、最新GitHub CI成功後にReady-to-mergeと判定する。

## Publication Review Remediation

Task 2 の独立 bundle review で、Trend v6 は shared panel で completion-aligned N225 を計算する一方、最終 observation / bundle が N225 excess を保存していないことが判明した。Trend consumer は各 horizon の `forward_close_n225_excess_return_*` を最終 observation と bundle に透過し、sparse-session regression と live bundle auditで検証する。v6 は変更せず immutable archive とし、修正版は Trend v7 として新規発行する。

Technical v9 は N225 endpoint 数値を修正したが、artifact 内の `invalidation_disposition` が v7→v8 で止まり、v8→v9 supersession reason を保持しなかった。disposition と固定テストを v8→v9 reason まで更新し、v9 は変更せず immutable archive として Technical v10 を新規発行する。最終 catalog / registry は Trend v7、Fixed v10、Technical v10 を canonical とする。
