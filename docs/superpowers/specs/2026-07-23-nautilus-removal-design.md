# Nautilus Removal and VectorBT-Only Execution Design

## Goal

trading25 の backtest family を VectorBT の単一実行経路へ戻し、利用予定のない Nautilus verification に伴う adapter、二段ジョブ制御、API/UI 分岐、optional dependency、専用 CI を完全撤去する。個人開発・個人運用に不要な将来拡張用の複雑性を削減し、Optimization と Lab の通常実行に残っている verification candidate 構築コストも除去する。

保存済み Nautilus job は存在しないため、`nautilus` enum 値、legacy parser、過去 child job の非表示処理、migration は残さない。

## Chosen architecture

Backtest、Optimization、Lab はすべて VectorBT fast path だけを実行する。

- `POST /api/backtest/run` は engine を選択させず、常に VectorBT backtest を submit する。
- Optimization と candidate-producing Lab (`generate` / `evolve` / `optimize`) は単一 stage で完了する。
- Optimization と Lab の `fast_candidates` は、上位候補の可視化に有用な VectorBT-native result として維持する。
- Lab の `save=true` は fast path の最良候補を保存する。verification mismatch による保存スキップや authoritative candidate への差し替えは行わない。
- `RunSpec`、`RunMetadata`、`CanonicalExecutionResult`、`ArtifactIndex` は実行 provenance と成果物解決に有用なので維持する。
- 内部 provenance の `engine_family` は `vectorbt` と未知データ用の `unknown` を保持できるが、`nautilus` は完全に削除する。

canonical result や artifact contract 自体を VectorBT 専用形式へ再設計することは今回の範囲外とする。Nautilus 固有実装の削除と、汎用成果物契約の全面再設計を混ぜない。

## Removed backend runtime

次を削除する。

- `domains/backtest/nautilus_adapter.py`
- `domains/backtest/nautilus_metrics.py`
- `application/services/verification_orchestrator.py`
- `backtest_worker` の Nautilus runner injection、engine dispatch branch、missing dependency handling
- Optimization/Lab worker の verification stage、verification child cancellation、50% progress split
- Optimization/Lab service の verification seed、candidate config override、internal `_verification_*` metadata 生成
- `EngineFamily.NAUTILUS`
- `EnginePolicy`、`EnginePolicyMode`
- verification candidate/overall status、delta、summary model
- Nautilus execution policy version
- internal verification child job を list/get から隠す route 処理

Optimization の全 grid combination に対する `build_config_override()` と、Lab Optimize の全 trial に対する verification-only `StrategyCandidate` 再構築を廃止する。fast top-10 表示に必要な candidate id、score、metrics の構築だけを残す。

worker の heartbeat、timeout、cancel、lease、terminal state、通常の result persistence は維持する。削除対象は verification child に固有の orchestration に限定する。

## Public API contract

次の request field を OpenAPI から削除する。

- `BacktestRequest.engine_family`
- `OptimizationRequest.engine_policy`
- `LabGenerateRequest.engine_policy`
- `LabEvolveRequest.engine_policy`
- `LabOptimizeRequest.engine_policy`

次の response field と supporting schema を OpenAPI から削除する。

- Optimization/Lab result の `verification`
- `EnginePolicy` / `EnginePolicyMode`
- `VerificationSummary` と candidate/delta/status schemas
- verification child run id

`fast_candidates` と `CanonicalExecutionMetrics` は残す。`CanonicalExecutionMetrics` は verification 以外の canonical execution result でも使われるため削除しない。

対象 request model は `extra="forbid"` を明示する。削除済みの `engine_family`、`engine_policy`、`verification_top_k` を送る旧 client は 422 で拒否し、黙って VectorBT 実行へフォールバックしない。Lab request 間でも同じ extra-field policy を適用する。

OpenAPI の SoT は FastAPI schema とし、backend 変更後に `@trading25/contracts` の `bt:sync` を実行して `bt-openapi.json` と generated TypeScript types を更新する。generated files は手編集しない。

## Web and shared TypeScript

次を削除する。

- Engine Policy (`fast_only` / `fast_then_verify`) selector
- verification Top K input と各 form の local state
- Optimization/Lab request の policy builder
- Verification Summary UI、verified metrics、delta、mismatch、authoritative winner 表示
- progress message の `Nautilus verification` 文字列判定
- Lab history の Verification column
- shared API client の verification-only aliases と Zod schemas

Backtest request は strategy name と optional config override だけを送る。Optimization/Lab form は policy state を持たず、既存の fast execution parameters だけを送る。

`VerificationSummarySection` 内の Fast Ranking 表示は切り分ける。既存 result surface で上位候補表示が必要な箇所には、verification 非依存の小さな Fast Candidates 表示として残す。重複表示で価値がない箇所は既存 best/worst/history 表示を優先し、無理に新コンポーネントを増やさない。

## Dependency, CI, and documentation cleanup

次を active repository surface から削除する。

- `pyproject.toml` の `nautilus` dependency group と `nautilus-trader`
- `uv.lock` の Nautilus package/dependency resolution
- `.github/workflows/nautilus-smoke.yml`
- `scripts/test-nautilus-smoke.sh`
- Nautilus runtime smoke test
- Dependabot の `nautilus-trader` pattern
- active `AGENTS.md`、README、bt docs、repository-local skills の Nautilus / two-stage verification guidance

完了済み issue、archive 文書、過去の design/plan は歴史記録なので原則変更しない。active guidance から archive の Nautilus 方針を参照しない状態にする。

## Data flow and job lifecycle

Backtest request は route から service、worker、VectorBT runner へ一方向に流れ、engine selection branch を持たない。worker は VectorBT result を job manager へ保存し、通常の canonical result と artifact index を構築して terminal state にする。

Optimization と Lab は fast computation 完了後、その result を一度永続化して parent job を completed にする。verification seed の埋め込み、child backtest job の作成、child completion の再集約、parent result の二度目の保存は行わない。

Lab の保存処理は fast execution service の既存 save path を唯一の経路とする。`save=false` は従来どおり成果物を保存せず、`save=true` は各 Lab type の fast winner を保存する。

## Error handling

- 削除済み request field は 422 validation error とする。
- Backtest、Optimization、Lab の通常の validation、timeout、cancel、heartbeat、worker failure は既存挙動を維持する。
- Nautilus dependency missing、unsupported Nautilus execution semantics、verification mismatch、verification metrics missing などのエラー分類は実装ごと削除する。
- unknown engine を VectorBT に黙ってフォールバックする公開経路は作らない。公開 request に engine selection 自体を持たせない。
- 保存済み Nautilus job は存在しない前提なので、legacy job hydration や migration を導入しない。

## Verification strategy

実装は TDD で進め、production code の変更前に対象の契約・挙動テストを失敗させる。

- Backtest request が engine field なしで成功し、`engine_family` を送ると 422 になる。
- Optimization/Lab request が policy field なしで成功し、`engine_policy` を送ると 422 になる。
- Optimization worker は fast result を一度保存して completed になり、verification child job を作らない。
- Lab generate/evolve/optimize は fast winner を保存し、verification metadata を生成しない。
- Optimization/Lab response は `fast_candidates` を維持し、`verification` を公開しない。
- Web form は Engine Policy / Top K を表示せず、新しい request contract を送る。
- Web progress/history/result は verification stage や Verification column/card を表示しない。
- OpenAPI と generated TS types に Nautilus、engine policy、verification schema が存在しない。
- optional dependency、専用 workflow/script/test、active docs/guidance に Nautilus 参照が残らない。

最終検証は変更対象の backend pytest、ruff、pyright、OpenAPI contract sync/check、shared API client tests、web tests、TypeScript typecheck、Biome lint、repository-wide Nautilus reference scan を実行する。archive/完了 issue の歴史的参照は scan の許容対象として明示的に分離する。

## Scope boundaries

VectorBT execution semantics、PIT data access、strategy signal evaluation、optimization scoring、Lab candidate generation、canonical result format、artifact storage、portfolio DB schema は変更しない。Nautilus以外の将来 engine を新設する抽象や placeholder も追加しない。
