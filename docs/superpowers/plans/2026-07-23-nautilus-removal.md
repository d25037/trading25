# Nautilus Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Nautilus固有のruntime、二段verification、公開API/UI、依存・CIを完全撤去し、Backtest/Optimization/LabをVectorBT単一実行経路へ簡素化する。

**Architecture:** 公開requestからengine selectionとengine policyを削除し、workerはVectorBT fast pathだけを実行する。`RunSpec`、canonical result、artifact index、`fast_candidates`はprovenance・結果表示として維持し、Nautilus固有のorchestrator、child job、verification schemaだけを削除する。

**Tech Stack:** Python 3.12、FastAPI、Pydantic v2、pytest、VectorBT、React 19、TypeScript、Zod、Vitest、OpenAPI generated contracts、uv、bun

## Global Constraints

- 保存済みNautilus jobは存在しないため、legacy parser、migration、read-only compatibilityを追加しない。
- `EngineFamily`型はinternal provenance用に維持し、値は`vectorbt`と`unknown`だけにする。
- `fast_candidates`と`CanonicalExecutionMetrics`は維持する。
- Backtest/Optimization/Lab requestは削除済みfieldを`extra="forbid"`で422にする。
- generated OpenAPI/TypeScript filesと`uv.lock`は手編集しない。
- `docs/archive`、`issues`、過去の`docs/superpowers`は歴史資料として変更しない。
- genericな`verification`は市場データ・分析の検証にも使うため、repository-wide禁止語にしない。
- implementationはテストを先に変更・追加し、期待したREDを確認してからproduction codeを変更する。

---

## File map

- `apps/bt/src/entrypoints/http/{schemas,routes}`: 公開request/response契約とroute wiring。
- `apps/bt/src/application/services/{backtest_service,optimization_service,lab_service,run_contracts}.py`: submit契約、worker command、neutral execution helpers。
- `apps/bt/src/application/workers/{backtest_worker,optimization_worker,lab_worker}.py`: VectorBT-only worker lifecycle。
- `apps/bt/src/domains/backtest/contracts.py`: engine-neutral provenance、canonical metrics、fast candidate contract。
- `apps/ts/packages/contracts`: FastAPI OpenAPI snapshotとgenerated types。
- `apps/ts/packages/api-clients/src/backtest`: generated contract aliasesとLab runtime schemas。
- `apps/ts/packages/web/src/components`: form、progress、history、fast candidate表示。
- `apps/bt/pyproject.toml` / `apps/bt/uv.lock` / `.github`: optional dependencyとCI。
- active `AGENTS.md` / README / bt docs / `.codex/skills`: VectorBT-only運用ガイダンス。

---

### Task 1: Remove engine selection from public backend APIs

**Files:**
- Modify: `apps/bt/tests/server/test_schemas.py`
- Modify: `apps/bt/tests/unit/server/routes/test_backtest.py`
- Modify: `apps/bt/tests/unit/server/routes/test_optimize.py`
- Modify: `apps/bt/tests/server/routes/test_lab.py`
- Modify: `apps/bt/tests/unit/server/services/test_backtest_service.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/backtest.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/optimize.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/lab.py`
- Modify: `apps/bt/src/application/contracts/lab.py`
- Modify: `apps/bt/src/entrypoints/http/routes/backtest.py`
- Modify: `apps/bt/src/entrypoints/http/routes/optimize.py`
- Modify: `apps/bt/src/entrypoints/http/routes/lab.py`
- Modify: `apps/bt/src/application/services/backtest_service.py`

**Interfaces:**
- Consumes: existing `BacktestRequest`, `OptimizationRequest`, Lab request models and service submit methods.
- Produces: public requests without `engine_family`/`engine_policy`; responses without `verification`; `fast_candidates` unchanged.

- [ ] **Step 1: Write failing schema tests for removed fields**

Replace the engine-policy assertions in `tests/server/test_schemas.py` with:

```python
from pydantic import ValidationError

from src.domains.backtest.contracts import EngineFamily


class TestBacktestRequest:
    def test_basic(self) -> None:
        req = BacktestRequest(strategy_name="test")
        assert req.strategy_name == "test"
        assert req.strategy_config_override is None

    def test_rejects_removed_engine_family(self) -> None:
        with pytest.raises(ValidationError):
            BacktestRequest.model_validate(
                {"strategy_name": "test", "engine_family": "vectorbt"}
            )


class TestOptimizationRequest:
    def test_basic(self) -> None:
        assert OptimizationRequest(strategy_name="test").strategy_name == "test"

    def test_rejects_removed_execution_options(self) -> None:
        with pytest.raises(ValidationError):
            OptimizationRequest.model_validate(
                {
                    "strategy_name": "test",
                    "engine_" + "policy": {"mode": "fast_only"},
                }
            )


def test_engine_family_contains_only_provenance_values() -> None:
    assert {item.value for item in EngineFamily} >= {"vectorbt", "unknown"}
```

Add route tests which POST a normal request without the removed fields and which expect 422 when `engine_family` or the constructed key `"engine_" + "policy"` is supplied. Parameterize Lab generate/evolve/optimize old-policy requests. Keep the removed token split in active test source so the final active-surface scan remains meaningful.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
uv run --directory apps/bt pytest \
  tests/server/test_schemas.py \
  tests/server/routes/test_lab.py \
  tests/unit/server/routes/test_backtest.py \
  tests/unit/server/routes/test_optimize.py -q
```

Expected: FAIL because Backtest still requires `engine_family`, Optimization/Lab still accept `engine_policy`, and response models still expose `verification`.

- [ ] **Step 3: Remove the public fields and route wiring**

Use `ConfigDict(extra="forbid")` on `BacktestRequest`, `OptimizationRequest`, `LabGenerateRequest`, `LabEvolveRequest`, and `LabOptimizeRequest`. Their relevant shapes become:

```python
class BacktestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy_name: str = Field(..., min_length=1)
    strategy_config_override: dict[str, Any] | None = None


class OptimizationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy_name: str = Field(..., min_length=1)
```

Remove `verification` from `OptimizationJobResponse` and the three candidate-producing Lab result models. Preserve:

```python
fast_candidates: list[FastCandidateSummary] = Field(default_factory=list)
```

Change routes to call services without engine arguments:

```python
job_id = await backtest_service.submit_backtest(
    strategy_name=request.strategy_name,
    config_override=request.strategy_config_override,
)

job_id = await optimization_service.submit_optimization(
    strategy_name=request.strategy_name,
)
```

Remove Lab submit kwargs named `engine_policy` and remove route-level `resolve_verification_summary` calls. Remove `engine_family` from `BacktestService.submit_backtest`; allow `build_strategy_run_spec` to infer VectorBT.

- [ ] **Step 4: Update affected mocks and verify GREEN**

Delete the explicit-Nautilus service test. Update submit mock expectations so they contain no engine policy and assert ordinary `run_spec.engine_family == EngineFamily.VECTORBT`.

Run the command from Step 2 plus:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_backtest_service.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 1**

```bash
git add apps/bt/src/entrypoints/http apps/bt/src/application/contracts/lab.py \
  apps/bt/src/application/services/backtest_service.py apps/bt/tests
git commit -m "refactor(bt): remove engine selection from public APIs"
```

---

### Task 2: Simplify Optimization to one VectorBT stage

**Files:**
- Modify: `apps/bt/tests/unit/server/test_optimization_worker.py`
- Modify: `apps/bt/tests/unit/server/services/test_optimization_service.py`
- Modify: `apps/bt/tests/unit/server/test_run_contracts.py`
- Modify: `apps/bt/src/application/services/run_contracts.py`
- Modify: `apps/bt/src/application/services/optimization_service.py`
- Modify: `apps/bt/src/application/workers/optimization_worker.py`

**Interfaces:**
- Consumes: `CanonicalExecutionMetrics`, `ParameterOptimizationEngine.optimize()` results.
- Produces: `build_canonical_metrics_from_payload(payload)`, a single-stage optimization result with top-10 `fast_candidates`.

- [ ] **Step 1: Write a failing no-rebuild Optimization test**

In `test_execute_optimization_sync_extracts_payload`, make the fake engine fail if verification-only config rebuilding occurs:

```python
def _fail_build_config_override(_params: dict[str, object]) -> dict[str, object]:
    raise AssertionError("must not rebuild candidate configs")

fake_engine.build_config_override = _fail_build_config_override

payload = worker_mod._execute_optimization_sync("strategy")

assert len(payload["fast_candidates"]) <= 10
assert not any(key.startswith("_verification") for key in payload)
```

Update the worker-command test to expect only `--job-id`, `--strategy-name`, and `--timeout-seconds`, with no `--engine-policy-json`.

- [ ] **Step 2: Run focused Optimization tests and verify RED**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/test_optimization_worker.py \
  tests/unit/server/services/test_optimization_service.py -q
```

Expected: FAIL when `_execute_optimization_sync` calls `build_config_override`, and when the service command still includes engine policy JSON.

- [ ] **Step 3: Move canonical metric conversion to a neutral helper**

Add this helper to `application/services/run_contracts.py` before deleting the orchestrator:

```python
def build_canonical_metrics_from_payload(
    payload: dict[str, Any] | None,
) -> CanonicalExecutionMetrics | None:
    if not isinstance(payload, dict):
        return None
    values = {
        "total_return": payload.get("total_return"),
        "sharpe_ratio": payload.get("sharpe_ratio"),
        "sortino_ratio": payload.get("sortino_ratio"),
        "calmar_ratio": payload.get("calmar_ratio"),
        "max_drawdown": payload.get("max_drawdown"),
        "win_rate": payload.get("win_rate"),
        "trade_count": payload.get("trade_count"),
    }
    if not any(value is not None for value in values.values()):
        return None
    return CanonicalExecutionMetrics.model_validate(values)
```

Add direct tests for `None`, an empty mapping, and a populated metric mapping to `test_run_contracts.py`.

- [ ] **Step 4: Remove Optimization policy and verification orchestration**

In `_execute_optimization_sync`, build only the top-10 display list:

```python
fast_candidates = []
for rank, result in enumerate(opt_result.all_results, start=1):
    if rank > 10:
        break
    metrics = build_canonical_metrics_from_payload(result.get("metric_values"))
    fast_candidates.append(
        {
            "candidate_id": f"grid_{rank:04d}",
            "rank": rank,
            "score": float(result.get("score", 0.0)),
            "metrics": metrics.model_dump(mode="json") if metrics else None,
        }
    )
```

Remove all verification imports, seeds, child cancellation, 50% progress, second result persistence, `engine_policy` parameters, and CLI parsing. `run_optimization_worker` must persist the fast result once, then mark the job completed. Simplify the service command builder to:

```python
return [
    sys.executable,
    "-m",
    _WORKER_MODULE,
    "--job-id",
    job_id,
    "--strategy-name",
    strategy_name,
    "--timeout-seconds",
    str(self._worker_timeout_seconds),
]
```

- [ ] **Step 5: Delete obsolete Optimization tests and verify GREEN**

Delete tests for requested verification and non-object engine policy. Retain parent timeout/cancel/heartbeat assertions but remove child-cancellation fixtures.

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/test_optimization_worker.py \
  tests/unit/server/services/test_optimization_service.py \
  tests/unit/server/test_run_contracts.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit Task 2**

```bash
git add apps/bt/src/application/services/run_contracts.py \
  apps/bt/src/application/services/optimization_service.py \
  apps/bt/src/application/workers/optimization_worker.py \
  apps/bt/tests/unit/server
git commit -m "refactor(bt): simplify optimization to vectorbt fast path"
```

---

### Task 3: Simplify Lab to one VectorBT stage

**Files:**
- Modify: `apps/bt/tests/server/routes/test_lab.py`
- Modify: `apps/bt/tests/unit/server/test_lab_worker.py`
- Modify: `apps/bt/src/application/services/lab_service.py`
- Modify: `apps/bt/src/application/workers/lab_worker.py`

**Interfaces:**
- Consumes: `build_canonical_metrics_from_payload`, Lab candidate evaluators and existing YAML save paths.
- Produces: fast-only generate/evolve/optimize results; fast winner save semantics; full-range Optuna progress.

- [ ] **Step 1: Write failing tests for no verification metadata and no candidate rebuild**

For generate/evolve/optimize service result tests add:

```python
assert not any(key.startswith("_verification") for key in result)
```

For Optimize, make verification-only trial reconstruction fail:

```python
optimizer.build_candidate_from_params.side_effect = AssertionError(
    "must not rebuild every trial for verification"
)
```

Assert `fast_candidates` remains score-ranked. Change the progress test to expect ordinary full progress:

```python
assert progress_updates[-1]["progress"] == pytest.approx(0.25)
```

for trial 1 of 4, without an engine-policy payload.

- [ ] **Step 2: Run focused Lab tests and verify RED**

```bash
uv run --directory apps/bt pytest \
  tests/server/routes/test_lab.py \
  tests/unit/server/test_lab_worker.py -q
```

Expected: FAIL because Lab results still contain internal verification keys, Optuna rebuilds all trial candidates, and progress can be halved for verification.

- [ ] **Step 3: Remove verification seed creation from LabService**

Remove `engine_policy` from `submit_generate`, `submit_evolve`, and `submit_optimize`, their run-spec parameters, and worker payloads. Remove verification imports and `_candidate_to_config_override` once unused.

Generate should return its result items and existing best-candidate save path only. Evolve/Optimize should retain top-10 `fast_candidates`, using the neutral metric helper, but omit `_verification_*` keys. In Optimize, do not call `build_candidate_from_params` inside the sorted-history display loop.

- [ ] **Step 4: Remove verification branches from LabWorker**

Delete `_resolve_engine_policy`, `_should_run_verification`, `_resolve_verified_complete_message`, `_save_verified_result`, verification child cancellation, and child orchestration. Pass `save` directly into the synchronous Lab method:

```python
save = bool(payload.get("save", False))
```

Always calculate Optuna progress as:

```python
progress = completed / total if total > 0 else 0.0
```

Persist the result once and mark the parent completed with its normal completion message.

- [ ] **Step 5: Remove obsolete Lab tests and verify GREEN**

Delete tests for fast-only metadata stripping, requested verification, engine-policy resolution, and verified-result saving. Keep parent timeout/cancel/heartbeat coverage without child jobs.

Run the command from Step 2. Expected: PASS.

- [ ] **Step 6: Commit Task 3**

```bash
git add apps/bt/src/application/services/lab_service.py \
  apps/bt/src/application/workers/lab_worker.py \
  apps/bt/tests/server/routes/test_lab.py \
  apps/bt/tests/unit/server/test_lab_worker.py
git commit -m "refactor(bt): simplify lab to vectorbt fast path"
```

---

### Task 4: Delete Nautilus runtime and verification contracts

**Files:**
- Modify: `apps/bt/src/domains/backtest/contracts.py`
- Modify: `apps/bt/src/application/services/run_contracts.py`
- Modify: `apps/bt/src/application/workers/backtest_worker.py`
- Modify: `apps/bt/src/entrypoints/http/routes/backtest.py`
- Modify: `apps/bt/tests/unit/server/test_backtest_worker.py`
- Modify: `apps/bt/tests/unit/server/test_run_contracts.py`
- Modify: `apps/bt/tests/unit/server/routes/test_backtest.py`
- Delete: `apps/bt/src/domains/backtest/nautilus_adapter.py`
- Delete: `apps/bt/src/domains/backtest/nautilus_metrics.py`
- Delete: `apps/bt/src/application/services/verification_orchestrator.py`
- Delete: `apps/bt/tests/unit/backtest/test_nautilus_adapter.py`
- Delete: `apps/bt/tests/unit/server/services/test_verification_orchestrator.py`

**Interfaces:**
- Consumes: VectorBT `BacktestRunner`, neutral `EngineFamily`, canonical contracts.
- Produces: `EngineFamily = {VECTORBT, UNKNOWN}` and a branch-free backtest worker.

- [ ] **Step 1: Tighten contract tests before deletion**

Replace the explicit-Nautilus run-contract test with:

```python
def test_engine_family_has_no_execution_alternative(self) -> None:
    assert {item.value for item in EngineFamily} == {"vectorbt", "unknown"}
```

Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/test_run_contracts.py -q
```

Expected: FAIL because `nautilus` is still an enum member.

- [ ] **Step 2: Remove Nautilus and verification models**

Delete `EngineFamily.NAUTILUS`, `EnginePolicyMode`, `EnginePolicy`, all `Verification*` models, and Nautilus policy version handling. Preserve `FastCandidateSummary`, `CanonicalExecutionMetrics`, `RunSpec`, `RunMetadata`, `CanonicalExecutionResult`, and artifact types.

Remove `_verification_candidates`, `_verification_requested_top_k`, and `_verification_scoring_weights` from `_INTERNAL_RAW_RESULT_KEYS`.

- [ ] **Step 3: Make BacktestWorker branch-free**

Remove Nautilus runner injection and engine dispatch. The execution block becomes:

```python
result = await asyncio.to_thread(
    resolved_runner.execute,
    strategy=effective_strategy_name,
    progress_callback=progress_callback,
    config_override=effective_config_override,
)
```

Keep claim, heartbeat, timeout, cancellation, result persistence, canonical refresh, and resource cleanup unchanged.

- [ ] **Step 4: Remove internal-child route filtering and delete files**

Change `_get_job_or_404` to check only job existence and make `list_jobs` map every manager result. Delete the three production modules and their dedicated unit tests listed above.

Delete worker tests for Nautilus dispatch/missing dependency and route tests for hiding verification child jobs.

- [ ] **Step 5: Run backend runtime regressions**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/test_backtest_worker.py \
  tests/unit/server/test_run_contracts.py \
  tests/unit/server/routes/test_backtest.py \
  tests/unit/server/test_optimization_worker.py \
  tests/unit/server/test_lab_worker.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit Task 4**

```bash
git add -A apps/bt/src/domains/backtest apps/bt/src/application \
  apps/bt/src/entrypoints/http/routes/backtest.py apps/bt/tests
git commit -m "refactor(bt): remove nautilus runtime"
```

---

### Task 5: Synchronize OpenAPI and simplify shared TypeScript contracts

**Files:**
- Create: `apps/ts/packages/contracts/src/types/vectorbt-only-contract.test-d.ts`
- Generate: `apps/ts/packages/contracts/openapi/bt-openapi.json`
- Generate: `apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts`
- Modify: `apps/ts/packages/api-clients/src/backtest/types.ts`
- Modify: `apps/ts/packages/api-clients/src/backtest/index.ts`
- Modify: `apps/ts/packages/api-clients/src/backtest/lab-result-schemas.ts`
- Modify: `apps/ts/packages/api-clients/src/backtest/lab-result-schemas.test.ts`
- Modify: `apps/ts/packages/api-clients/src/backtest/BacktestClient.test.ts`

**Interfaces:**
- Consumes: completed FastAPI schema from Tasks 1-4.
- Produces: generated VectorBT-only request types, retained engine provenance and fast-candidate schemas.

- [ ] **Step 1: Add a failing generated-contract type test**

Create:

```typescript
import type { components } from '../clients/backtest/generated/bt-api-types';

type Schemas = components['schemas'];
type Assert<T extends true> = T;
type AssertFalse<T extends false> = T;
type HasKey<T, K extends PropertyKey> = K extends keyof T ? true : false;
type RemovedPolicyKey = `engine_${'policy'}`;
type RemovedVerificationSchema = `Verification${'Summary'}`;
type RemovedEngineValue = `nauti${'lus'}`;

type _BacktestHasNoEngine = AssertFalse<HasKey<Schemas['BacktestRequest'], 'engine_family'>>;
type _OptimizationHasNoPolicy = AssertFalse<HasKey<Schemas['OptimizationRequest'], RemovedPolicyKey>>;
type _GenerateHasNoPolicy = AssertFalse<HasKey<Schemas['LabGenerateRequest'], RemovedPolicyKey>>;
type _EvolveHasNoPolicy = AssertFalse<HasKey<Schemas['LabEvolveRequest'], RemovedPolicyKey>>;
type _OptimizeHasNoPolicy = AssertFalse<HasKey<Schemas['LabOptimizeRequest'], RemovedPolicyKey>>;
type _HasNoVerificationSchema = AssertFalse<
  RemovedVerificationSchema extends keyof Schemas ? true : false
>;
type _RemovedEngineValueAbsent = AssertFalse<
  RemovedEngineValue extends Schemas['EngineFamily'] ? true : false
>;
type _VectorbtRemains = Assert<
  'vectorbt' extends Schemas['EngineFamily'] ? true : false
>;
type _UnknownRemains = Assert<
  'unknown' extends Schemas['EngineFamily'] ? true : false
>;
type _FastCandidatesRemain = Assert<
  HasKey<Schemas['OptimizationJobResponse'], 'fast_candidates'>
>;
```

- [ ] **Step 2: Verify RED against the old generated snapshot**

```bash
bun --cwd apps/ts run --filter @trading25/contracts typecheck
```

Expected: FAIL because the old generated contract still contains the removed fields and schemas.

- [ ] **Step 3: Regenerate OpenAPI and generated types**

```bash
bun --cwd apps/ts run --filter @trading25/contracts bt:sync
```

Do not hand-edit either generated file.

- [ ] **Step 4: Remove verification-only API-client aliases and Zod schemas**

Delete `EnginePolicy*` and `Verification*` aliases/exports. Preserve `EngineFamily`, `FastCandidateSummary`, `CanonicalExecutionMetrics`, and canonical run/artifact aliases.

Remove verification schemas and result fields from `lab-result-schemas.ts`. Update the Evolve/Optimize parsing tests to prove fast candidates survive parsing:

```typescript
const parsed = LabEvolveResultSchema.parse({
  lab_type: 'evolve',
  best_strategy_id: 'candidate-1',
  best_score: 1.25,
  history: [],
  fast_candidates: [
    { candidate_id: 'evolve_0001', rank: 1, score: 1.25, metrics: null },
  ],
  saved_strategy_path: null,
  saved_history_path: null,
});

expect(parsed.fast_candidates?.[0]?.candidate_id).toBe('evolve_0001');
```

Update the BacktestClient request fixture to:

```typescript
const request: BacktestRequest = { strategy_name: 'sma_cross' };
```

- [ ] **Step 5: Verify generated and client contracts GREEN**

```bash
bun --cwd apps/ts run --filter @trading25/contracts typecheck
bun --cwd apps/ts run --filter @trading25/contracts bt:check
bun --cwd apps/ts test \
  packages/api-clients/src/backtest/lab-result-schemas.test.ts \
  packages/api-clients/src/backtest/BacktestClient.test.ts
bun --cwd apps/ts run --filter @trading25/api-clients typecheck
```

Expected: PASS.

- [ ] **Step 6: Commit Task 5**

```bash
git add apps/ts/packages/contracts apps/ts/packages/api-clients
git commit -m "refactor(ts): sync vectorbt-only backtest contracts"
```

---

### Task 6: Remove engine-policy controls from Web forms

**Files:**
- Delete: `apps/ts/packages/web/src/components/EnginePolicySelector.tsx`
- Modify: `apps/ts/packages/web/src/components/Backtest/BacktestRunner.tsx`
- Modify: `apps/ts/packages/web/src/components/Backtest/BacktestRunner.test.tsx`
- Modify: `apps/ts/packages/web/src/hooks/useBacktest.test.tsx`
- Modify: `apps/ts/packages/web/src/components/Lab/LabGenerateForm.tsx`
- Modify: `apps/ts/packages/web/src/components/Lab/LabGenerateForm.test.tsx`
- Modify: `apps/ts/packages/web/src/components/Lab/LabEvolveForm.tsx`
- Modify: `apps/ts/packages/web/src/components/Lab/LabEvolveForm.test.tsx`
- Modify: `apps/ts/packages/web/src/components/Lab/LabOptimizeForm.tsx`
- Modify: `apps/ts/packages/web/src/components/Lab/LabOptimizeForm.test.tsx`

**Interfaces:**
- Consumes: VectorBT-only generated request types from Task 5.
- Produces: forms that send no engine/policy fields and expose no Engine Policy or Top K controls.

- [ ] **Step 1: Change form request expectations first**

Update expected payloads so Backtest sends:

```typescript
expect(mockMutateAsync).toHaveBeenCalledWith({
  strategy_name: 'production/example',
});
```

and Optimization/Lab expected payloads contain no `engine_policy`. Add UI assertions:

```typescript
expect(screen.queryByText('Engine Policy')).not.toBeInTheDocument();
expect(screen.queryByText('Top K')).not.toBeInTheDocument();
```

- [ ] **Step 2: Run Web form tests and verify RED/type failure**

```bash
bun --cwd apps/ts run --filter @trading25/web test -- \
  src/components/Backtest/BacktestRunner.test.tsx \
  src/components/Lab/LabGenerateForm.test.tsx \
  src/components/Lab/LabEvolveForm.test.tsx \
  src/components/Lab/LabOptimizeForm.test.tsx \
  src/hooks/useBacktest.test.tsx
```

Expected: FAIL because forms still render controls and send removed fields.

- [ ] **Step 3: Delete policy state and selector wiring**

Remove `EnginePolicyMode`, `buildEnginePolicy`, selector imports, local mode/Top-K state, child props, `engine_family: 'vectorbt'`, and all `engine_policy` assignments. Delete `EnginePolicySelector.tsx`. Update BacktestRunner copy so it describes grid details and terminal summaries without a verification stage.

- [ ] **Step 4: Verify Web forms GREEN**

Run the command from Step 2 and:

```bash
bun --cwd apps/ts run --filter @trading25/web typecheck
```

Expected: PASS.

- [ ] **Step 5: Commit Task 6**

```bash
git add -A apps/ts/packages/web/src/components apps/ts/packages/web/src/hooks
git commit -m "refactor(web): remove engine policy controls"
```

---

### Task 7: Replace verification UI with fast-candidate display

**Files:**
- Create: `apps/ts/packages/web/src/components/FastCandidatesSection.tsx`
- Create: `apps/ts/packages/web/src/components/FastCandidatesSection.test.tsx`
- Delete: `apps/ts/packages/web/src/components/VerificationSummarySection.tsx`
- Delete: `apps/ts/packages/web/src/components/VerificationSummarySection.test.tsx`
- Modify: `apps/ts/packages/web/src/components/Backtest/OptimizationJobProgressCard.tsx`
- Modify: `apps/ts/packages/web/src/components/Backtest/OptimizationJobProgressCard.test.tsx`
- Modify: `apps/ts/packages/web/src/components/Lab/LabGenerateResults.tsx`
- Modify: `apps/ts/packages/web/src/components/Lab/LabEvolveResults.tsx`
- Modify: `apps/ts/packages/web/src/components/Lab/LabOptimizeResults.tsx`
- Modify: `apps/ts/packages/web/src/components/Lab/LabJobProgress.tsx`
- Modify: `apps/ts/packages/web/src/components/Lab/LabJobProgress.test.tsx`
- Modify: `apps/ts/packages/web/src/components/Lab/LabJobHistoryTable.tsx`
- Modify: `apps/ts/packages/web/src/components/Lab/LabJobHistoryTable.test.tsx`

**Interfaces:**
- Consumes: `FastCandidateSummary[]` only.
- Produces: reusable fast-ranking card; ordinary progress/history with no stage inference.

- [ ] **Step 1: Write the FastCandidatesSection test first**

```typescript
render(
  <FastCandidatesSection
    fastCandidates={[
      {
        candidate_id: 'grid_0001',
        rank: 1,
        score: 1.2345,
        metrics: {
          total_return: 12.5,
          sharpe_ratio: 1.4,
          max_drawdown: -5.0,
          trade_count: 18,
        },
      },
    ]}
  />,
);

expect(screen.getByText('Fast Ranking')).toBeInTheDocument();
expect(screen.getByText('grid_0001')).toBeInTheDocument();
expect(screen.queryByText('Verification')).not.toBeInTheDocument();
```

Update progress/history tests to assert no Fast/Verification stage labels or Verification column while preserving the raw message, progress bar, Type, Strategy, and Created columns.

- [ ] **Step 2: Run focused UI tests and verify RED**

```bash
bun --cwd apps/ts run --filter @trading25/web test -- \
  src/components/FastCandidatesSection.test.tsx \
  src/components/Backtest/OptimizationJobProgressCard.test.tsx \
  src/components/Lab/LabJobProgress.test.tsx \
  src/components/Lab/LabJobHistoryTable.test.tsx
```

Expected: FAIL because the new component does not exist and old verification/stage UI is still rendered.

- [ ] **Step 3: Extract only the fast-ranking card**

Create the complete display component:

```typescript
import type { CanonicalExecutionMetrics, FastCandidateSummary } from '@trading25/api-clients/backtest';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';

interface FastCandidatesSectionProps {
  fastCandidates?: FastCandidateSummary[] | null;
  className?: string;
}

function formatMetric(value: number | null | undefined, digits = 2): string {
  if (value == null || Number.isNaN(value)) return '-';
  return value.toFixed(digits);
}

function formatMetricsLabel(metrics?: CanonicalExecutionMetrics | null): string {
  if (!metrics) return 'metrics unavailable';
  return [
    `ret ${formatMetric(metrics.total_return)}%`,
    `sh ${formatMetric(metrics.sharpe_ratio)}`,
    `dd ${formatMetric(metrics.max_drawdown)}%`,
    `tr ${formatMetric(metrics.trade_count, 0)}`,
  ].join(' / ');
}

export function FastCandidatesSection({
  fastCandidates,
  className,
}: FastCandidatesSectionProps) {
  if (!fastCandidates || fastCandidates.length === 0) return null;
  return (
    <Card className={className}>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm">Fast Ranking</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="overflow-auto rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="text-xs">Rank</TableHead>
                <TableHead className="text-xs">Candidate</TableHead>
                <TableHead className="text-right text-xs">Score</TableHead>
                <TableHead className="text-xs">Metrics</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {fastCandidates.map((candidate) => (
                <TableRow key={candidate.candidate_id}>
                  <TableCell className="text-xs">{candidate.rank}</TableCell>
                  <TableCell className="font-mono text-xs">{candidate.candidate_id}</TableCell>
                  <TableCell className="text-right text-xs font-medium">
                    {candidate.score.toFixed(4)}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {formatMetricsLabel(candidate.metrics)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}
```

Move the existing Fast Ranking card and metric formatting from `VerificationSummarySection`; do not move delta/status/mismatch code. Delete the old component and test.

- [ ] **Step 4: Remove stage and history inference**

Use `FastCandidatesSection` in Optimization, Evolve, and Optimize result surfaces. Generate has no `fast_candidates`, so remove its verification call without replacement. Delete `resolveStageLabel`, `resolveVerificationLabel`, message substring matching, and the Verification history column. Keep raw worker message/progress display.

- [ ] **Step 5: Verify UI GREEN**

Run the command from Step 2 and:

```bash
bun --cwd apps/ts run --filter @trading25/web typecheck
```

Expected: PASS.

- [ ] **Step 6: Commit Task 7**

```bash
git add -A apps/ts/packages/web/src/components
git commit -m "refactor(web): replace verification UI with fast ranking"
```

---

### Task 8: Remove dependency, CI, and active-guidance surfaces

**Files:**
- Create: `apps/bt/tests/unit/scripts/test_removed_nautilus_surfaces.py`
- Modify: `apps/bt/pyproject.toml`
- Generate: `apps/bt/uv.lock`
- Delete: `.github/workflows/nautilus-smoke.yml`
- Delete: `scripts/test-nautilus-smoke.sh`
- Delete: `apps/bt/tests/smoke/test_nautilus_runtime_smoke.py`
- Modify: `.github/dependabot.yml`
- Modify: `apps/bt/tests/unit/scripts/test_ci_workflow.py`
- Modify: `scripts/dep-direction-allowlist.txt`
- Modify: `AGENTS.md`
- Modify: `apps/bt/AGENTS.md`
- Modify: `README.md`
- Modify: `apps/bt/docs/parameter-optimization-system.md`
- Modify: `apps/bt/docs/parameter-optimization.md`
- Modify: `apps/bt/docs/research-core.md`
- Modify: `.codex/skills/bt-agent-system/SKILL.md`
- Modify: `.codex/skills/bt-optimization/SKILL.md`
- Modify: `.codex/skills/bt-research-workflow/SKILL.md`

**Interfaces:**
- Consumes: all completed runtime/API/UI deletions.
- Produces: dependency- and guidance-level removal guard; historical documents untouched.

- [ ] **Step 1: Add a failing active-surface regression guard**

Create a test which keeps forbidden tokens split so it does not match itself:

```python
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[5]
THIS_FILE = Path(__file__).resolve()
REMOVED_PATHS = (
    "apps/bt/src/domains/backtest/" + "nauti" + "lus_adapter.py",
    "apps/bt/src/domains/backtest/" + "nauti" + "lus_metrics.py",
    "apps/bt/src/application/services/verification_orchestrator.py",
    "apps/bt/tests/unit/backtest/test_" + "nauti" + "lus_adapter.py",
    "apps/bt/tests/unit/server/services/test_verification_orchestrator.py",
    "apps/bt/tests/smoke/test_" + "nauti" + "lus_runtime_smoke.py",
    ".github/workflows/" + "nauti" + "lus-smoke.yml",
    "scripts/test-" + "nauti" + "lus-smoke.sh",
    "apps/ts/packages/web/src/components/Engine" + "PolicySelector.tsx",
    "apps/ts/packages/web/src/components/Verification" + "SummarySection.tsx",
)
FORBIDDEN = (
    "nauti" + "lus",
    "fast_then_" + "verify",
    "verification_top_" + "k",
    "Engine" + "Policy",
    "Verification" + "Summary",
)
ACTIVE_ROOTS = (
    "apps/bt/src",
    "apps/bt/tests",
    "apps/bt/docs",
    "apps/ts/packages/api-clients/src",
    "apps/ts/packages/contracts/src",
    "apps/ts/packages/web/src",
    ".github/workflows",
    ".codex/skills",
)
ACTIVE_FILES = (
    "AGENTS.md",
    "README.md",
    "apps/bt/AGENTS.md",
    "apps/bt/pyproject.toml",
    "apps/bt/uv.lock",
    "apps/ts/packages/contracts/openapi/bt-openapi.json",
    ".github/dependabot.yml",
    "scripts/dep-direction-allowlist.txt",
)
TEXT_SUFFIXES = {".json", ".md", ".py", ".sh", ".toml", ".ts", ".tsx", ".txt", ".yaml", ".yml"}


def _active_files() -> list[Path]:
    files = [REPO_ROOT / relative for relative in ACTIVE_FILES]
    for relative_root in ACTIVE_ROOTS:
        root = REPO_ROOT / relative_root
        files.extend(
            path
            for path in root.rglob("*")
            if path.is_file()
            and path.suffix in TEXT_SUFFIXES
            and path.resolve() != THIS_FILE
            and "dist" not in path.parts
            and "node_modules" not in path.parts
        )
    return sorted(set(files))


def test_removed_paths_do_not_exist() -> None:
    assert [path for path in REMOVED_PATHS if (REPO_ROOT / path).exists()] == []


def test_active_surfaces_do_not_reference_removed_contracts() -> None:
    violations = []
    for path in _active_files():
        text = path.read_text(encoding="utf-8")
        for token in FORBIDDEN:
            if token.casefold() in text.casefold():
                violations.append(f"{path.relative_to(REPO_ROOT)}: {token}")
    assert violations == []
```

The roots intentionally exclude `docs/archive`, `issues`, and `docs/superpowers`.

- [ ] **Step 2: Run the guard and verify RED**

```bash
uv run --directory apps/bt pytest \
  tests/unit/scripts/test_removed_nautilus_surfaces.py -q
```

Expected: FAIL with remaining dependency, workflow, docs, and skill references.

- [ ] **Step 3: Remove dependency and CI surfaces**

Delete the `nautilus` dependency group and `nautilus_smoke` pytest marker. Delete the workflow/script/smoke test. Remove `nautilus-trader` from Dependabot and delete Nautilus-specific constants/tests from `test_ci_workflow.py` while keeping other workflow governance tests.

Remove the deleted UI paths from `scripts/dep-direction-allowlist.txt` and add `apps/ts/packages/web/src/components/FastCandidatesSection.tsx`, because the component imports `FastCandidateSummary` and `CanonicalExecutionMetrics` from the shared API client.

- [ ] **Step 4: Regenerate and verify the uv lock**

```bash
uv lock --directory apps/bt
uv lock --directory apps/bt --check
uv sync --directory apps/bt --locked
```

Expected: success, with no `nautilus-trader` package/group. Let uv decide which transitive packages remain.

- [ ] **Step 5: Rewrite active guidance to VectorBT-only behavior**

Remove optional Nautilus setup, two-stage engine policy, Top K, dedicated smoke CI, and Nautilus verification claims. State that Optimization/Lab use one VectorBT stage, retain ranked `fast_candidates`, and preserve ordinary timeout/cancel/terminal behavior.

Do not edit historical archives, completed issues, the approved design, or this plan.

- [ ] **Step 6: Verify cleanup guard and skill governance GREEN**

```bash
uv run --directory apps/bt pytest \
  tests/unit/scripts/test_removed_nautilus_surfaces.py \
  tests/unit/scripts/test_ci_workflow.py -q
python3 scripts/skills/audit_skills.py --strict-legacy
```

Expected: PASS.

- [ ] **Step 7: Commit Task 8**

```bash
git add -A apps/bt/pyproject.toml apps/bt/uv.lock apps/bt/tests \
  .github scripts AGENTS.md README.md apps/bt/AGENTS.md apps/bt/docs .codex/skills
git commit -m "chore: remove nautilus dependencies and guidance"
```

---

### Task 9: Run full cross-project verification

**Files:**
- Verify only; modify a file only when a failing check identifies a regression caused by Tasks 1-8.

**Interfaces:**
- Consumes: all prior task outputs.
- Produces: evidence that active runtime/contracts/UI contain no Nautilus-specific surface and existing VectorBT flows remain green.

- [ ] **Step 1: Run focused backend suites**

```bash
uv run --directory apps/bt pytest \
  tests/server/test_schemas.py \
  tests/server/routes/test_lab.py \
  tests/unit/server/routes/test_backtest.py \
  tests/unit/server/routes/test_optimize.py \
  tests/unit/server/services/test_backtest_service.py \
  tests/unit/server/services/test_optimization_service.py \
  tests/unit/server/test_backtest_worker.py \
  tests/unit/server/test_optimization_worker.py \
  tests/unit/server/test_lab_worker.py \
  tests/unit/server/test_run_contracts.py \
  tests/unit/scripts/test_ci_workflow.py \
  tests/unit/scripts/test_removed_nautilus_surfaces.py -q
```

Expected: PASS.

- [ ] **Step 2: Run backend static checks**

```bash
uv run --directory apps/bt ruff check src tests
uv run --directory apps/bt pyright src
```

Expected: PASS with no new warnings.

- [ ] **Step 3: Verify OpenAPI synchronization**

```bash
./scripts/check-contract-sync.sh
bun --cwd apps/ts run --filter @trading25/contracts bt:check
```

Expected: PASS and no generated drift.

- [ ] **Step 4: Run TypeScript tests and static checks**

```bash
bun --cwd apps/ts run --filter @trading25/api-clients test
bun --cwd apps/ts run --filter @trading25/web test
bun --cwd apps/ts run quality:typecheck
bun --cwd apps/ts run quality:lint
```

Expected: PASS.

- [ ] **Step 5: Run active and historical reference scans separately**

Active scan, expected zero matches:

```bash
rg -n -i --hidden \
  'nautilus|fast_then_verify|verification_top_k|engine_policy|EnginePolicy|VerificationSummary' \
  AGENTS.md README.md apps/bt/AGENTS.md \
  apps/bt/pyproject.toml apps/bt/uv.lock apps/bt/src apps/bt/tests \
  .github scripts .codex/skills apps/bt/docs \
  apps/ts/packages/api-clients/src apps/ts/packages/contracts/openapi \
  apps/ts/packages/contracts/src apps/ts/packages/web/src \
  --glob '!.git/**'
```

Historical scan, matches allowed and reviewed:

```bash
rg -n -i \
  'nautilus|fast_then_verify|verification_top_k|engine_policy' \
  docs/archive issues docs/superpowers
```

- [ ] **Step 6: Inspect final diff and commit any verification-only fixes**

```bash
git diff --check
git status --short
git diff --stat
```

If Task 9 required fixes, stage only those files and commit:

```bash
git commit -m "test: verify vectorbt-only execution"
```

If no fixes were required, do not create an empty commit.
