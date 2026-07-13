# Signal Attribution Contract Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the complete nine-model signal-attribution result graph application-owned and remove its final application-to-HTTP-schema dependency without changing runtime or wire behavior.

**Architecture:** Extend `src.application.contracts.backtest` with the existing nested attribution model graph. Keep the analyzer and persistence paths dictionary-based, validate only at the existing run-registry boundary, and make HTTP wrappers use module-qualified application annotations. Extend the existing static ownership guard for every migrated model name.

**Tech Stack:** Python 3.12, Pydantic 2, FastAPI, pytest, Ruff, Pyright, OpenAPI, Bun, generated TypeScript contracts

## Global Constraints

- Move exactly the nine result models named in the approved design; keep requests and HTTP response wrappers in the HTTP layer.
- Preserve every class name, annotation, Literal value, field description, default, default factory, and required field exactly.
- Preserve artifact → canonical payload → raw result candidate order and validation fallback behavior.
- Preserve all nine OpenAPI component names and generated TypeScript definitions with zero textual diff.
- Compatibility aliases, schema-layer re-exports, wrapper contracts, subclasses, and transitional imports are forbidden.
- The architecture guard must cover all nine names, not only `SignalAttributionResult`.
- The exact application-to-HTTP-schema baseline must decrease from 50 to 49 entries.
- Every production change follows a witnessed RED → GREEN test cycle.

---

### Task 1: Add the Canonical Signal Attribution Model Graph

**Files:**

- Modify: `apps/bt/src/application/contracts/backtest.py`
- Modify: `apps/bt/tests/unit/application/contracts/test_backtest.py`

**Interfaces:**

- Produces: the nine `SignalAttribution*` application contracts
- Consumes: Pydantic `BaseModel`, `Field`, `ValidationError`; typing `Literal`
- Preserves: the existing nested validation and serialization graph

- [ ] **Step 1: Add failing contract tests**

Extend `test_backtest.py` to import all nine not-yet-present models. Add tests that construct a complete `SignalAttributionResult` and assert its JSON payload; validate a minimal payload with omitted optional fields and omitted `scores`; and reject missing required lists and invalid Literal values.

The complete fixture must include:

```python
SignalAttributionResult(
    baseline_metrics=SignalAttributionMetrics(total_return=10.0, sharpe_ratio=1.1),
    signals=[
        SignalAttributionSignalResult(
            signal_id="entry.range_break",
            scope="entry",
            param_key="range_break",
            signal_name="Range Break",
            loo=SignalAttributionLooResult(
                status="ok",
                variant_metrics=SignalAttributionMetrics(total_return=8.0, sharpe_ratio=0.9),
                delta_total_return=2.0,
                delta_sharpe_ratio=0.2,
            ),
            shapley=SignalAttributionShapleyResult(
                status="ok",
                total_return=1.5,
                sharpe_ratio=0.1,
                method="exact",
                sample_size=8,
            ),
        )
    ],
    top_n_selection=SignalAttributionTopNSelection(
        top_n_requested=5,
        top_n_effective=1,
        selected_signal_ids=["entry.range_break"],
        scores=[SignalAttributionTopNScore(signal_id="entry.range_break", score=1.0)],
    ),
    timing=SignalAttributionTiming(
        total_seconds=1.0,
        baseline_seconds=0.2,
        loo_seconds=0.3,
        shapley_seconds=0.5,
    ),
    shapley=SignalAttributionShapleyMeta(
        method="exact", sample_size=8, evaluations=8
    ),
)
```

Use `model_validate` tests to assert:

- omitted `scores` becomes `[]`;
- omitted optional values become `None`;
- missing `signals` or `selected_signal_ids` raises `ValidationError`;
- `status="unknown"` and `scope="both"` raise `ValidationError`.

- [ ] **Step 2: Run and witness RED**

```bash
./scripts/bt-pytest.sh tests/unit/application/contracts/test_backtest.py
```

Expected: collection fails because the new `SignalAttribution*` names do not exist in `src.application.contracts.backtest`.

- [ ] **Step 3: Add the exact nine model definitions**

Add `Literal` to the typing imports and copy the current model graph into `application/contracts/backtest.py` without semantic edits:

```python
class SignalAttributionMetrics(BaseModel):
    total_return: float = Field(description="トータルリターン")
    sharpe_ratio: float = Field(description="シャープレシオ")


class SignalAttributionLooResult(BaseModel):
    status: Literal["ok", "error"] = Field(description="計算ステータス")
    variant_metrics: SignalAttributionMetrics | None = Field(default=None, description="当該シグナル無効化時のメトリクス")
    delta_total_return: float | None = Field(default=None, description="baseline - variant の total_return 差分")
    delta_sharpe_ratio: float | None = Field(default=None, description="baseline - variant の sharpe_ratio 差分")
    error: str | None = Field(default=None, description="エラー詳細")


class SignalAttributionShapleyResult(BaseModel):
    status: Literal["ok", "error"] = Field(description="計算ステータス")
    total_return: float | None = Field(default=None, description="total_returnへのShapley寄与")
    sharpe_ratio: float | None = Field(default=None, description="sharpe_ratioへのShapley寄与")
    method: str = Field(description="計算方式（exact/permutation/error）")
    sample_size: int | None = Field(default=None, description="計算に使ったサンプル数")
    error: str | None = Field(default=None, description="エラー詳細")


class SignalAttributionSignalResult(BaseModel):
    signal_id: str = Field(description="シグナル識別子（entry.<param_key> / exit.<param_key>）")
    scope: Literal["entry", "exit"] = Field(description="シグナルの適用スコープ")
    param_key: str = Field(description="SignalParams上のparam_key")
    signal_name: str = Field(description="表示用シグナル名")
    loo: SignalAttributionLooResult = Field(description="LOO寄与結果")
    shapley: SignalAttributionShapleyResult | None = Field(default=None, description="Shapley寄与結果（topN対象外はnull）")


class SignalAttributionTopNScore(BaseModel):
    signal_id: str = Field(description="シグナル識別子")
    score: float = Field(description="LOO絶対値正規化の合成スコア")


class SignalAttributionTopNSelection(BaseModel):
    top_n_requested: int = Field(description="要求されたTopN")
    top_n_effective: int = Field(description="実際に選定されたTopN")
    selected_signal_ids: list[str] = Field(description="Shapley計算対象のsignal_id一覧")
    scores: list[SignalAttributionTopNScore] = Field(default_factory=list, description="上位シグナルの選定スコア")


class SignalAttributionTiming(BaseModel):
    total_seconds: float = Field(description="総処理時間（秒）")
    baseline_seconds: float = Field(description="baseline計算時間（秒）")
    loo_seconds: float = Field(description="LOO計算時間（秒）")
    shapley_seconds: float = Field(description="Shapley計算時間（秒）")


class SignalAttributionShapleyMeta(BaseModel):
    method: str | None = Field(default=None, description="計算方式（exact/permutation/error）")
    sample_size: int | None = Field(default=None, description="近似時のサンプル数")
    error: str | None = Field(default=None, description="エラー詳細")
    evaluations: int | None = Field(default=None, description="評価実行回数")


class SignalAttributionResult(BaseModel):
    baseline_metrics: SignalAttributionMetrics = Field(description="ベースラインのメトリクス")
    signals: list[SignalAttributionSignalResult] = Field(description="シグナル別寄与結果")
    top_n_selection: SignalAttributionTopNSelection = Field(description="Shapley対象TopNの選定情報")
    timing: SignalAttributionTiming = Field(description="処理時間情報")
    shapley: SignalAttributionShapleyMeta = Field(description="Shapley計算メタ情報")
```

Preserve the existing Japanese class docstrings when moving the definitions.

- [ ] **Step 4: Run GREEN verification**

```bash
./scripts/bt-pytest.sh tests/unit/application/contracts/test_backtest.py
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt ruff check src/application/contracts/backtest.py tests/unit/application/contracts/test_backtest.py
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt pyright src/application/contracts/backtest.py
```

Expected: all contract tests, Ruff, and Pyright pass.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/application/contracts/backtest.py apps/bt/tests/unit/application/contracts/test_backtest.py
git commit -m "feat(bt): add canonical signal attribution contracts"
```

---

### Task 2: Delete HTTP Ownership and Migrate the Resolver

**Files:**

- Modify: `apps/bt/tests/unit/architecture/application_contract_boundary_guard.py`
- Modify: `apps/bt/tests/unit/architecture/test_layer_boundaries.py`
- Modify: `apps/bt/tests/unit/architecture/application_http_schema_imports.txt`
- Modify: `apps/bt/src/entrypoints/http/schemas/backtest.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/__init__.py`
- Modify: `apps/bt/src/application/services/run_registry.py`

**Interfaces:**

- Consumes: the nine canonical models from Task 1
- Produces: module-qualified application annotations in HTTP response wrappers
- Preserves: request/wrapper ownership, resolver candidate order, route behavior, and analyzer dictionaries

- [ ] **Step 1: Extend the ownership guard for all nine names**

Add every migrated model name to `FORBIDDEN_HTTP_APPLICATION_CONTRACT_NAMES`. Extend the synthetic direct-import and HTTP-binding cases so the root model is explicitly tested while the shared set enforces all nine names.

- [ ] **Step 2: Run and witness RED**

```bash
./scripts/bt-pytest.sh tests/unit/architecture/test_layer_boundaries.py
```

Expected: failures identify the nine current HTTP class bindings, the schema package export, and the `run_registry` direct import. Existing job and summary guard cases remain green.

- [ ] **Step 3: Remove the HTTP-owned graph and migrate consumers**

Delete the nine class definitions from `schemas/backtest.py` and remove its unused `Literal` import. Change the response wrapper fields to:

```python
result_data: backtest_contracts.SignalAttributionResult | None = Field(
    default=None,
    description="寄与分析結果（完了時のみ）",
)

result: backtest_contracts.SignalAttributionResult = Field(description="寄与分析結果")
```

In `run_registry.py`, import `SignalAttributionResult` from `src.application.contracts.backtest` together with `BacktestResultSummary`. Remove `SignalAttributionResult` from `schemas/__init__.py` imports and `__all__`.

Do not bind any of the nine application models in an HTTP schema module.

- [ ] **Step 4: Shrink the baseline**

Delete exactly:

```text
application/services/run_registry.py|src.entrypoints.http.schemas.backtest
```

Confirm the baseline contains exactly 49 non-comment entries.

- [ ] **Step 5: Run architecture and behavior verification**

```bash
./scripts/bt-pytest.sh \
  tests/unit/application/contracts/test_backtest.py \
  tests/unit/architecture/test_layer_boundaries.py \
  tests/unit/server/test_run_registry.py \
  tests/unit/server/routes/test_backtest.py \
  tests/unit/backtest/test_signal_attribution.py \
  tests/unit/server/services/test_backtest_attribution_service.py \
  tests/server/test_schemas.py
```

Expected: all tests pass; artifact/canonical/raw precedence, route job/result responses, analyzer shape, and persistence remain unchanged.

- [ ] **Step 6: Run static and contract verification**

```bash
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt ruff check src tests/unit/architecture tests/unit/application/contracts
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt pyright src
./scripts/check-dep-direction.sh
./scripts/check-contract-sync.sh
bun --cwd apps/ts run --filter @trading25/contracts typecheck
bun --cwd apps/ts run --filter @trading25/api-clients typecheck
git diff --exit-code -- apps/ts/packages/contracts/openapi/bt-openapi.json apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts
```

Expected: every command exits 0 and generated contracts have no diff.

- [ ] **Step 7: Commit**

```bash
git add -A apps/bt/src apps/bt/tests
git commit -m "refactor(bt): move signal attribution contracts to application"
```

---

### Task 3: Whole-Slice Verification and Review

**Files:**

- Verify only; no planned production changes

**Interfaces:**

- Consumes: Tasks 1–2
- Produces: completion evidence for the next DTO migration

- [ ] **Step 1: Re-run the complete relevant suite from clean HEAD**

Run the Task 2 architecture/behavior command plus:

```bash
./scripts/bt-pytest.sh tests/unit/application/contracts tests/unit/architecture tests/unit/server/test_run_registry.py tests/unit/server/routes/test_backtest.py tests/unit/backtest/test_signal_attribution.py tests/unit/server/services/test_backtest_attribution_service.py tests/server/test_schemas.py
```

Expected: all tests pass.

- [ ] **Step 2: Re-run static, contract, and repository checks**

```bash
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt ruff check src tests/unit/architecture tests/unit/application/contracts
UV_CACHE_DIR=/tmp/trading25-uv-cache uv run --directory apps/bt pyright src
./scripts/check-dep-direction.sh
./scripts/check-contract-sync.sh
bun --cwd apps/ts run --filter @trading25/contracts typecheck
bun --cwd apps/ts run --filter @trading25/api-clients typecheck
python3 scripts/skills/refresh_skill_references.py --check
git diff --check
git status --short
```

Expected: all commands exit 0, generated files are unchanged, baseline is 49, and worktree is clean.

- [ ] **Step 3: Request whole-branch review**

Review the design, plan, reports, and complete diff from the pre-slice commit. The reviewer must verify the nine-model closure, absence of compatibility surfaces, exact baseline reduction, resolver semantics, OpenAPI identity, TypeScript compatibility, and TDD evidence. Fix and re-review every Critical or Important finding before completion.

