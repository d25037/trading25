# Strategy Guide

## Sources of truth

- Runtime loading, category resolution, merge behavior, and strict validation live under
  [`src/domains/strategy/runtime/`](../src/domains/strategy/runtime/).
- The repository baseline is [`config/default.yaml`](../config/default.yaml). A runtime
  override from `TRADING25_DEFAULT_CONFIG_PATH` or
  `~/.local/share/trading25/config/default.yaml` takes precedence.
- `experimental`, `production`, and `legacy` strategies are normally stored under
  `~/.local/share/trading25/strategies/`. Project-owned `reference` examples live in
  [`config/strategies/reference/`](../config/strategies/reference/). Use the category-qualified
  name when a basename may be shadowed.
- The backend strict validator is authoritative. The Web editor displays backend validation,
  schema guidance, and metadata; it does not implement a separate strategy validator.
- [`contracts/strategy-config-v3.schema.json`](../../../contracts/strategy-config-v3.schema.json)
  is the current stable strategy contract. Runtime behavior and SoT boundaries also follow
  [`AGENTS.md`](../AGENTS.md) and the
  [architecture SoT matrix](../../../docs/architecture-sot-matrix.md).

## Shipped examples

- [`buy_and_hold.yaml`](../config/strategies/reference/buy_and_hold.yaml) is the smallest
  reference strategy.
- [`sma_cross.yaml`](../config/strategies/reference/sma_cross.yaml) demonstrates entry and exit
  signals.
- [`strategy_template.yaml`](../config/strategies/reference/strategy_template.yaml) is an
  authoring reference, not a promise that every commented combination is suitable for every
  execution policy or data scope.

`bt list` is the current inventory because it combines project-owned examples with the local
XDG strategy directories. Do not infer availability from a fixed list in documentation.

## List, validate, and run

Run commands from `apps/bt`:

```bash
uv run bt list
uv run bt validate reference/buy_and_hold
```

Before running a custom strategy, save a market-backed YAML such as the minimal example below
under the XDG `experimental` category. Then use its category-qualified name:

```bash
uv run bt validate experimental/my_strategy
uv run bt backtest experimental/my_strategy
```

`bt backtest` writes the normal result artifact set, including the static HTML report and
metrics JSON.

## YAML and backend validation

A minimal market-backed strategy can use this shape:

```yaml
display_name: My Strategy
description: Minimal market-backed example

shared_config:
  data_source: market
  universe_preset: prime
  start_date: "2024-01-04"
  end_date: "2024-12-30"
  universe_as_of_date: "2024-01-04"

entry_filter_params:
  buy_and_hold:
    enabled: true

exit_trigger_params: {}
```

`entry_filter_params` are combined as entry filters; enabled `exit_trigger_params` extend exit
behavior as triggers. The selected execution policy can impose additional rules, including an
empty exit block for round-trip modes.

Validate a saved file with `bt validate`. The Web authoring flow and API use
`POST /api/strategies/{strategy_name}/validate`, which performs strict nested-key validation,
production requirements, and compiled-strategy validation. Saving through
`PUT /api/strategies/{strategy_name}` also uses backend validation. `production` and
`experimental` YAML may be updated; rename and delete remain limited to `experimental`.

Normal backtest, research, lab, and screening strategies use `shared_config.data_source: market`
and an explicit `shared_config.universe_preset`. A physical dataset snapshot is only for an
archived reproducibility run with `data_source: dataset_snapshot`, `dataset_snapshot`, and
`static_universe: true` set together. Production YAML must declare its universe preset in the
strategy file rather than relying only on the default config.

## Signal registry and metadata

[`SIGNAL_REGISTRY`](../src/domains/strategy/signals/registry.py) is the runtime registry for
signal functions, parameter keys, categories, data requirements, availability policy, and
entry/exit support. [`SignalParams`](../src/shared/models/signals/composite.py) is the typed
parameter model used by strict YAML validation.

The backend derives authoring metadata from those sources:

- `GET /api/signals/reference` returns signal descriptions, fields, requirements, and usage
  guidance.
- `GET /api/signals/schema` returns the generated `SignalParams` JSON Schema.
- `GET /api/strategies/editor/reference` returns metadata used by the Strategy Editor.

Treat these responses as current metadata. Do not copy a fixed signal count or reimplement
parameter validation in the frontend or documentation.

## Optimization block

The strategy YAML top-level `optimization` block is the only normal runtime SoT for parameter
ranges:

```yaml
entry_filter_params:
  crossover:
    enabled: true
    type: sma
    direction: golden
    fast_period: 20
    slow_period: 100

optimization:
  description: SMA period search
  parameter_ranges:
    entry_filter_params:
      crossover:
        fast_period: [10, 20, 30]
        slow_period: [50, 100, 200]
```

Candidate lists must target parameters present in the strategy and must satisfy dependency
constraints such as `slow_period > fast_period`. The Web `Backtest > Strategies > Optimize`
flow can generate a draft, validate it, save it into the strategy YAML, and execute it. See
[`parameter-optimization.md`](./parameter-optimization.md) for the current contract.

Run a saved specification with:

```bash
uv run bt backtest experimental/my_strategy --optimize
```

Legacy `*_grid.yaml` files are not read by normal execution. Migrate them once into their
strategy YAML files with:

```bash
uv run bt migrate-optimization-specs
```

## Adding a strategy or signal

To add a strategy:

1. Start from a shipped reference and save the new YAML in the XDG `experimental` category.
2. Give it a distinct category-qualified name and set the intended market universe and signal
   parameters explicitly.
3. Validate it through the backend, then run a representative backtest before moving it to
   `production`.
4. Keep `reference` files project-owned. Confirm the resolver's actual path before editing a
   basename that can be shadowed by an XDG strategy.

To add a signal:

1. Implement the calculation in `src/domains/strategy/signals/` and add its typed parameters to
   `SignalParams`.
2. Register the function, parameter key, data requirements, availability policy, and entry/exit
   capability in `SIGNAL_REGISTRY`.
3. Update the stable strategy contract when the public YAML shape changes, and regenerate the
   OpenAPI/TypeScript contract if an API schema changes.
4. Add domain, validation, metadata, and execution tests. Confirm the generated reference and
   schema expose the new signal before authoring production YAML.

## Verification

For a strategy-only change, validate and exercise the exact category-qualified strategy:

```bash
uv run bt validate experimental/my_strategy
uv run bt backtest experimental/my_strategy
```

For runtime, signal, schema, or optimization changes, run the relevant focused tests first and
then the repository-root quality commands required by the change. The normal repository-root
test entry points are:

```bash
./scripts/test-packages.sh
./scripts/test-apps.sh
```

If the FastAPI schema changed, run this from `apps/ts` and commit both the OpenAPI snapshot and
generated TypeScript types:

```bash
bun run --filter @trading25/contracts bt:sync
```

`bt:sync` requires a successful source export; it does not use a running server or a stale
snapshot as fallback. Use `bt:generate-offline` only when intentionally regenerating types from
the already committed snapshot.
