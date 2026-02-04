# trading25 Monorepo

## Structure
- `apps/ts` - TypeScript/Bun application and packages
- `apps/bt` - Python backtest application
- `contracts` - Shared, versioned contracts between apps
- `docs` - Project documentation

## Quick Start
### apps/ts (Bun)
```bash
cd apps/ts
bun install
bun run dev
```

### apps/bt (Python)
```bash
cd apps/bt
uv sync
uv run bt server --port 3002
```

Note: if you move the `apps/bt` directory (or rename the repo),
recreate the virtualenv so `uv run pytest` works:
```bash
uv sync --reinstall --locked
```

## Tests (Smoke)
```bash
cd apps/ts
bun run test
```

```bash
cd apps/bt
pytest
```
