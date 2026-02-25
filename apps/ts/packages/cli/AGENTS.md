# CLI Package - Command Line Interface

Gunshi CLI for JQuants API, dataset management, and portfolio operations.

## Architecture

- **Framework**: Gunshi + Chalk + Ora + OutputManager
- **Environment**: Monorepo root `.env` (`<repo-root>/.env`) をSoTとして使用
- **Logger**: Node.js specific (separate from frontend)

## Command Groups

| Group | Purpose |
|-------|---------|
| `jquants auth` | Token refresh, status check |
| `jquants fetch` | Raw API data fetching |
| `db` | Market data sync, validate, refresh |
| `dataset` | Create, validate, info, sample, search |
| `analysis` | ROE, ranking, screening, factor-regression, portfolio-factor-regression |
| `portfolio` | Create, list, show, add/remove stocks |
| `watchlist` | Create, list, show, delete, add/remove stocks |
| `backtest` (`bt`) | Run strategies, cancel jobs, check results via trading25-bt backend |

## Structure

```
src/
├── index.ts              # Entry point
├── commands/
│   ├── analysis/         # ROE, ranking, screening
│   ├── db/               # sync, validate, refresh
│   ├── dataset/          # create, validate, info
│   ├── jquants/          # auth/, fetch/
│   ├── backtest/         # Backtest run, cancel, list, results, status, validate
│   ├── portfolio/        # CRUD operations
│   └── watchlist/        # Lightweight stock monitoring lists
└── utils/
    ├── api-client.ts     # API communication
    └── OutputManager.ts  # Progress reporting
```

## Gunshi Pattern

```typescript
import { define } from 'gunshi';

export const syncCommand = define({
  name: 'sync',
  description: 'Synchronize market data',
  args: {
    init: { type: 'boolean', description: 'Force initial sync' },
    debug: { type: 'boolean', description: 'Enable debug logging' },
  },
  run: async (ctx) => {
    const { init, debug } = ctx.values;
    // handler logic
  },
});
```

## Error Handling

Exception-based error propagation with `CLIError` hierarchy. `process.exit()` is forbidden outside `index.ts`.

```typescript
// Error classes (utils/error-handling.ts)
CLIError              // Base: exitCode, silent, cause
CLIValidationError    // Input validation (exitCode=1, silent=false)
CLINotFoundError      // Resource not found (exitCode=1, silent=false)
CLIAPIError           // API failures (exitCode=1, silent=false)
CLICancelError        // User cancellation (exitCode=0, silent=true)
```

### Catch block pattern (with spinner)

```typescript
} catch (error) {
  handleCommandError(error, spinner, {
    failMessage: 'Operation failed',
    debug,
    tips: ['Ensure the API server is running: uv run bt server --port 3002'],
  });
}
```

`handleCommandError` re-throws `CLIError` directly (with `spinner.stop()`), or wraps unknown errors as `CLIError(msg, 1, true)`.

### Catch block pattern (without spinner)

```typescript
} catch (error) {
  if (error instanceof CLIError) throw error;
  const errorMessage = error instanceof Error ? error.message : String(error);
  console.error(chalk.red(`✗ Failed: ${errorMessage}`));
  throw new CLIError(errorMessage, 1, true, { cause: error });
}
```

### Entry point (index.ts)

Uses `process.exitCode` for natural termination. Silent errors skip logging.

## Skills Reference

- **Dataset operations**: `dataset-management` skill
- **Analysis algorithms**: `financial-analysis` skill
- **Portfolio commands**: `portfolio-management` skill

## Development

```bash
bun run cli:dev                       # Development mode (from apps/ts)
bun run cli:build                     # Compile CLI package (from apps/ts)
bun run --filter @trading25/cli test  # Run tests
```
