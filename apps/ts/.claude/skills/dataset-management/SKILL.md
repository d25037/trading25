---
name: dataset-management
description: Dataset creation, management, and XDG-compliant path handling.
globs: "packages/shared/**/*.ts, packages/cli/**/*.ts, packages/api/**/*.ts"
alwaysApply: false
---

# Dataset Management

## XDG-Compliant Paths

**Market Database**: `$HOME/.local/share/trading25/market.db`
**Datasets Directory**: `$HOME/.local/share/trading25/datasets/`
**Portfolio Database**: `$HOME/.local/share/trading25/portfolio.db`

Customize with `XDG_DATA_HOME` environment variable.

## Dataset Presets

| Category | Preset | Description |
|----------|--------|-------------|
| Basic | `fullMarket` | All markets (Prime + Standard + Growth), 10 years |
| Basic | `primeMarket` | Prime market (cap >= 100B), 10 years |
| Basic | `standardMarket` | Standard market only, 10 years |
| Basic | `growthMarket` | Growth market only, 10 years |
| Testing | `quickTesting` | CI/dev (Prime, 3 stocks), 10 years [default] |
| Index | `topix100` | TOPIX 100 (Core30 + Large70), 10 years |
| Index | `topix500` | TOPIX 500 (Core30 + Large70 + Mid400), 10 years |
| Index | `mid400` | TOPIX Mid400 only, 10 years |
| Index | `primeExTopix500` | Prime excluding TOPIX 500, 10 years |

All presets include margin data (`includeMargin: true`).

**Single Source of Truth**: Preset names are defined in `DATASET_PRESET_NAMES` (`packages/shared/src/dataset/config/presets/metadata.ts`). CLI and API derive their types from this array — do not duplicate the definition.

## CLI Commands (Requires API Server)

```bash
# Start API server first
bun dev:api

# Create datasets
bun cli dataset create prime.db --preset primeMarket
bun cli dataset create markets/prime.db --preset primeMarket  # Subdirectory

# Resume incomplete dataset (fetch missing data)
bun cli dataset create prime.db --preset primeMarket --resume

# Manage datasets
bun cli dataset validate prime.db
bun cli dataset info prime.db --json
bun cli dataset sample prime.db --size 100 --by-market
bun cli dataset search prime.db toyota
```

## Database Sync Commands

```bash
bun cli db sync                    # Auto-detect (initial or incremental)
bun cli db sync --init             # Force 2-year initial sync
bun cli db validate                # Validate integrity, detect stock splits
bun cli db refresh 7203 6758       # Refetch specific stocks
bun cli db stats                   # Database statistics
```

## Path Security

```typescript
import { getDatasetPath } from '@trading25/shared/utils/dataset-paths';

// Valid paths
getDatasetPath('prime.db');           // → .../datasets/prime.db
getDatasetPath('markets/prime.db');   // → .../datasets/markets/prime.db

// Blocked (throws error)
getDatasetPath('/absolute/path.db');  // Absolute paths blocked
getDatasetPath('../outside.db');      // Parent references blocked
```

## Dataset Architecture

**Core Classes**:
- `DatasetBuilder` - Dataset creation with presets (supports `build()` and `buildResume()`)
- `DatasetReader` - High-performance queries (supports `getResumeStatus()` for missing data detection)
- `Database` - Unified database operations
- `DataFetcher` - Rate-limited API fetching
- `ApiClient` - JQuants API wrapper
- `RateLimiter` - Plan-based rate limiting

```typescript
import { DatasetBuilder, DatasetReader, presets, getDatasetPath } from '@trading25/shared';

const dbPath = getDatasetPath('prime.db');  // XDG-compliant path
const config = presets.primeMarket(dbPath);
const builder = new DatasetBuilder(config, jquantsClient);
await builder.build(progress => console.log(progress));

const reader = new DatasetReader(dbPath);
const data = await reader.getStockData('7203');
```


## Market Sync Features

- **Stock Split Detection**: Automatic detection of `adjustment_factor != 1.0`
- **Historical Refetch**: Code-based API calls for adjusted prices
- **TOPIX as Calendar**: Uses TOPIX data to detect trading days
- **Consecutive Failure Tracking**: Early termination after 5 failures

## Statements Field Coverage

`dataset info` displays financial statements field coverage with period-appropriate denominators:

| Field | Period | Notes |
|-------|--------|-------|
| EPS, Profit, Equity | All | Core fields |
| Next Year Forecast EPS | FY only | |
| BPS, Dividend | FY only | |
| Operating Cash Flow | FY + 2Q | |
| Sales, Operating Profit, Forecast EPS | All | Extended fields |
| Ordinary Profit | All | J-GAAP only (no US-GAAP/IFRS) |

**Schema Validation**: Datasets with outdated schema (missing extended fields) show validation error. Recreate with `--overwrite` to fix.
