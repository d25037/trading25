# Dataset - Simplified Financial Data Management

## Overview

Dataset module provides **dramatically simplified** financial data management, reducing complexity from 82 files to ~15 files while maintaining all functionality.

## Key Improvements

- **82 files → 15 files** (80% reduction)
- **7 repository interfaces → 1 database class**
- **5 coordinators → 1 builder class**
- **Multiple factories → simple constructors**
- **Clear, linear control flow**
- **API-First**: All CLI operations now via API server with job-based async creation

## Quick Start

### Using via API (Recommended)

Dataset operations are now primarily accessed through the API server:

```bash
# Start API server
bun dev:api

# Create dataset
bun cli dataset create prime.db --preset primeMarket

# Query dataset
bun cli dataset info prime.db
bun cli dataset validate prime.db
bun cli dataset sample prime.db --size 100
bun cli dataset search prime.db toyota
```

### Direct Usage (For Custom Applications)

```typescript
import { DatasetBuilder, DatasetReader, presets } from '@trading25/shared/dataset';

// Build a dataset
const config = presets.primeMarket();
const builder = new DatasetBuilder(config, jquantsClient);
const result = await builder.build(progress => {
  console.log(`${progress.stage}: ${progress.processed}/${progress.total}`);
});

// Read data
const reader = new DatasetReader('./dataset.db');
const toyotaData = await reader.getStockData('7203');
const stats = await reader.getDatasetStats();
await reader.close();
```

### Custom Configuration
```typescript
import { createConfig } from '@trading25/shared/dataset';

const config = createConfig({
  outputPath: './custom.db',
  markets: ['prime', 'standard'],
  includeMargin: true,
  includeTOPIX: true,
  maxStocks: 100 // For testing
});
```

## Architecture

```
15 files in flat structure:
- types.ts (all type definitions)
- config.ts (configuration with presets)
- database.ts (unified SQLite operations)
- fetchers.ts (all API operations)
- builder.ts (dataset creation)
- reader.ts (data access)
- + 9 supporting files
```

## Core Classes

### `DatasetBuilder`
Primary class for creating datasets from JQuants API data.

```typescript
const builder = new DatasetBuilder(config, client);
const result = await builder.build(onProgress);
```

### `DatasetReader`
Primary class for reading data from existing datasets.

```typescript
const reader = new DatasetReader('./data.db');
const stocks = await reader.getStockList();
const quotes = await reader.getStockData('7203');
await reader.close();
```

### `Database` (DrizzleDatasetDatabase)
Internal SQLite operations (not typically used directly).

```typescript
import { Database } from '@trading25/shared/dataset';

const db = new Database('./data.db');
await db.insertStock(stockData);
await db.close();
```

## Configuration Presets

- **`presets.primeMarket()`** - Prime market stocks (10 years, full data)
- **`presets.fullMarket()`** - All markets (Prime + Standard + Growth, 10 years, full data)
- **`presets.testing()`** - Small subset for development/testing (10 stocks)
- **`presets.sample400Prime()`** - Random 400 stocks from Prime market
- **`presets.topix100()`** - TOPIX 100 stocks (Core30 + Large70)
- And more... (see `presets` object for full list)

## API Endpoints

The API server provides the following dataset endpoints:

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/dataset` | Start dataset creation job |
| GET | `/api/dataset/jobs/{jobId}` | Get job status |
| DELETE | `/api/dataset/jobs/{jobId}` | Cancel running job |
| GET | `/api/dataset/{name}/info` | Get dataset information |
| GET | `/api/dataset/{name}/validate` | Validate dataset integrity |
| GET | `/api/dataset/{name}/sample` | Sample stocks from dataset |
| GET | `/api/dataset/{name}/search` | Search stocks in dataset |

## Key Features

- **All original functionality maintained**
  - Stock quotes (OHLCV data)
  - Margin trading data
  - TOPIX index data
  - 33 sector indices
  - Financial statements
  - Rate limiting & progress reporting

- **Dramatically simplified**
  - Single point of entry for each operation
  - Clear, linear execution flow
  - Minimal configuration required

- **Better performance**
  - Fewer abstraction layers
  - Less object creation overhead
  - Direct database operations

- **Easier maintenance**
  - Fewer files to understand
  - Clear responsibility boundaries
  - Simpler testing requirements

## XDG-Compliant Storage

All datasets are stored in XDG Base Directory structure:

- **Default Location**: `$HOME/.local/share/trading25/datasets/`
- **Customizable**: Set `XDG_DATA_HOME` environment variable
- **Subdirectory Support**: e.g., `markets/prime.db`
- **Cross-Project Sharing**: Single dataset accessible from all projects

```typescript
import { getDatasetPath } from '@trading25/shared/utils/dataset-paths';

const path = getDatasetPath('prime.db');
// → $HOME/.local/share/trading25/datasets/prime.db
```

## Philosophy

This module follows the principle of **"Simplicity over patterns"** - choosing clear, readable code over complex design patterns when the problem doesn't warrant the complexity.
