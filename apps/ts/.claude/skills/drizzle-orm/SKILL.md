---
name: drizzle-orm
description: Drizzle ORM schema definitions and database operations.
globs: "packages/shared/src/db/**/*.ts, packages/api/**/*.ts"
alwaysApply: false
---

# Drizzle ORM Integration

## Module Structure

```
packages/shared/src/db/
├── index.ts                        # Main exports with backward-compatible aliases
├── columns/
│   └── stock-code.ts               # Auto-normalizing stock code column
├── constants/
│   └── index-master-data.ts        # JQuants index definitions
├── schema/
│   ├── portfolio-schema.ts         # Portfolio tables
│   ├── market-schema.ts            # Market data tables
│   └── dataset-schema.ts           # Dataset tables
├── drizzle-portfolio-database.ts   # Portfolio CRUD
├── drizzle-market-database.ts      # Market data operations
├── drizzle-market-reader.ts        # Market data queries
└── drizzle-dataset-database.ts     # Dataset CRUD
```

## Stock Code Normalization

Automatic conversion from 5-digit JQuants codes to 4-digit normalized codes:

```typescript
// Custom column type
import { stockCodeColumn } from '@trading25/shared/db/columns/stock-code';

// In schema
export const stocks = sqliteTable('stocks', {
  code: stockCodeColumn('code').primaryKey(),  // Auto-normalizes
  // ...
});
```

## Usage

**New Drizzle-based imports** (recommended):
```typescript
import {
  DrizzlePortfolioDatabase,
  DrizzleMarketDatabase,
  DrizzleDatasetDatabase
} from '@trading25/shared/db';
```

**Backward-compatible imports** (existing code continues to work):
```typescript
import {
  PortfolioDatabase,
  MarketDatabase,
  MarketDataReader,
  DatabaseV2
} from '@trading25/shared';
```

## Schema Examples

**Market Schema**:
```typescript
export const dailyQuotes = sqliteTable('daily_quotes', {
  id: integer('id').primaryKey({ autoIncrement: true }),
  code: stockCodeColumn('code').notNull(),
  date: text('date').notNull(),
  open: real('open'),
  high: real('high'),
  low: real('low'),
  close: real('close'),
  volume: integer('volume'),
  adjustmentFactor: real('adjustment_factor'),
});
```

**Portfolio Schema**:
```typescript
export const portfolios = sqliteTable('portfolios', {
  id: integer('id').primaryKey({ autoIncrement: true }),
  name: text('name').notNull().unique(),
  description: text('description'),
  createdAt: text('created_at').default(sql`CURRENT_TIMESTAMP`),
  updatedAt: text('updated_at').default(sql`CURRENT_TIMESTAMP`),
});

export const portfolioItems = sqliteTable('portfolio_items', {
  id: integer('id').primaryKey({ autoIncrement: true }),
  portfolioId: integer('portfolio_id').references(() => portfolios.id, { onDelete: 'cascade' }),
  code: stockCodeColumn('code').notNull(),
  quantity: integer('quantity').notNull(),
  purchasePrice: real('purchase_price').notNull(),
  // ...
});
```

## Index Master Data

Static constants for all JQuants index definitions:

```typescript
import { INDEX_MASTER_DATA, TOPIX_17_SECTORS, SECTOR_33 } from '@trading25/shared/db/constants';

// No API calls needed for index metadata
const topix17 = TOPIX_17_SECTORS;  // 17 industry groups
const sectors = SECTOR_33;         // 33 detailed sectors
```

## Key Features

- **Type-Safe Schema**: All tables defined with Drizzle
- **Auto-Normalization**: Stock codes automatically converted
- **Backward Compatibility**: Original class names exported as aliases
- **Static Constants**: Index master data without API calls
