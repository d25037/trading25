---
name: portfolio-management
description: Portfolio CRUD operations and stock holdings management.
globs: "packages/shared/**/*.ts, packages/cli/**/*.ts, packages/api/**/*.ts"
alwaysApply: false
---

# Portfolio Management

## Database Location

`$HOME/.local/share/trading25/portfolio.db` (XDG-compliant)
- Customize with `XDG_DATA_HOME` environment variable
- Automatically created on first use

## Database Schema (v1.0.0)

**portfolios** table:
- id, name (unique), description, created_at, updated_at

**portfolio_items** table:
- id, portfolio_id (FK), code, company_name, quantity, purchase_price
- purchase_date, account, notes, created_at, updated_at
- Constraints: UNIQUE(portfolio_id, code), quantity > 0, purchase_price > 0

## CLI Commands

```bash
# Create portfolio
bun cli portfolio create "My Portfolio" --description "Long-term holdings"

# List all portfolios
bun cli portfolio list

# Show portfolio details
bun cli portfolio show "My Portfolio"

# Add stock
bun cli portfolio add-stock "My Portfolio" 7203 \
  --quantity 100 --price 2500 --date 2024-01-01 \
  --account NISA --notes "Toyota Motor"

# Update stock
bun cli portfolio update-stock "My Portfolio" 7203 --quantity 150 --price 2600

# Remove stock
bun cli portfolio remove-stock "My Portfolio" 7203

# Delete portfolio
bun cli portfolio delete "My Portfolio"
```

## API Endpoints

**ID-based** (programmatic access):
| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/api/portfolio` | List all portfolios |
| POST | `/api/portfolio` | Create portfolio |
| GET | `/api/portfolio/{id}` | Get portfolio with items |
| PUT | `/api/portfolio/{id}` | Update portfolio |
| DELETE | `/api/portfolio/{id}` | Delete portfolio (cascade) |
| POST | `/api/portfolio/{id}/items` | Add stock |
| PUT | `/api/portfolio/{id}/items/{itemId}` | Update by item ID |
| DELETE | `/api/portfolio/{id}/items/{itemId}` | Remove by item ID |
| GET | `/api/portfolio/{id}/performance` | Performance with benchmark |
| GET | `/api/portfolio/{id}/factor-regression` | Factor regression analysis |

**Name+Code-based** (CLI/human access):
| Method | Endpoint | Purpose |
|--------|----------|---------|
| PUT | `/api/portfolio/{name}/stocks/{code}` | Update by name+code |
| DELETE | `/api/portfolio/{name}/stocks/{code}` | Remove by name+code |

## TypeScript Usage

```typescript
import { PortfolioDatabase, getPortfolioDbPath } from '@trading25/shared/portfolio';

const db = new PortfolioDatabase(getPortfolioDbPath());

// Create portfolio
const portfolio = db.createPortfolio({
  name: 'My Portfolio',
  description: 'Long-term holdings'
});

// Add stock
db.createPortfolioItem({
  portfolioId: portfolio.id,
  code: '7203',
  companyName: 'Toyota Motor',
  quantity: 100,
  purchasePrice: 2500,
  purchaseDate: new Date('2024-01-01'),
  account: 'NISA',
  notes: 'Blue chip stock'
});

// Query
const portfolios = db.getAllPortfolios();
const withItems = db.getPortfolioWithItems(portfolio.id);
const summaries = db.getPortfolioSummaries();

db.close();
```

## Performance Analysis

**Benchmark Comparison**:
- `GET /api/portfolio/{id}/performance?lookbackDays=30&benchmarkCode=TOPIX`
- Returns alpha, beta, correlation, R², relative return vs benchmark
- Supports TOPIX, TOPIX500, TOPIX100 as benchmark codes

**Metrics calculated**:
- **Alpha**: Annualized excess return (daily alpha × 252)
- **Beta**: Portfolio sensitivity to benchmark
- **Correlation**: Pearson correlation with benchmark
- **R²**: Regression fit quality
- **Relative Return**: Portfolio return minus benchmark return

**TypeScript Usage**:
```typescript
import { calculateBenchmarkMetrics } from '@trading25/shared/portfolio-performance';

const metrics = calculateBenchmarkMetrics(
  portfolioReturns,  // PerformanceDataPoint[]
  benchmarkPrices,   // PriceData[]
  'TOPIX',
  'TOPIX',
  30  // minimum data points
);
```

## Validation

- **Stock code**: 4 characters (e.g., 7203, 285A)
- **Duplicate prevention**: Same stock code cannot be added twice to same portfolio
- **Foreign keys**: Cascade delete on portfolio deletion
