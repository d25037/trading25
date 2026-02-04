# Testing Guide

This project uses Vitest for unit testing across all packages. Tests are located alongside source files with `.test.ts` or `.test.tsx` extensions.

## Quick Start

```bash
# Run all tests
bun run test

# Run tests with UI
bun run test:ui

# Run tests with coverage
bun run test:coverage
```

## Package-specific Testing

### Shared Package (`packages/shared/`)

Tests core business logic:
- **JQuantsClient**: Authentication, API calls, error handling
- **Technical Analysis**: SMA, EMA, MACD, PPO indicators with real market data
- **Timeframe Conversion**: Daily to weekly/monthly OHLC aggregation
- **Test Utilities**: Type-safe array helpers, mock data fixtures

```bash
cd packages/shared
bun run test                # Watch mode
bun run test:run        # Single run
bun run test:coverage   # With coverage
```

### Frontend Package (`packages/frontend/`)

Tests React components and server endpoints:
- **App Component**: User interactions, state management, API integration
- **Server API**: Hono HTML server endpoints

```bash
cd packages/frontend
bun run test                # Watch mode
bun run test:ui         # Interactive UI
bun run test:coverage   # With coverage
```

### CLI Package (`packages/cli/`)

Tests command-line interface:
- **Commands**: auth-status, stock, quote, margin
- **Error Handling**: Network failures, authentication errors
- **Output Formatting**: Console output validation

```bash
cd packages/cli
bun run test                # Watch mode
bun run test:coverage   # With coverage
```

## Test Structure

### Mock Data & Type-Safe Testing

Mock data fixtures and type-safe helpers are available:
```typescript
// Shared package - Type-safe array access
import { getFirstElementOrFail, getElementOrFail } from '../test-utils/array-helpers';
import { mockListedInfo, mockJQuantsConfig } from '../test-utils/fixtures';

// Frontend package  
import { mockEngineStatus, mockFetch } from './test-utils/mocks';

// CLI package
import { mockChalk } from './test-utils/mocks';
```

### Type-Safe Test Utilities

Use array helpers to avoid non-null assertions in tests:
```typescript
// ❌ Avoid
const firstResult = results[0]!;

// ✅ Use type-safe helpers instead
const firstResult = getFirstElementOrFail(results, 'Expected at least one result');
const specificResult = getElementOrFail(results, 2, 'Expected result at index 2');
```

### Testing Patterns

**API Client Testing:**
```typescript
it('should handle authentication errors', async () => {
  fetchSpy.mockResolvedValue(createMockErrorResponse('Auth failed', 401));
  await expect(client.getListedInfo()).rejects.toThrow();
});
```

**React Component Testing:**
```typescript
it('should render chart components', async () => {
  render(<App />);
  expect(screen.getByText('Trading25')).toBeInTheDocument();
  
  await waitFor(() => {
    expect(screen.getByTestId('chart-container')).toBeInTheDocument();
  });
});
```

**CLI Command Testing:**
```typescript
it('should display authentication status', async () => {
  mockJQuantsClient.getAuthStatus.mockReturnValue(mockAuthStatus);
  
  await action(); // Simulate command execution
  
  expect(mockConsole.log).toHaveBeenCalledWith(expect.stringContaining('Authentication Status'));
});
```

**Technical Analysis Testing:**
```typescript
import { loadToyotaData } from '../__fixtures__/toyota-data-loader';
import { getFirstElementOrFail } from '../test-utils/array-helpers';

it('should calculate SMA with real market data', () => {
  const prices = loadToyotaData().map(d => d.close);
  const result = sma(prices, 20);
  
  const firstSMA = getFirstElementOrFail(result, 'Expected SMA calculation result');
  expect(firstSMA).toBeGreaterThan(0);
});
```

**Timeframe Conversion Testing:**
```typescript
it('should convert daily to weekly OHLC', () => {
  const result = dailyToWeekly(testOHLCData);
  
  const weeklyData = getFirstElementOrFail(result.data, 'Expected weekly conversion result');
  expect(weeklyData.date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  expect(parseDate(weeklyData.date).getUTCDay()).toBe(1); // Monday
});
```

## Test Coverage Goals

- **Shared**: >90% - Core business logic
- **Frontend**: >85% - UI components and API endpoints  
- **CLI**: >80% - Command handlers and error scenarios

## Configuration

### Vitest Config

Each package has its own `vitest.config.ts`:
- **shared**: Node environment, TypeScript support
- **frontend**: jsdom environment, React testing, path aliases
- **cli**: Node environment, mock support for external dependencies

### Test Setup

Frontend package includes `test-setup.ts` for jest-dom matchers:
```typescript
import '@testing-library/jest-dom';
```

## Running Tests in CI

Tests can be run in CI environments:
```bash
bun run test:run        # All packages, single run
bun run test:coverage   # With coverage reports
```

Coverage reports are generated in each package's `coverage/` directory.

## Debugging Tests

Use Vitest UI for interactive debugging:
```bash
bun run test:ui
```

Or debug individual test files:
```bash
npx vitest run src/clients/JQuantsClient.test.ts
```

## Best Practices

1. **Test Naming**: Describe behavior, not implementation
2. **Type Safety**: Use array helpers, avoid biome-ignore and non-null assertions
3. **Mocking**: Mock external dependencies, not internal logic
4. **Assertions**: Use specific matchers for better error messages
5. **Cleanup**: Use beforeEach/afterEach for consistent test state
6. **Coverage**: Focus on critical paths and edge cases
7. **Real Data**: Use CSV fixtures for validation of complex calculations

## Code Quality in Tests

⚠️ **CRITICAL**: Follow strict Biome compliance even in test files:

- **No biome-ignore comments** - Use type-safe alternatives
- **No non-null assertions** - Use `getElementOrFail()` and similar helpers
- **Function complexity < 15** - Extract test helper functions
- **Proper error messages** - Provide descriptive failure messages

```typescript
// ❌ Avoid in tests
const result = data[0]!;
// biome-ignore lint/complexity/noExcessiveComplexity: test helper

// ✅ Use instead
const result = getFirstElementOrFail(data, 'Expected test data');
// Extract complex test logic into helper functions
```