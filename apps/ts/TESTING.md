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

## E2E Smoke (Web)

Web package has Playwright smoke tests for browser-level critical paths.

```bash
cd packages/web

# Install Chromium browser used by CI/local smoke
bun run e2e:install

# Run smoke scenarios only
bun run e2e:smoke

# Open Playwright UI
bun run e2e:ui
```

## Package-specific Testing

### Utils Package (`packages/utils/`)

Tests shared utilities:
- **Test Utilities**: Type-safe array helpers, mock data fixtures
- **Runtime Helpers**: env/date/path/logger/error helper behavior

```bash
cd packages/utils
bun run test                # Watch mode
bun run test:coverage   # With coverage
```

### Contracts Package (`packages/contracts/`)

Tests OpenAPI contract sync logic and generated type guardrails:

```bash
cd packages/contracts
bun run test
bun run bt:generate-types
```

### Web Package (`packages/web/`)

Tests React components and server endpoints:
- **App Component**: User interactions, state management, API integration
- **FastAPI Proxy Integration**: `/api` contract consumers

```bash
cd packages/web
bun run test                # Watch mode
bun run test:ui         # Interactive UI
bun run test:coverage   # With coverage
```

## Test Structure

### Mock Data & Type-Safe Testing

Mock data fixtures and type-safe helpers are available:
```typescript
// Shared package - Type-safe array access
import { getFirstElementOrFail, getElementOrFail } from '@trading25/utils/test-utils';
import { mockListedInfo, mockJQuantsConfig } from '@trading25/utils/test-utils/fixtures';

// Frontend package  
import { mockEngineStatus, mockFetch } from './test-utils/mocks';

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

**Utility Module Testing:**
```typescript
import { getFirstElementOrFail } from '@trading25/utils/test-utils';

it('returns first element with strong typing', () => {
  const first = getFirstElementOrFail([1, 2, 3], 'Expected at least one value');
  expect(first).toBe(1);
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

- **Domain/Utils**: >80% - Core business logic
- **Web**: >45% lines / >70% functions

## Configuration

### Vitest Config

Each package has its own `vitest.config.ts`:
- **contracts/domain/utils**: Node environment, TypeScript support
- **web**: happy-dom environment, React testing, path aliases

### DOM Environment Policy

- `packages/web` uses `happy-dom` as the standard Vitest environment.
- Shared DOM gaps should be patched in `packages/web/src/test-setup.ts` before introducing a second DOM implementation.
- As of 2026-03-23, the web test suite no longer needs per-file `jsdom` overrides.
- If a future test truly requires `jsdom`, document the missing API or browser behavior inline in the test file and mirror that reason here.

### Test Setup

Web package loads local DOM matchers from `packages/web/src/test-dom-matchers.ts` via `packages/web/src/test-setup.ts`.
Keep matcher additions minimal and scoped to APIs that are already used across the suite.

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
