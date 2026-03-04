// Mock implementation of bun:sqlite for Vitest
// Vitest doesn't understand Bun's built-in modules, so we provide a stub

export class Database {
  query(_sql: string) {
    return {
      all: () => [],
      get: () => null,
      run: () => ({ changes: 0, lastInsertRowid: 0 }),
    };
  }

  exec(_sql: string) {}

  close() {}

  prepare(_sql: string) {
    return {
      all: () => [],
      get: () => null,
      run: () => ({ changes: 0, lastInsertRowid: 0 }),
    };
  }

  transaction(fn: (...args: unknown[]) => unknown) {
    // Mock transaction - just execute the function
    return (...args: unknown[]) => fn(...args);
  }
}

export default { Database };
