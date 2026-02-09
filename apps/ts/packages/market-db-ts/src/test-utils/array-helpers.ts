/**
 * Type guard and helper functions for safe array access in tests
 * These utilities help avoid non-null assertions while maintaining type safety
 */

/**
 * Type guard to check if an array has an element at a specific index
 */
export function hasElementAt<T>(array: T[], index: number): array is T[] & { [K in typeof index]: T } {
  return index >= 0 && index < array.length && array[index] !== undefined;
}

/**
 * Type guard to check if an array is non-empty
 */
export function isNonEmptyArray<T>(array: T[]): array is [T, ...T[]] {
  return array.length > 0;
}

/**
 * Safely get an element from an array (returns undefined if not found)
 */
export function getElement<T>(array: T[], index: number): T | undefined {
  return array[index];
}

/**
 * Get an element from an array or throw an error if not found
 * Used in tests where we expect the element to exist
 */
export function getElementOrFail<T>(array: T[], index: number, message?: string): T {
  const element = array[index];
  if (element === undefined) {
    throw new Error(message || `Element at index ${index} not found in array of length ${array.length}`);
  }
  return element;
}

/**
 * Get the first element of an array or throw an error if empty
 */
export function getFirstElementOrFail<T>(array: T[], message?: string): T {
  return getElementOrFail(array, 0, message || 'Expected array to have at least one element');
}

/**
 * Get the last element of an array or throw an error if empty
 */
export function getLastElementOrFail<T>(array: T[], message?: string): T {
  if (array.length === 0) {
    throw new Error(message || 'Expected array to have at least one element');
  }
  return getElementOrFail(array, array.length - 1, message);
}

/**
 * Assert that an array has a specific length and return it with proper typing
 */
export function assertArrayLength<T>(
  array: T[],
  expectedLength: number,
  message?: string
): T[] & { length: typeof expectedLength } {
  if (array.length !== expectedLength) {
    throw new Error(message || `Expected array length ${expectedLength}, got ${array.length}`);
  }
  return array as T[] & { length: typeof expectedLength };
}

/**
 * Assert that an array has at least a minimum length
 */
export function assertMinArrayLength<T>(array: T[], minLength: number, message?: string): T[] & { length: number } {
  if (array.length < minLength) {
    throw new Error(message || `Expected array length >= ${minLength}, got ${array.length}`);
  }
  return array as T[] & { length: number };
}
