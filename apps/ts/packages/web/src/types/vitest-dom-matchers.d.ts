type AttributeExpectation = RegExp | string;
type StyleExpectation = Record<string, number | string> | string;
type ValueExpectation = number | readonly string[] | string;

interface WebDomMatchers<R = unknown> {
  toBeInTheDocument(): R;
  toBeDisabled(): R;
  toBeEnabled(): R;
  toHaveAttribute(name: string, value?: AttributeExpectation): R;
  toHaveClass(...classNames: string[]): R;
  toHaveTextContent(expected: AttributeExpectation): R;
  toBeChecked(): R;
  toHaveValue(expectedValue: ValueExpectation): R;
  toHaveStyle(expectedStyle: StyleExpectation): R;
  toBeEmptyDOMElement(): R;
}

declare module 'vitest' {
  interface Assertion<T = unknown> extends WebDomMatchers<T> {}
  interface AsymmetricMatchersContaining extends WebDomMatchers {}
}

export {};
