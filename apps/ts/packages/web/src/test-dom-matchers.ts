import { expect } from 'vitest';

type AttributeExpectation = RegExp | string;
type StyleExpectation = Record<string, number | string> | string;
type ValueExpectation = number | readonly string[] | string;

let domMatchersRegistered = false;

function describeReceived(received: unknown): string {
  if (received === null) {
    return 'null';
  }
  if (received === undefined) {
    return 'undefined';
  }
  if (received instanceof Element) {
    return received.outerHTML;
  }
  if (received instanceof Node) {
    return received.nodeName;
  }
  return String(received);
}

function resolveElement(received: unknown): Element | null {
  return received instanceof Element ? received : null;
}

function normalizeText(value: string | null | undefined): string {
  return (value ?? '').replace(/\s+/g, ' ').trim();
}

function matchesText(actual: string, expected: AttributeExpectation): boolean {
  if (expected instanceof RegExp) {
    expected.lastIndex = 0;
    return expected.test(actual);
  }
  return actual === expected;
}

function parseExpectedStyles(expected: StyleExpectation): Map<string, string> {
  const style = document.createElement('div').style;

  if (typeof expected === 'string') {
    style.cssText = expected;
  } else {
    const styleDeclaration = style as unknown as Record<string, string>;
    for (const [property, value] of Object.entries(expected)) {
      if (property.includes('-')) {
        style.setProperty(property, String(value));
        continue;
      }
      styleDeclaration[property] = String(value);
    }
  }

  const parsedStyles = new Map<string, string>();
  for (let index = 0; index < style.length; index += 1) {
    const property = style.item(index);
    if (!property) {
      continue;
    }
    parsedStyles.set(property, style.getPropertyValue(property).trim());
  }
  return parsedStyles;
}

function getElementValue(element: Element): ValueExpectation | null {
  if (element instanceof HTMLSelectElement && element.multiple) {
    return Array.from(element.selectedOptions).map((option) => option.value);
  }
  if (
    element instanceof HTMLInputElement ||
    element instanceof HTMLOptionElement ||
    element instanceof HTMLSelectElement ||
    element instanceof HTMLTextAreaElement
  ) {
    return element.value;
  }
  return null;
}

function getInlineStyleValue(element: Element, property: string): string {
  if (element instanceof HTMLElement || element instanceof SVGElement) {
    return element.style.getPropertyValue(property).trim();
  }
  return '';
}

function areValuesEqual(actualValue: ValueExpectation, expectedValue: ValueExpectation): boolean {
  if (Array.isArray(actualValue) && Array.isArray(expectedValue)) {
    return (
      actualValue.length === expectedValue.length && actualValue.every((value, index) => value === expectedValue[index])
    );
  }

  if (typeof actualValue === 'string' && typeof expectedValue === 'number') {
    return Number(actualValue) === expectedValue;
  }

  return actualValue === expectedValue;
}

export function registerDomMatchers() {
  if (domMatchersRegistered) {
    return;
  }
  domMatchersRegistered = true;

  expect.extend({
    toBeInTheDocument(received: unknown) {
      const pass = received instanceof Node && received.isConnected;
      return {
        pass,
        message: () =>
          pass
            ? `Expected ${describeReceived(received)} not to be connected to the current document`
            : `Expected ${describeReceived(received)} to be connected to the current document`,
      };
    },

    toBeDisabled(received: unknown) {
      const element = resolveElement(received);
      const pass = element?.matches(':disabled') ?? false;
      return {
        pass,
        message: () =>
          pass
            ? `Expected ${describeReceived(received)} not to be disabled`
            : `Expected ${describeReceived(received)} to be disabled`,
      };
    },

    toBeEnabled(received: unknown) {
      const element = resolveElement(received);
      const pass = element !== null && !element.matches(':disabled');
      return {
        pass,
        message: () =>
          pass
            ? `Expected ${describeReceived(received)} not to be enabled`
            : `Expected ${describeReceived(received)} to be enabled`,
      };
    },

    toHaveAttribute(received: unknown, name: string, expectedValue?: AttributeExpectation) {
      const element = resolveElement(received);
      const actualValue = element?.getAttribute(name) ?? null;
      const pass =
        actualValue !== null && (expectedValue === undefined ? true : matchesText(actualValue, expectedValue));
      return {
        pass,
        message: () => {
          if (expectedValue === undefined) {
            return pass
              ? `Expected ${describeReceived(received)} not to have attribute "${name}"`
              : `Expected ${describeReceived(received)} to have attribute "${name}"`;
          }
          return pass
            ? `Expected attribute "${name}" not to equal ${String(expectedValue)}`
            : `Expected attribute "${name}" to equal ${String(expectedValue)}, received ${String(actualValue)}`;
        },
      };
    },

    toHaveClass(received: unknown, ...expectedClassNames: string[]) {
      const element = resolveElement(received);
      const expectedClasses = expectedClassNames.flatMap((className) => className.split(/\s+/).filter(Boolean));
      const classList = element?.classList ?? null;
      const missingClasses = expectedClasses.filter((className) => !classList?.contains(className));
      const pass = element !== null && missingClasses.length === 0;
      return {
        pass,
        message: () =>
          pass
            ? `Expected ${describeReceived(received)} not to contain classes ${expectedClasses.join(', ')}`
            : `Expected ${describeReceived(received)} to contain classes ${expectedClasses.join(', ')}, missing ${missingClasses.join(', ')}`,
      };
    },

    toHaveTextContent(received: unknown, expected: AttributeExpectation) {
      const element = resolveElement(received);
      const actualText = normalizeText(element?.textContent);
      const pass =
        element !== null &&
        (expected instanceof RegExp ? matchesText(actualText, expected) : actualText.includes(normalizeText(expected)));
      return {
        pass,
        message: () =>
          pass
            ? `Expected ${describeReceived(received)} not to contain text ${String(expected)}`
            : `Expected ${describeReceived(received)} to contain text ${String(expected)}, received ${actualText}`,
      };
    },

    toBeChecked(received: unknown) {
      const element = resolveElement(received);
      const pass =
        element !== null &&
        ((element instanceof HTMLInputElement && ['checkbox', 'radio'].includes(element.type) && element.checked) ||
          element.getAttribute('aria-checked') === 'true');
      return {
        pass,
        message: () =>
          pass
            ? `Expected ${describeReceived(received)} not to be checked`
            : `Expected ${describeReceived(received)} to be checked`,
      };
    },

    toHaveValue(received: unknown, expectedValue: ValueExpectation) {
      const element = resolveElement(received);
      const actualValue = element ? getElementValue(element) : null;
      const pass = actualValue !== null && areValuesEqual(actualValue, expectedValue);
      return {
        pass,
        message: () =>
          pass
            ? `Expected ${describeReceived(received)} not to have value ${JSON.stringify(expectedValue)}`
            : `Expected ${describeReceived(received)} to have value ${JSON.stringify(expectedValue)}, received ${JSON.stringify(actualValue)}`,
      };
    },

    toHaveStyle(received: unknown, expectedStyle: StyleExpectation) {
      const element = resolveElement(received);
      const expectedStyles = parseExpectedStyles(expectedStyle);
      const mismatches =
        element === null
          ? Array.from(expectedStyles.keys())
          : Array.from(expectedStyles.entries()).filter(([property, expectedValue]) => {
              const actualValue =
                getComputedStyle(element).getPropertyValue(property).trim() || getInlineStyleValue(element, property);
              return actualValue !== expectedValue;
            });
      const pass = element !== null && mismatches.length === 0;
      return {
        pass,
        message: () =>
          pass
            ? `Expected ${describeReceived(received)} not to match styles ${JSON.stringify(Object.fromEntries(expectedStyles))}`
            : `Expected ${describeReceived(received)} to match styles ${JSON.stringify(Object.fromEntries(expectedStyles))}, mismatches ${JSON.stringify(mismatches)}`,
      };
    },

    toBeEmptyDOMElement(received: unknown) {
      const element = resolveElement(received);
      const pass =
        element !== null &&
        Array.from(element.childNodes).every((node) => {
          if (node.nodeType === Node.COMMENT_NODE) {
            return true;
          }
          if (node.nodeType === Node.TEXT_NODE) {
            return normalizeText(node.textContent) === '';
          }
          return false;
        });
      return {
        pass,
        message: () =>
          pass
            ? `Expected ${describeReceived(received)} not to be an empty DOM element`
            : `Expected ${describeReceived(received)} to be an empty DOM element`,
      };
    },
  });
}
