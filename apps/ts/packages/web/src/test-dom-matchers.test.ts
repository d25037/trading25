import { beforeEach, describe, expect, it, vi } from 'vitest';
import { registerDomMatchers } from './test-dom-matchers';

function appendToBody<T extends Node>(node: T): T {
  document.body.append(node);
  return node;
}

function createOption(label: string, value: string, selected = false): HTMLOptionElement {
  const option = document.createElement('option');
  option.textContent = label;
  option.value = value;
  option.selected = selected;
  return option;
}

describe('test-dom-matchers', () => {
  beforeEach(() => {
    document.body.innerHTML = '';
  });

  it('registers matchers idempotently and handles document presence', () => {
    registerDomMatchers();
    registerDomMatchers();

    const element = appendToBody(document.createElement('div'));
    const textNode = appendToBody(document.createTextNode('hello'));

    expect(element).toBeInTheDocument();
    expect(textNode).toBeInTheDocument();
    expect(document.querySelector('[data-missing]')).not.toBeInTheDocument();
  });

  it('supports attribute, class, and text-content assertions', () => {
    const element = appendToBody(document.createElement('div'));
    element.setAttribute('data-state', 'open');
    element.className = 'panel is-active';
    element.textContent = '  Hello   Trading 25  ';

    expect(element).toHaveAttribute('data-state');
    expect(element).toHaveAttribute('data-state', 'open');
    expect(element).toHaveAttribute('data-state', /^op/);
    expect(element).toHaveClass('panel');
    expect(element).toHaveClass('panel is-active');
    expect(element).toHaveTextContent('Hello Trading');
    expect(element).toHaveTextContent(/trading 25/i);
    expect(element).not.toHaveAttribute('aria-hidden');
  });

  it('supports enabled, disabled, and checked states', () => {
    const disabledButton = appendToBody(document.createElement('button'));
    disabledButton.disabled = true;

    const enabledButton = appendToBody(document.createElement('button'));

    const checkbox = appendToBody(document.createElement('input'));
    checkbox.type = 'checkbox';
    checkbox.checked = true;

    const switchElement = appendToBody(document.createElement('div'));
    switchElement.setAttribute('role', 'switch');
    switchElement.setAttribute('aria-checked', 'true');

    expect(disabledButton).toBeDisabled();
    expect(disabledButton).not.toBeEnabled();
    expect(enabledButton).toBeEnabled();
    expect(enabledButton).not.toBeDisabled();
    expect(checkbox).toBeChecked();
    expect(switchElement).toBeChecked();
  });

  it('supports value assertions for scalar, numeric, and multi-select inputs', () => {
    const input = appendToBody(document.createElement('input'));
    input.value = 'alpha';

    const numberInput = appendToBody(document.createElement('input'));
    numberInput.type = 'number';
    numberInput.value = '42';

    const select = appendToBody(document.createElement('select'));
    select.multiple = true;

    const firstOption = createOption('First', 'first', true);
    const secondOption = createOption('Second', 'second', true);
    select.append(firstOption, secondOption);

    const container = appendToBody(document.createElement('div'));

    expect(input).toHaveValue('alpha');
    expect(numberInput).toHaveValue(42);
    expect(select).toHaveValue(['first', 'second']);
    expect(container).not.toHaveValue('alpha');
  });

  it('supports style assertions from string and object expectations', () => {
    const element = appendToBody(document.createElement('div'));
    element.style.height = '24px';
    element.style.color = 'rgb(239, 83, 80)';

    expect(element).toHaveStyle('height: 24px;');
    expect(element).toHaveStyle({ color: 'rgb(239, 83, 80)' });
    expect(document.querySelector('[data-missing-style]')).not.toHaveStyle('height: 24px;');
  });

  it('falls back to inline style lookup when computed styles are unavailable', () => {
    const svgElement = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svgElement.style.setProperty('--accent', '#0ea5e9');

    const xmlElement = new DOMParser().parseFromString('<root />', 'application/xml').documentElement;

    const getComputedStyleSpy = vi.spyOn(globalThis, 'getComputedStyle').mockImplementation(
      () =>
        ({
          getPropertyValue: () => '',
        }) as unknown as CSSStyleDeclaration
    );

    try {
      expect(svgElement).toHaveStyle('--accent: #0ea5e9;');
      expect(xmlElement).not.toHaveStyle('--accent: #0ea5e9;');
    } finally {
      getComputedStyleSpy.mockRestore();
    }
  });

  it('supports empty-dom-element assertions while ignoring comments and whitespace', () => {
    const empty = appendToBody(document.createElement('div'));
    empty.append(document.createComment('ignored'));
    empty.append(document.createTextNode('   '));

    const nonEmpty = appendToBody(document.createElement('div'));
    nonEmpty.append(document.createElement('span'));

    expect(empty).toBeEmptyDOMElement();
    expect(nonEmpty).not.toBeEmptyDOMElement();
  });

  it('reports failure messages for unsupported or negated assertions', () => {
    const element = appendToBody(document.createElement('div'));
    element.setAttribute('data-state', 'open');
    element.className = 'panel is-active';
    element.textContent = 'Hello Trading 25';
    element.style.color = 'rgb(239, 83, 80)';

    const empty = appendToBody(document.createElement('div'));
    const enabledButton = appendToBody(document.createElement('button'));
    const disabledButton = appendToBody(document.createElement('button'));
    disabledButton.disabled = true;

    const checkbox = appendToBody(document.createElement('input'));
    checkbox.type = 'checkbox';
    checkbox.checked = true;

    const input = appendToBody(document.createElement('input'));
    input.value = 'alpha';

    expect(() => expect(document.querySelector('[data-missing]')).toBeInTheDocument()).toThrow(/null/);
    expect(() => expect(undefined).toBeDisabled()).toThrow(/undefined/);
    expect(() => expect(element).not.toHaveAttribute('data-state')).toThrow(/not to have attribute/);
    expect(() => expect(element).not.toHaveAttribute('data-state', 'open')).toThrow(/not to equal/);
    expect(() => expect(element).not.toHaveClass('panel')).toThrow(/not to contain classes/);
    expect(() => expect(element).not.toHaveTextContent('Hello Trading')).toThrow(/not to contain text/);
    expect(() => expect(element).not.toHaveStyle({ color: 'rgb(239, 83, 80)' })).toThrow(/not to match styles/);
    expect(() => expect(empty).not.toBeEmptyDOMElement()).toThrow(/not to be an empty DOM element/);
    expect(() => expect(enabledButton).not.toBeEnabled()).toThrow(/not to be enabled/);
    expect(() => expect(disabledButton).not.toBeDisabled()).toThrow(/not to be disabled/);
    expect(() => expect(checkbox).not.toBeChecked()).toThrow(/not to be checked/);
    expect(() => expect(input).not.toHaveValue('alpha')).toThrow(/not to have value/);
  });
});
