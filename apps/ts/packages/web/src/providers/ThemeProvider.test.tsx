import { render, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ThemeProvider } from './ThemeProvider';

function mockMatchMedia(matches: boolean) {
  return vi.fn().mockImplementation(() => ({
    matches,
    media: '(prefers-color-scheme: dark)',
    onchange: null,
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
    addListener: vi.fn(),
    removeListener: vi.fn(),
  }));
}

describe('ThemeProvider', () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.classList.remove('light', 'dark');
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: mockMatchMedia(false),
    });
  });

  afterEach(() => {
    localStorage.clear();
    document.documentElement.classList.remove('light', 'dark');
  });

  it('falls back to default theme when stored theme is invalid', async () => {
    localStorage.setItem('theme', '[object Object]');

    render(
      <ThemeProvider defaultTheme="system">
        <div>content</div>
      </ThemeProvider>
    );

    await waitFor(() => {
      expect(document.documentElement.classList.contains('light')).toBe(true);
    });
    expect(localStorage.getItem('theme')).toBe('system');
  });

  it('applies valid stored theme value', async () => {
    localStorage.setItem('theme', 'dark');

    render(
      <ThemeProvider defaultTheme="system">
        <div>content</div>
      </ThemeProvider>
    );

    await waitFor(() => {
      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });
    expect(localStorage.getItem('theme')).toBe('dark');
  });
});
