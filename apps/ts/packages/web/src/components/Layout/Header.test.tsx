import { fireEvent, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { Header } from './Header';

let pathname = '/symbol-workbench';

vi.mock('@tanstack/react-router', () => ({
  useRouterState: ({ select }: { select: (state: { location: { pathname: string } }) => string }) =>
    select({ location: { pathname } }),
  Link: ({
    to,
    children,
    onClick,
    className,
    ...props
  }: {
    to: string;
    children: ReactNode;
    onClick?: () => void;
    className?: string;
  }) => (
    <a
      href={to}
      onClick={(event) => {
        event.preventDefault();
        onClick?.();
      }}
      className={className}
      {...props}
    >
      {children}
    </a>
  ),
}));

vi.mock('@/components/ui/theme-toggle', () => ({
  ThemeToggle: () => <button type="button">ThemeToggle</button>,
}));

function mockHeaderMediaQuery(matches: boolean) {
  vi.stubGlobal(
    'matchMedia',
    vi.fn().mockImplementation((query: string) => ({
      matches,
      media: query,
      onchange: null,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      addListener: vi.fn(),
      removeListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }))
  );
}

describe('Header', () => {
  beforeEach(() => {
    pathname = '/symbol-workbench';
    vi.unstubAllGlobals();
  });

  it('renders logo, primary navigation items, and overflow navigation trigger', () => {
    render(<Header />);

    expect(screen.getByText('Trading25')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Symbol Workbench' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Portfolio' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Indices' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Research' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Screening' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Ranking' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Backtest' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Market DB' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'More' })).toBeInTheDocument();
  });

  it('keeps N225 Options in the overflow navigation menu', async () => {
    const user = userEvent.setup();

    render(<Header />);

    await user.click(screen.getByRole('button', { name: 'More' }));

    expect(screen.getByRole('link', { name: 'N225 Options' })).toBeInTheDocument();
    expect(screen.getAllByRole('link', { name: 'Research' })).toHaveLength(1);
    expect(screen.getAllByRole('link', { name: 'Market DB' })).toHaveLength(1);
  });

  it('places Research immediately to the left of Market DB in primary navigation', () => {
    render(<Header />);

    const primaryNavigationLabels = within(screen.getByRole('navigation'))
      .getAllByRole('link')
      .map((link) => link.textContent);

    expect(primaryNavigationLabels).toEqual([
      'Symbol Workbench',
      'Portfolio',
      'Indices',
      'Screening',
      'Ranking',
      'Backtest',
      'Research',
      'Market DB',
    ]);
  });

  it('uses a current-page mobile navigation trigger and moves all destinations into the menu', async () => {
    const user = userEvent.setup();
    mockHeaderMediaQuery(true);

    pathname = '/portfolio';
    render(<Header />);

    const mobileTrigger = screen.getByRole('button', { name: 'Portfolio' });
    expect(mobileTrigger).toHaveAttribute('data-state', 'active');
    expect(screen.queryByRole('button', { name: 'More' })).not.toBeInTheDocument();

    await user.click(mobileTrigger);

    expect(screen.getByText('Navigation')).toBeInTheDocument();
    expect(screen.getAllByRole('link', { name: 'Symbol Workbench' }).length).toBeGreaterThan(0);
    expect(
      screen.getAllByRole('link', { name: 'Portfolio' }).some((link) => link.getAttribute('aria-current') === 'page')
    ).toBe(true);
    expect(screen.getAllByRole('link', { name: 'Market DB' }).length).toBeGreaterThan(0);
  });

  it('highlights current route', () => {
    pathname = '/screening';
    render(<Header />);

    expect(screen.getByRole('link', { name: 'Screening' })).toHaveAttribute('aria-current', 'page');
    expect(screen.getByRole('link', { name: 'Symbol Workbench' })).not.toHaveAttribute('aria-current');
  });

  it('highlights Market DB as a primary route', () => {
    pathname = '/market-db';
    render(<Header />);

    expect(screen.getByRole('link', { name: 'Market DB' })).toHaveAttribute('aria-current', 'page');
    expect(screen.getByRole('button', { name: 'More' })).toHaveAttribute('data-state', 'inactive');
  });

  it('closes the overflow menu on outside click', async () => {
    const user = userEvent.setup();

    render(<Header />);

    await user.click(screen.getByRole('button', { name: 'More' }));
    expect(screen.getByRole('link', { name: 'N225 Options' })).toBeInTheDocument();

    fireEvent.pointerDown(document.body);

    expect(screen.queryByRole('link', { name: 'N225 Options' })).not.toBeInTheDocument();
  });

  it('closes the overflow menu on escape', async () => {
    const user = userEvent.setup();

    render(<Header />);

    await user.click(screen.getByRole('button', { name: 'More' }));
    expect(screen.getByRole('link', { name: 'N225 Options' })).toBeInTheDocument();

    fireEvent.keyDown(document, { key: 'Escape' });

    expect(screen.queryByRole('link', { name: 'N225 Options' })).not.toBeInTheDocument();
  });

  it('closes the overflow menu after selecting an overflow destination', async () => {
    const user = userEvent.setup();

    render(<Header />);

    await user.click(screen.getByRole('button', { name: 'More' }));
    expect(screen.getByRole('link', { name: 'N225 Options' })).toBeInTheDocument();

    await user.click(screen.getByRole('link', { name: 'N225 Options' }));

    expect(screen.queryByRole('link', { name: 'N225 Options' })).not.toBeInTheDocument();
  });

  it('closes the overflow menu when the route changes', async () => {
    const user = userEvent.setup();
    const { rerender } = render(<Header />);

    await user.click(screen.getByRole('button', { name: 'More' }));
    expect(screen.getByRole('link', { name: 'N225 Options' })).toBeInTheDocument();

    pathname = '/market-db';
    rerender(<Header />);

    expect(screen.queryByRole('link', { name: 'N225 Options' })).not.toBeInTheDocument();
  });

  it('renders theme toggle', () => {
    render(<Header />);

    expect(screen.getByText('ThemeToggle')).toBeInTheDocument();
  });
});
