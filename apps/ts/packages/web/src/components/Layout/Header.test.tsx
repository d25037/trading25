import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { Header } from './Header';

let pathname = '/charts';

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

describe('Header', () => {
  beforeEach(() => {
    pathname = '/charts';
  });

  it('renders logo, primary navigation items, and overflow navigation trigger', () => {
    render(<Header />);

    expect(screen.getByText('Trading25')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Charts' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Portfolio' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Indices' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Screening' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Ranking' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Backtest' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'More' })).toBeInTheDocument();
    expect(screen.queryByText('Research')).not.toBeInTheDocument();
  });

  it('reveals overflow navigation items from the more menu', async () => {
    const user = userEvent.setup();

    render(<Header />);

    await user.click(screen.getByRole('button', { name: 'More' }));

    expect(screen.getByRole('link', { name: 'Research' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'N225 Options' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Market DB' })).toBeInTheDocument();
  });

  it('highlights current route', () => {
    pathname = '/screening';
    render(<Header />);

    expect(screen.getByRole('link', { name: 'Screening' })).toHaveAttribute('aria-current', 'page');
    expect(screen.getByRole('link', { name: 'Charts' })).not.toHaveAttribute('aria-current');
  });

  it('shows overflow current route on the more navigation trigger', async () => {
    const user = userEvent.setup();

    pathname = '/market-db';
    render(<Header />);

    const moreTrigger = screen.getByRole('button', { name: 'Market DB' });

    expect(moreTrigger).toHaveAttribute('data-state', 'active');

    await user.click(moreTrigger);

    expect(screen.getByRole('link', { name: 'Market DB' })).toHaveAttribute('aria-current', 'page');
  });

  it('maps settings route to the market db overflow navigation state', async () => {
    const user = userEvent.setup();

    pathname = '/settings';
    render(<Header />);

    const moreTrigger = screen.getByRole('button', { name: 'Market DB' });

    expect(moreTrigger).toHaveAttribute('data-state', 'active');

    await user.click(moreTrigger);

    expect(screen.getByRole('link', { name: 'Market DB' })).toHaveAttribute('aria-current', 'page');
  });

  it('closes the overflow menu on outside click', async () => {
    const user = userEvent.setup();

    render(<Header />);

    await user.click(screen.getByRole('button', { name: 'More' }));
    expect(screen.getByRole('link', { name: 'Research' })).toBeInTheDocument();

    fireEvent.pointerDown(document.body);

    expect(screen.queryByRole('link', { name: 'Research' })).not.toBeInTheDocument();
  });

  it('closes the overflow menu on escape', async () => {
    const user = userEvent.setup();

    render(<Header />);

    await user.click(screen.getByRole('button', { name: 'More' }));
    expect(screen.getByRole('link', { name: 'Research' })).toBeInTheDocument();

    fireEvent.keyDown(document, { key: 'Escape' });

    expect(screen.queryByRole('link', { name: 'Research' })).not.toBeInTheDocument();
  });

  it('closes the overflow menu after selecting an overflow destination', async () => {
    const user = userEvent.setup();

    render(<Header />);

    await user.click(screen.getByRole('button', { name: 'More' }));
    expect(screen.getByRole('link', { name: 'Research' })).toBeInTheDocument();

    await user.click(screen.getByRole('link', { name: 'Research' }));

    expect(screen.queryByRole('link', { name: 'Research' })).not.toBeInTheDocument();
  });

  it('renders theme toggle', () => {
    render(<Header />);

    expect(screen.getByText('ThemeToggle')).toBeInTheDocument();
  });
});
