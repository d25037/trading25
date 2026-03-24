import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { Header } from './Header';

const mockNavigate = vi.fn();
let pathname = '/charts';

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => mockNavigate,
  useRouterState: ({ select }: { select: (state: { location: { pathname: string } }) => string }) =>
    select({ location: { pathname } }),
}));

vi.mock('@/components/ui/theme-toggle', () => ({
  ThemeToggle: () => <button type="button">ThemeToggle</button>,
}));

describe('Header', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    pathname = '/charts';
  });

  it('renders logo and navigation items', () => {
    render(<Header />);

    expect(screen.getByText('Trading25')).toBeInTheDocument();
    expect(screen.getByText('Charts')).toBeInTheDocument();
    expect(screen.getByText('Portfolio')).toBeInTheDocument();
    expect(screen.getByText('Indices')).toBeInTheDocument();
    expect(screen.getByText('N225 Options')).toBeInTheDocument();
    expect(screen.getByText('Screening')).toBeInTheDocument();
    expect(screen.getByText('Ranking')).toBeInTheDocument();
    expect(screen.getByText('Backtest')).toBeInTheDocument();
    expect(screen.getByText('Market DB')).toBeInTheDocument();
  });

  it('changes active tab on click', async () => {
    const user = userEvent.setup();

    render(<Header />);

    await user.click(screen.getByText('Portfolio'));

    expect(mockNavigate).toHaveBeenCalledWith({ to: '/portfolio' });
  });

  it('highlights current route', () => {
    pathname = '/screening';
    render(<Header />);

    expect(screen.getByRole('button', { name: 'Screening' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByRole('button', { name: 'Charts' })).toHaveAttribute('aria-pressed', 'false');
  });

  it('renders theme toggle', () => {
    render(<Header />);

    expect(screen.getByText('ThemeToggle')).toBeInTheDocument();
  });
});
