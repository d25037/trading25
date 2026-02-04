import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { useUiStore } from '@/stores/uiStore';
import { Header } from './Header';

vi.mock('@/components/ui/theme-toggle', () => ({
  ThemeToggle: () => <button type="button">ThemeToggle</button>,
}));

describe('Header', () => {
  beforeEach(() => {
    useUiStore.setState({ activeTab: 'charts' });
  });

  it('renders logo and navigation items', () => {
    render(<Header />);

    expect(screen.getByText('Trading25')).toBeInTheDocument();
    expect(screen.getByText('Charts')).toBeInTheDocument();
    expect(screen.getByText('Portfolio')).toBeInTheDocument();
    expect(screen.getByText('Indices')).toBeInTheDocument();
    expect(screen.getByText('Analysis')).toBeInTheDocument();
    expect(screen.getByText('Backtest')).toBeInTheDocument();
    expect(screen.getByText('Settings')).toBeInTheDocument();
  });

  it('changes active tab on click', async () => {
    const user = userEvent.setup();

    render(<Header />);

    await user.click(screen.getByText('Portfolio'));

    expect(useUiStore.getState().activeTab).toBe('portfolio');
  });

  it('renders theme toggle', () => {
    render(<Header />);

    expect(screen.getByText('ThemeToggle')).toBeInTheDocument();
  });
});
