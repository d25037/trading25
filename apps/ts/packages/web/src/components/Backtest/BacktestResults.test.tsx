import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { BacktestResults } from './BacktestResults';

vi.mock('./HtmlFileBrowser', () => ({
  HtmlFileBrowser: () => <div>Backtest Browser Content</div>,
}));

vi.mock('./OptimizationHtmlFileBrowser', () => ({
  OptimizationHtmlFileBrowser: () => <div>Optimization Browser Content</div>,
}));

describe('BacktestResults', () => {
  it('renders backtest tab content by default', () => {
    render(<BacktestResults />);

    expect(screen.getByRole('button', { name: 'Backtest' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Optimization' })).toBeInTheDocument();
    expect(screen.getByText('Backtest Browser Content')).toBeInTheDocument();
    expect(screen.queryByText('Optimization Browser Content')).not.toBeInTheDocument();
  });

  it('switches between backtest and optimization tabs', async () => {
    const user = userEvent.setup();
    render(<BacktestResults />);

    await user.click(screen.getByRole('button', { name: 'Optimization' }));
    expect(screen.getByText('Optimization Browser Content')).toBeInTheDocument();
    expect(screen.queryByText('Backtest Browser Content')).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Backtest' }));
    expect(screen.getByText('Backtest Browser Content')).toBeInTheDocument();
    expect(screen.queryByText('Optimization Browser Content')).not.toBeInTheDocument();
  });
});
