import { render, screen } from '@testing-library/react';
import { BarChart3, TrendingUp } from 'lucide-react';
import { describe, expect, it } from 'vitest';
import { SummaryMetrics } from './SummaryMetrics';

describe('SummaryMetrics', () => {
  it('renders metric items with tone, meta, and custom grid columns', () => {
    const { container } = render(
      <SummaryMetrics
        columns={2}
        className="summary-grid"
        items={[
          {
            icon: TrendingUp,
            label: 'Win Rate',
            value: '62.1%',
            meta: 'Last 30 sessions',
            tone: 'positive',
          },
          {
            icon: BarChart3,
            label: 'Signals',
            value: '18',
          },
        ]}
      />
    );

    expect(screen.getByText('Win Rate')).toBeInTheDocument();
    expect(screen.getByText('62.1%')).toHaveClass('text-green-600');
    expect(screen.getByText('Last 30 sessions')).toBeInTheDocument();
    expect(screen.getByText('Signals')).toBeInTheDocument();
    expect(screen.getByText('18')).toBeInTheDocument();
    expect(container.firstChild).toHaveClass('summary-grid');
  });
});
