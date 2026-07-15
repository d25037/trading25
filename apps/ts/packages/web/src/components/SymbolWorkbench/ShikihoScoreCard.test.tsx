import { render, screen, within } from '@testing-library/react';
import type { ShikihoSnapshotV1 } from '@trading25/shikiho-extension/contract';
import { describe, expect, test } from 'vitest';
import { ShikihoScoreCard } from './ShikihoScoreCard';

const completeScore: ShikihoSnapshotV1['score'] = {
  overall: 3,
  growth: 1,
  profitability: 2,
  safety: 3,
  scale: 4,
  value: 5,
  priceMomentum: 2,
};

describe('ShikihoScoreCard', () => {
  test('renders the overall score as three of five filled stars', () => {
    render(<ShikihoScoreCard score={completeScore} />);

    const header = screen.getByTestId('shikiho-score-header');
    expect(within(header).getByRole('heading', { name: '四季報スコア' })).toBeInTheDocument();
    expect(within(header).getByText('3')).toHaveClass('text-red-500');

    const stars = within(header).getAllByTestId('shikiho-score-star');
    expect(stars).toHaveLength(5);
    expect(stars.filter((star) => star.dataset.state === 'filled')).toHaveLength(3);
    expect(stars.filter((star) => star.dataset.state === 'empty')).toHaveLength(2);
    expect(stars.slice(0, 3).every((star) => star.classList.contains('text-red-500'))).toBe(true);
    expect(stars.slice(3).every((star) => star.classList.contains('text-muted-foreground/25'))).toBe(true);
  });

  test('renders a centered responsive score body with an accessible labeled six-axis radar', () => {
    render(<ShikihoScoreCard score={completeScore} />);

    const body = screen.getByTestId('shikiho-score-body');
    expect(body).toHaveClass('mx-auto', 'max-w-3xl', 'md:grid-cols-[minmax(220px,260px)_minmax(0,1fr)]');

    const radar = within(body).getByRole('img', {
      name: '四季報スコア 成長性 1、収益性 2、安全性 3、規模 4、割安度 5、値上がり 2',
    });
    for (const label of ['成長性', '収益性', '安全性', '規模', '割安度', '値上がり']) {
      expect(within(radar).getByText(label)).toBeInTheDocument();
    }
    expect(within(radar).getAllByTestId('shikiho-score-vertex')).toHaveLength(6);
    expect(within(radar).getByTestId('shikiho-score-data-polygon')).toHaveClass(
      'fill-orange-400/20',
      'stroke-orange-500'
    );
  });

  test('renders the six values as a compact two-column definition list', () => {
    render(<ShikihoScoreCard score={completeScore} />);

    const metrics = screen.getByTestId('shikiho-score-values');
    expect(metrics).toHaveClass('grid-cols-2');
    for (const [label, value] of [
      ['成長性', '1'],
      ['収益性', '2'],
      ['安全性', '3'],
      ['規模', '4'],
      ['割安度', '5'],
      ['値上がり', '2'],
    ] as const) {
      const labelElement = within(metrics).getByText(label);
      expect(labelElement).toBeInTheDocument();
      expect(labelElement.nextElementSibling).toHaveTextContent(value);
    }
  });

  test('omits the radar and renders a dash when one axis is missing', () => {
    render(<ShikihoScoreCard score={{ ...completeScore, safety: null }} />);

    expect(screen.queryByRole('img')).not.toBeInTheDocument();
    const metrics = screen.getByTestId('shikiho-score-values');
    expect(within(metrics).getByText('安全性').nextElementSibling).toHaveTextContent('—');
  });
});
