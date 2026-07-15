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

    expect(screen.getByText('総合 3 / 5')).toBeInTheDocument();
    expect(screen.getAllByTestId('shikiho-score-star')).toHaveLength(5);
    expect(screen.getAllByTestId('shikiho-score-star-filled')).toHaveLength(3);
  });

  test('renders an accessible six-axis radar and all numeric metric values', () => {
    render(<ShikihoScoreCard score={completeScore} />);

    expect(
      screen.getByRole('img', {
        name: '四季報スコア 成長性 1、収益性 2、安全性 3、規模 4、割安度 5、値上がり 2',
      })
    ).toBeInTheDocument();

    const metrics = screen.getByTestId('shikiho-score-values');
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
