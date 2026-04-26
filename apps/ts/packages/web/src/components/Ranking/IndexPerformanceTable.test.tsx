import { fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { IndexPerformanceItem } from '@/types/ranking';
import { IndexPerformanceTable } from './IndexPerformanceTable';

function createItem(overrides: Partial<IndexPerformanceItem>): IndexPerformanceItem {
  return {
    code: 'TOPIX',
    name: 'TOPIX',
    category: 'topix',
    currentDate: '2024-01-19',
    baseDate: '2024-01-16',
    currentClose: 1060,
    baseClose: 1020,
    changeAmount: 40,
    changePercentage: 3.92,
    lookbackDays: 3,
    ...overrides,
  };
}

function mockIndexMediaQuery(matches: boolean) {
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

describe('IndexPerformanceTable', () => {
  beforeEach(() => {
    vi.unstubAllGlobals();
  });
  it('renders empty state with selected lookback days', () => {
    render(
      <IndexPerformanceTable items={[]} isLoading={false} error={null} onIndexClick={vi.fn()} lookbackDays={10} />
    );

    expect(screen.getByText('Baseline: 10 trading sessions before each index close')).toBeInTheDocument();
    expect(screen.getByText('No index performance data available')).toBeInTheDocument();
  });

  it('falls back to 5-day baseline when no rows or selected lookback are provided', () => {
    render(<IndexPerformanceTable items={undefined} isLoading={false} error={null} onIndexClick={vi.fn()} />);

    expect(screen.getByText('Baseline: 5 trading sessions before each index close')).toBeInTheDocument();
  });

  it('sorts rows by change, category, and code, then handles row clicks', () => {
    const onIndexClick = vi.fn();
    render(
      <IndexPerformanceTable
        items={[
          createItem({ code: 'N225', name: 'Nikkei 225', category: 'market', changePercentage: 2.5 }),
          createItem({ code: 'JPX400', name: 'JPX 400', category: 'market', changePercentage: 2.5 }),
          createItem({ code: 'TOPIX', name: 'TOPIX', category: 'topix', changePercentage: 2.5 }),
          createItem({
            code: 'MOTHERS',
            name: 'Mothers',
            category: 'growth',
            changePercentage: -1.25,
            changeAmount: -12.5,
            currentClose: 987.5,
            baseClose: 1000,
          }),
        ]}
        isLoading={false}
        error={null}
        onIndexClick={onIndexClick}
      />
    );

    const [, ...dataRows] = screen.getAllByRole('row');
    expect(dataRows).toHaveLength(4);
    const [topixRow, jpx400Row, n225Row, mothersRow] = dataRows;
    if (!topixRow || !jpx400Row || !n225Row || !mothersRow) {
      throw new Error('Expected four data rows');
    }

    expect(topixRow).toHaveTextContent('TOPIX');
    expect(jpx400Row).toHaveTextContent('JPX400');
    expect(n225Row).toHaveTextContent('N225');
    expect(mothersRow).toHaveTextContent('MOTHERS');
    expect(screen.getByRole('columnheader', { name: '3D' })).toBeInTheDocument();

    fireEvent.click(jpx400Row);
    expect(onIndexClick).toHaveBeenCalledWith('JPX400');
  });

  it('renders mobile index cards and keeps index navigation', () => {
    const onIndexClick = vi.fn();
    mockIndexMediaQuery(true);

    render(
      <IndexPerformanceTable
        items={[createItem({ code: 'N225', name: 'Nikkei 225', category: 'market', changePercentage: 2.5 })]}
        isLoading={false}
        error={null}
        onIndexClick={onIndexClick}
      />
    );

    expect(screen.queryByRole('columnheader', { name: 'Code' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /N225/ }));
    expect(onIndexClick).toHaveBeenCalledWith('N225');
  });

  it('virtualizes long lists', () => {
    const items = Array.from({ length: 121 }, (_, index) =>
      createItem({
        code: `IDX${String(index).padStart(3, '0')}`,
        name: `Index ${index}`,
        category: index % 2 === 0 ? 'market' : 'style',
        changePercentage: 121 - index,
        currentClose: 1000 + index,
        baseClose: 900 + index,
      })
    );

    render(<IndexPerformanceTable items={items} isLoading={false} error={null} onIndexClick={vi.fn()} />);

    expect(screen.getByText('(121)')).toBeInTheDocument();
    expect(screen.getByText('IDX000')).toBeInTheDocument();
    expect(screen.queryByText('IDX120')).not.toBeInTheDocument();
  });
});
