import { fireEvent, render, screen } from '@testing-library/react';
import type { IndexPerformanceItem } from '@trading25/contracts/types/api-response-types';
import { beforeEach, describe, expect, it, vi } from 'vitest';
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

    expect(screen.getByText('基準: 各指数終値の 10 営業日前')).toBeInTheDocument();
    expect(screen.getByText('No index performance data available')).toBeInTheDocument();
  });

  it('falls back to 5-day baseline when no rows or selected lookback are provided', () => {
    render(<IndexPerformanceTable items={undefined} isLoading={false} error={null} onIndexClick={vi.fn()} />);

    expect(screen.getByText('基準: 各指数終値の 5 営業日前')).toBeInTheDocument();
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
    expect(screen.getByRole('columnheader', { name: '3日騰落率' })).toBeInTheDocument();

    fireEvent.click(jpx400Row);
    expect(onIndexClick).toHaveBeenCalledWith('JPX400');
  });

  it('renders sector strength score and bucket, sorted ahead of change for sector rows', () => {
    render(
      <IndexPerformanceTable
        items={[
          createItem({
            code: '0050',
            name: '東証業種別 輸送用機器',
            category: 'sector33',
            changePercentage: 10,
            sectorStrengthScore: 0.1,
            sectorStrengthBucket: 'sector_weak',
            sector20dTopixExcessPct: -3,
            sector60dTopixExcessPct: -5,
            sectorBreadth20dPct: 20,
            sectorStockCount: 12,
          }),
          createItem({
            code: '004F',
            name: '東証業種別 電気機器',
            category: 'sector33',
            changePercentage: 1,
            sectorStrengthScore: 0.9,
            sectorStrengthBucket: 'sector_strong',
            sector20dTopixExcessPct: 5,
            sector60dTopixExcessPct: 8,
            sectorBreadth20dPct: 70,
            sectorStockCount: 120,
          }),
        ]}
        isLoading={false}
        error={null}
        onIndexClick={vi.fn()}
        title="33業種指数"
      />
    );

    expect(screen.getByText('33業種指数')).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Trade Score' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Bucket' })).toBeInTheDocument();
    const [, ...dataRows] = screen.getAllByRole('row');
    expect(dataRows[0]).toHaveTextContent('004F');
    expect(dataRows[0]).toHaveTextContent('0.90');
    expect(dataRows[0]).toHaveTextContent('Strong');
    expect(dataRows[1]).toHaveTextContent('0050');
    expect(dataRows[1]).toHaveTextContent('0.10');
    expect(dataRows[1]).toHaveTextContent('Weak');
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

    expect(screen.queryByRole('columnheader', { name: 'コード' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /N225/ }));
    expect(onIndexClick).toHaveBeenCalledWith('N225');
  });

  it('keeps mobile virtualized index cards scrollable for long lists', () => {
    mockIndexMediaQuery(true);
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
    const { container } = render(
      <IndexPerformanceTable items={items} isLoading={false} error={null} onIndexClick={vi.fn()} />
    );
    const scrollArea = container.querySelector('.overflow-auto');

    expect(scrollArea).not.toBeNull();
    expect(screen.getByText('IDX000')).toBeInTheDocument();
    expect(screen.queryByText('IDX120')).not.toBeInTheDocument();
    expect(container.querySelector('[aria-hidden="true"][style*="height"]')).not.toBeNull();

    fireEvent.scroll(scrollArea as Element, { target: { scrollTop: 120 * 116 } });

    expect(screen.getByText('IDX120')).toBeInTheDocument();
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
