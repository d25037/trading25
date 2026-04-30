import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { ValueCompositeRankingFilters } from './ValueCompositeRankingFilters';

describe('ValueCompositeRankingFilters', () => {
  it('changes score method with the segmented toggle', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(<ValueCompositeRankingFilters params={{ markets: 'standard', limit: 50 }} onChange={onChange} />);

    await user.click(screen.getByRole('button', { name: 'Equal weight' }));

    expect(onChange).toHaveBeenCalledWith({
      markets: 'standard',
      limit: 50,
      scoreMethod: 'equal_weight',
    });
  });

  it('changes to size tilt with the segmented toggle', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(<ValueCompositeRankingFilters params={{ markets: 'standard', limit: 50 }} onChange={onChange} />);

    await user.click(screen.getByRole('button', { name: 'Size tilt' }));

    expect(onChange).toHaveBeenCalledWith({
      markets: 'standard',
      limit: 50,
      scoreMethod: 'standard_size_tilt',
    });
  });

  it('uses PBR tilt as the fallback score method', () => {
    render(<ValueCompositeRankingFilters params={{ markets: 'standard', limit: 50 }} onChange={vi.fn()} />);

    expect(screen.getByRole('button', { name: 'PBR tilt' })).toHaveAttribute('aria-pressed', 'true');
  });

  it('changes forward EPS basis with the segmented toggle', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(
      <ValueCompositeRankingFilters
        params={{
          markets: 'standard',
          limit: 50,
          scoreMethod: 'standard_size_tilt',
          forwardEpsMode: 'latest',
        }}
        onChange={onChange}
      />
    );

    await user.click(screen.getByRole('button', { name: 'FY-only EPS' }));

    expect(onChange).toHaveBeenCalledWith({
      markets: 'standard',
      limit: 50,
      scoreMethod: 'standard_size_tilt',
      forwardEpsMode: 'fy',
    });
  });
});
