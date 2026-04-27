import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { ValueCompositeRankingFilters } from './ValueCompositeRankingFilters';

describe('ValueCompositeRankingFilters', () => {
  it('changes score method with the segmented toggle', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(
      <ValueCompositeRankingFilters
        params={{ markets: 'standard', limit: 50, scoreMethod: 'walkforward_regression_weight' }}
        onChange={onChange}
      />
    );

    await user.click(screen.getByRole('button', { name: 'Equal weight' }));

    expect(onChange).toHaveBeenCalledWith({
      markets: 'standard',
      limit: 50,
      scoreMethod: 'equal_weight',
    });
  });

  it('changes forward EPS basis with the segmented toggle', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(
      <ValueCompositeRankingFilters
        params={{
          markets: 'standard',
          limit: 50,
          scoreMethod: 'walkforward_regression_weight',
          forwardEpsMode: 'latest',
        }}
        onChange={onChange}
      />
    );

    await user.click(screen.getByRole('button', { name: 'FY-only EPS' }));

    expect(onChange).toHaveBeenCalledWith({
      markets: 'standard',
      limit: 50,
      scoreMethod: 'walkforward_regression_weight',
      forwardEpsMode: 'fy',
    });
  });
});
