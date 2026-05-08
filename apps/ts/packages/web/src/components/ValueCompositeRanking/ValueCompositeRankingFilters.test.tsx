import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { ValueCompositeRankingFilters } from './ValueCompositeRankingFilters';

describe('ValueCompositeRankingFilters', () => {
  it('changes to the prime production profile with the segmented toggle', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(<ValueCompositeRankingFilters params={{ markets: 'standard', limit: 50 }} onChange={onChange} />);

    await user.click(screen.getByRole('button', { name: 'Prime size75' }));

    expect(onChange).toHaveBeenCalledWith({
      markets: 'prime',
      limit: 50,
      profileId: 'prime_size75_forward_per25',
      scoreMethod: undefined,
    });
  });

  it('uses standard breakout as the fallback profile', () => {
    render(<ValueCompositeRankingFilters params={{ markets: 'standard', limit: 50 }} onChange={vi.fn()} />);

    expect(screen.getByRole('button', { name: 'Standard 120d' })).toHaveAttribute('aria-pressed', 'true');
  });

  it('changes forward EPS basis with the segmented toggle', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(
      <ValueCompositeRankingFilters
        params={{
          markets: 'standard',
          limit: 50,
          profileId: 'standard_breakout_120d20',
          forwardEpsMode: 'latest',
        }}
        onChange={onChange}
      />
    );

    await user.click(screen.getByRole('button', { name: 'FY-only EPS' }));

    expect(onChange).toHaveBeenCalledWith({
      markets: 'standard',
      limit: 50,
      profileId: 'standard_breakout_120d20',
      forwardEpsMode: 'fy',
    });
  });

  it('toggles the ADV60 hard filter', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(
      <ValueCompositeRankingFilters
        params={{ markets: 'standard', limit: 50, profileId: 'standard_breakout_120d20', applyLiquidityFilter: true }}
        onChange={onChange}
      />
    );

    await user.click(screen.getByRole('switch', { name: 'ADV60 >= 10mn' }));

    expect(onChange).toHaveBeenCalledWith({
      markets: 'standard',
      limit: 50,
      profileId: 'standard_breakout_120d20',
      applyLiquidityFilter: false,
    });
  });
});
