import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Filter, TrendingUp } from 'lucide-react';
import { describe, expect, it, vi } from 'vitest';
import {
  ModeSwitcherPanel,
  NavRail,
  SectionEyebrow,
  SegmentedTabs,
  SplitLayout,
  SplitMain,
  SplitSidebar,
  Surface,
} from './Workspace';

describe('Workspace layout primitives', () => {
  it('renders surface and eyebrow helpers', () => {
    render(
      <>
        <Surface className="custom-surface">Body</Surface>
        <SectionEyebrow className="custom-eyebrow">Label</SectionEyebrow>
      </>
    );

    expect(screen.getByText('Body')).toHaveClass('custom-surface');
    expect(screen.getByText('Label')).toHaveClass('custom-eyebrow');
  });

  it('handles segmented tab interactions and disabled states', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(
      <SegmentedTabs
        value="alpha"
        onChange={onChange}
        className="segmented-root"
        itemClassName="segmented-item"
        items={[
          { value: 'alpha', label: 'Alpha', icon: Filter },
          { value: 'beta', label: 'Beta' },
          { value: 'gamma', label: 'Gamma', icon: TrendingUp, disabled: true },
        ]}
      />
    );

    const activeTab = screen.getByRole('button', { name: 'Alpha' });
    const inactiveTab = screen.getByRole('button', { name: 'Beta' });
    const disabledTab = screen.getByRole('button', { name: 'Gamma' });

    await user.click(activeTab);
    await user.click(inactiveTab);
    await user.click(disabledTab);

    expect(onChange).toHaveBeenCalledTimes(1);
    expect(onChange).toHaveBeenCalledWith('beta');
    expect(activeTab).toHaveAttribute('aria-pressed', 'true');
    expect(inactiveTab).toHaveClass('segmented-item');
    expect(disabledTab).toBeDisabled();
  });

  it('renders mode switcher and split layout shells', () => {
    render(
      <ModeSwitcherPanel
        label="Mode"
        value="alpha"
        onChange={() => {}}
        className="panel-shell"
        itemClassName="panel-item"
        items={[
          { value: 'alpha', label: 'Alpha', icon: Filter },
          { value: 'beta', label: 'Beta' },
        ]}
      />
    );

    expect(screen.getByText('Mode')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Beta' })).toHaveClass('panel-item');

    const { container } = render(
      <SplitLayout data-testid="split-layout" className="layout-shell">
        <SplitSidebar data-testid="split-sidebar" className="sidebar-shell">
          Side
        </SplitSidebar>
        <SplitMain data-testid="split-main" className="main-shell">
          Main
        </SplitMain>
      </SplitLayout>
    );

    expect(screen.getByTestId('split-layout')).toHaveClass('layout-shell');
    expect(screen.getByTestId('split-sidebar').tagName).toBe('ASIDE');
    expect(screen.getByTestId('split-main')).toHaveClass('main-shell');
    expect(container).toHaveTextContent('Side');
    expect(container).toHaveTextContent('Main');
  });

  it('handles nav rail interactions and skips active or disabled entries', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(
      <NavRail
        value="overview"
        onChange={onChange}
        className="nav-rail"
        items={[
          { value: 'overview', label: 'Overview', icon: Filter },
          { value: 'details', label: 'Details', icon: TrendingUp },
          { value: 'locked', label: 'Locked', icon: Filter, disabled: true },
        ]}
      />
    );

    await user.click(screen.getByRole('button', { name: 'Overview' }));
    await user.click(screen.getByRole('button', { name: 'Details' }));
    await user.click(screen.getByRole('button', { name: 'Locked' }));

    expect(onChange).toHaveBeenCalledTimes(1);
    expect(onChange).toHaveBeenCalledWith('details');
    expect(screen.getByRole('button', { name: 'Locked' })).toBeDisabled();
  });
});
