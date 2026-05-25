import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { JobStatusIcon } from './JobStatusIcon';

describe('JobStatusIcon', () => {
  it('renders active statuses with a spinner class', () => {
    const { container } = render(<JobStatusIcon status="running" />);

    const icon = container.querySelector('svg');
    expect(icon).not.toBeNull();
    expect(icon?.getAttribute('class')).toContain('animate-spin');
  });

  it('renders terminal statuses without a spinner class', () => {
    const { container } = render(<JobStatusIcon status="completed" />);

    const icon = container.querySelector('svg');
    expect(icon).not.toBeNull();
    expect(icon?.getAttribute('class')).toContain('text-green-500');
    expect(icon?.getAttribute('class')).not.toContain('animate-spin');
  });

  it('can hide or show unknown statuses', () => {
    const hidden = render(<JobStatusIcon status="unknown" />);
    expect(hidden.container.querySelector('svg')).toBeNull();
    hidden.unmount();

    const shown = render(<JobStatusIcon status="unknown" showUnknown />);
    expect(shown.container.querySelector('svg')?.getAttribute('class')).toContain('text-yellow-500');
  });
});
