import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { MainLayout } from './MainLayout';

vi.mock('./Header', () => ({
  Header: () => <div data-testid="mock-header">Header</div>,
}));

describe('MainLayout', () => {
  it('renders header and content inside the main region', () => {
    const { container } = render(
      <MainLayout>
        <div>Page Content</div>
      </MainLayout>
    );

    expect(screen.getByTestId('mock-header')).toBeInTheDocument();
    expect(screen.getByText('Page Content')).toBeInTheDocument();
    expect(container.querySelector('main')).toHaveClass('flex', 'min-h-0', 'flex-1', 'overflow-auto');
  });
});
