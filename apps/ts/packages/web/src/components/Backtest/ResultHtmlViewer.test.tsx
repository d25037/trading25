import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { ResultHtmlViewer } from './ResultHtmlViewer';

describe('ResultHtmlViewer', () => {
  it('renders loading state', () => {
    render(<ResultHtmlViewer htmlContent={null} isLoading={true} />);

    expect(screen.queryByText('No HTML report available')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Open in New Tab' })).not.toBeInTheDocument();
    expect(document.querySelector('.animate-spin')).toBeInTheDocument();
  });

  it('renders empty state when html content is missing', () => {
    render(<ResultHtmlViewer htmlContent={null} isLoading={false} />);

    expect(screen.getByText('No HTML report available')).toBeInTheDocument();
  });

  it('renders iframe and opens report in a new tab', () => {
    const createObjectUrlSpy = vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:test-url');
    const revokeObjectUrlSpy = vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => {});
    const openSpy = vi.spyOn(window, 'open').mockImplementation(() => null);
    const setTimeoutSpy = vi.spyOn(window, 'setTimeout').mockImplementation((handler) => {
      if (typeof handler === 'function') {
        handler();
      }
      return 0 as unknown as ReturnType<typeof setTimeout>;
    });

    render(<ResultHtmlViewer htmlContent="<html><body>report</body></html>" isLoading={false} />);

    expect(screen.getByText('Backtest Report')).toBeInTheDocument();
    expect(screen.getByTitle('Backtest Report')).toHaveAttribute('srcdoc', '<html><body>report</body></html>');

    fireEvent.click(screen.getByRole('button', { name: 'Open in New Tab' }));

    expect(createObjectUrlSpy).toHaveBeenCalledTimes(1);
    expect(openSpy).toHaveBeenCalledWith('blob:test-url', '_blank', 'noopener,noreferrer');
    expect(setTimeoutSpy).toHaveBeenCalledWith(expect.any(Function), 60000);
    expect(revokeObjectUrlSpy).toHaveBeenCalledWith('blob:test-url');
  });
});
