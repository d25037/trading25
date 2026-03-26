import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { ResultHtmlViewer } from './ResultHtmlViewer';

describe('ResultHtmlViewer', () => {
  it('renders loading state', () => {
    render(<ResultHtmlViewer htmlContent={null} isLoading={true} />);

    expect(screen.queryByText('No HTML report available')).not.toBeInTheDocument();
    expect(document.querySelector('.animate-spin')).toBeInTheDocument();
  });

  it('renders empty state when html content is missing', () => {
    render(<ResultHtmlViewer htmlContent={null} isLoading={false} />);

    expect(screen.getByText('No HTML report available')).toBeInTheDocument();
  });

  it('renders iframe when html content is available', () => {
    render(<ResultHtmlViewer htmlContent="<html><body>report</body></html>" isLoading={false} />);

    expect(screen.getByTitle('Backtest Report')).toHaveAttribute('srcdoc', '<html><body>report</body></html>');
  });
});
