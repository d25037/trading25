import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { HistoryPage } from './HistoryPage';

describe('HistoryPage', () => {
  it('renders heading and placeholder text', () => {
    render(<HistoryPage />);

    expect(screen.getByText('Trading History')).toBeInTheDocument();
    expect(screen.getByText('Trading history and logs will be displayed here.')).toBeInTheDocument();
  });
});
