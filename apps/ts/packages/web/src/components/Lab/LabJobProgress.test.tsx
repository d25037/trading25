import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { LabJobProgress } from './LabJobProgress';

describe('LabJobProgress', () => {
  it('shows raw worker progress without an inferred stage label', () => {
    render(
      <LabJobProgress status="running" progress={0.75} message="Trial 15/20 完了" createdAt="2026-03-12T10:00:00Z" />
    );

    expect(screen.getByText('Trial 15/20 完了')).toBeInTheDocument();
    expect(screen.getByText('75%')).toBeInTheDocument();
    expect(screen.queryByText('Fast stage')).not.toBeInTheDocument();
    expect(screen.queryByText('Verification stage')).not.toBeInTheDocument();
  });

  it('keeps an arbitrary worker message raw', () => {
    render(
      <LabJobProgress
        status="running"
        progress={0.75}
        message="Worker phase 1/5"
        createdAt="2026-03-12T10:00:00Z"
      />
    );

    expect(screen.getByText('Worker phase 1/5')).toBeInTheDocument();
    expect(screen.queryByText('Fast stage')).not.toBeInTheDocument();
    expect(screen.queryByText('Verification stage')).not.toBeInTheDocument();
  });
});
