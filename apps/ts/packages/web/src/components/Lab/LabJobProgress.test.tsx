import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { LabJobProgress } from './LabJobProgress';

describe('LabJobProgress', () => {
  it('keeps fast stage labeling when progress exceeds 50% without a verification message', () => {
    render(
      <LabJobProgress status="running" progress={0.75} message="Trial 15/20 完了" createdAt="2026-03-12T10:00:00Z" />
    );

    expect(screen.getByText('Fast stage')).toBeInTheDocument();
    expect(screen.queryByText('Verification stage')).not.toBeInTheDocument();
  });

  it('shows verification stage only when the worker reports it explicitly', () => {
    render(
      <LabJobProgress
        status="running"
        progress={0.75}
        message="Nautilus verification 1/5"
        createdAt="2026-03-12T10:00:00Z"
      />
    );

    expect(screen.getByText('Verification stage')).toBeInTheDocument();
  });
});
