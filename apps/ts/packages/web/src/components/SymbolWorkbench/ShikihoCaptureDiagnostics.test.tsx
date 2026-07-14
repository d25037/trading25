import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ShikihoCaptureTraceV1 } from '@trading25/shikiho-extension/contract';
import { describe, expect, test } from 'vitest';
import { ShikihoCaptureDiagnostics } from './ShikihoCaptureDiagnostics';

function trace(overrides: Partial<ShikihoCaptureTraceV1> = {}): ShikihoCaptureTraceV1 {
  return {
    schemaVersion: 1,
    attemptId: 'attempt-1',
    code: '7203',
    mode: 'new_owned_tab',
    phase: 'observing_dom',
    startedAt: '2026-07-14T00:00:00.000Z',
    updatedAt: '2026-07-14T00:00:06.200Z',
    outcome: null,
    waitEndReason: null,
    receiverAttempts: 3,
    receiverReadyMs: 1_800,
    documentReadyState: 'interactive',
    navigation: {
      responseStartMs: 80,
      domInteractiveMs: 900,
      domContentLoadedMs: null,
      loadEndMs: 2_400,
    },
    dom: {
      firstSampleMs: 1_950,
      mutationBatches: 384,
      meaningfulChanges: 3,
      samples: 7,
      presentFields: ['identity', 'features', 'commentary'],
      missingFields: ['quote', 'consolidatedBusinesses', 'score'],
      firstSeenMs: {
        identity: 1_950,
        quote: null,
        features: 2_100,
        consolidatedBusinesses: null,
        commentary: 5_900,
        score: null,
        comparisonCompanies: null,
        industries: null,
        marketThemes: null,
        profile: null,
        editionLabel: null,
        pageUpdatedAt: null,
        coreReady: null,
      },
    },
    extraction: { samples: 7, lastMs: 4, maxMs: 12, totalMs: 49 },
    timings: {
      probeMs: 20,
      acquisitionMs: 35,
      receiverMs: 1_800,
      domObservationMs: 4_345,
      storageMs: 0,
      totalMs: 6_200,
    },
    ...overrides,
  };
}

describe('ShikihoCaptureDiagnostics', () => {
  test('shows the active phase and exposes compact diagnostics accessibly', async () => {
    render(<ShikihoCaptureDiagnostics trace={trace()} />);

    expect(screen.getByText('DOM確認 6.2秒')).toHaveAttribute('data-phase', 'observing_dom');
    const disclosure = screen.getByRole('button', { name: '取得診断' });
    expect(disclosure).toHaveAttribute('aria-expanded', 'false');
    expect(disclosure).toHaveAttribute('aria-controls');

    await userEvent.click(disclosure);

    expect(disclosure).toHaveAttribute('aria-expanded', 'true');
    const detailsId = disclosure.getAttribute('aria-controls');
    expect(detailsId).not.toBeNull();
    expect(document.getElementById(detailsId as string)).toBeInTheDocument();
    expect(screen.getByText('Receiver待ち')).toBeInTheDocument();
    expect(screen.getByText('1.8秒（3回）')).toBeInTheDocument();
    expect(screen.getByText('DOM更新 384 / 有効変化 3')).toBeInTheDocument();
    expect(screen.getByText('抽出 7回 / 合計 49ms / 最大 12ms')).toBeInTheDocument();
    expect(screen.getByText('responseStart')).toBeInTheDocument();
    expect(screen.getByText('80ms')).toBeInTheDocument();
    expect(screen.getByText('DOMContentLoaded')).toBeInTheDocument();
    expect(screen.getAllByText('—').length).toBeGreaterThan(0);
    expect(screen.getByText('特色')).toBeInTheDocument();
    expect(screen.getByText('2.1秒')).toBeInTheDocument();
  });

  test('shows the bounded terminal reason without fabricating missing timing values', async () => {
    render(
      <ShikihoCaptureDiagnostics
        trace={trace({
          phase: 'timeout',
          outcome: 'timeout',
          waitEndReason: 'deadline',
          receiverReadyMs: null,
          updatedAt: '2026-07-14T00:00:25.000Z',
          timings: {
            probeMs: 20,
            acquisitionMs: 35,
            receiverMs: 0,
            domObservationMs: 24_945,
            storageMs: 0,
            totalMs: 25_000,
          },
        })}
      />
    );

    expect(screen.getByText('タイムアウト 25.0秒')).toHaveAttribute('data-phase', 'timeout');
    await userEvent.click(screen.getByRole('button', { name: '取得診断' }));
    expect(screen.getByText('終了理由')).toBeInTheDocument();
    expect(screen.getByText('期限到達')).toBeInTheDocument();
    expect(screen.getByText('—（3回）')).toBeInTheDocument();
  });
});
