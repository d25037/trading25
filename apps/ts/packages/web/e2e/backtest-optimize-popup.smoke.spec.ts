import { expect, test } from '@playwright/test';

const STRATEGY_NAME = 'production/smoke_strategy';
const STRATEGY_DISPLAY_NAME = 'Smoke Strategy';

function jsonResponse(body: unknown, status = 200) {
  return {
    status,
    contentType: 'application/json',
    body: JSON.stringify(body),
  };
}

test.describe('backtest optimize popup smoke', () => {
  test.beforeEach(async ({ page }) => {
    await page.route('**/api/strategies**', async (route) => {
      const request = route.request();
      const url = new URL(request.url());
      const path = url.pathname;

      if (request.method() !== 'GET') {
        await route.continue();
        return;
      }

      if (path === '/api/strategies') {
        await route.fulfill(
          jsonResponse({
            strategies: [
              {
                name: STRATEGY_NAME,
                category: 'production',
                display_name: STRATEGY_DISPLAY_NAME,
                description: 'Smoke strategy for optimize popup flow',
                last_modified: '2026-02-19T00:00:00Z',
              },
            ],
            total: 1,
          })
        );
        return;
      }

      if (path.startsWith('/api/strategies/')) {
        const encodedRest = path.slice('/api/strategies/'.length);
        const strategyName = decodeURIComponent(encodedRest);
        if (strategyName === STRATEGY_NAME) {
          await route.fulfill(
            jsonResponse({
              name: STRATEGY_NAME,
              category: 'production',
              display_name: STRATEGY_DISPLAY_NAME,
              description: 'Smoke strategy for optimize popup flow',
              config: {
                entry_filter_params: {
                  period_extrema_break: {
                    period: 20,
                  },
                },
              },
              execution_info: {},
            })
          );
          return;
        }

        if (path === `/api/strategies/${encodeURIComponent(STRATEGY_NAME)}/optimization`) {
          await route.fulfill(
            jsonResponse({
              strategy_name: STRATEGY_NAME,
              persisted: true,
              source: 'saved',
              optimization: {
                description: 'Smoke strategy optimization',
                parameter_ranges: {
                  entry_filter_params: {
                    period_extrema_break: {
                      period: [10, 20],
                    },
                  },
                  exit_trigger_params: {
                    atr_stop: {
                      atr_multiplier: [1.5, 2.0],
                    },
                  },
                },
              },
              yaml_content: `description: Smoke strategy optimization
parameter_ranges:
  entry_filter_params:
    period_extrema_break:
      period: [10, 20]
  exit_trigger_params:
    atr_stop:
      atr_multiplier: [1.5, 2.0]
`,
              valid: true,
              ready_to_run: true,
              param_count: 2,
              combinations: 4,
              errors: [],
              warnings: [],
              drift: [],
            })
          );
          return;
        }
      }

      await route.continue();
    });

    await page.route('**/api/signals/reference', async (route) => {
      const request = route.request();
      const url = new URL(request.url());
      const path = url.pathname;

      if (request.method() === 'GET' && path === '/api/signals/reference') {
        await route.fulfill(
          jsonResponse({
            categories: [
              {
                key: 'breakout',
                label: 'Breakout',
              },
            ],
            signals: [
              {
                key: 'period_extrema_break',
                name: 'Period Extrema Break',
                category: 'breakout',
                description: 'Breakout signal for smoke flow',
                usage_hint: 'Use for trend following entries.',
                fields: [
                  {
                    name: 'period',
                    type: 'number',
                    description: 'Lookback period',
                    default: 20,
                    options: null,
                    constraints: {
                      ge: 1,
                    },
                  },
                ],
                yaml_snippet: `entry_filter_params:
  period_extrema_break:
    period: 20`,
                exit_disabled: false,
                data_requirements: ['stock_data'],
              },
            ],
            total: 1,
          })
        );
        return;
      }

      await route.continue();
    });
  });

  test('@smoke opens optimization popup editor from strategies tab', async ({ page }) => {
    await page.goto('/backtest');

    await page.getByRole('button', { name: 'Strategies' }).click();
    await expect(page.getByRole('heading', { name: STRATEGY_DISPLAY_NAME })).toBeVisible();

    await page.getByRole('heading', { name: STRATEGY_DISPLAY_NAME }).click();
    await page.getByRole('button', { name: 'Optimize', exact: true }).click();

    await expect(page.getByRole('heading', { name: 'Optimization Spec' })).toBeVisible();
    await expect(page.getByText('Current')).toBeVisible();
    await expect(page.getByText('Saved')).toBeVisible();
    await expect(page.getByText('State')).toBeVisible();

    await page.getByRole('button', { name: 'Open Editor' }).click();

    const dialog = page.getByRole('dialog');
    await expect(dialog).toBeVisible();
    await expect(dialog.getByText(`Optimization Spec Editor: ${STRATEGY_NAME}`)).toBeVisible();
    await expect(dialog.getByRole('heading', { name: 'Signal Reference' })).toBeVisible();
    await expect(dialog.getByRole('button', { name: 'Save' })).toBeVisible();
    await expect(dialog.getByRole('button', { name: 'Close' }).first()).toBeVisible();
  });
});
