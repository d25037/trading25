import { expect, test } from '@playwright/test';

test.describe('backtest health smoke', () => {
  test('@smoke shows connected backtest server via /api proxy', async ({ page }) => {
    await page.goto('/?tab=backtest');

    await page.getByRole('button', { name: 'Status' }).click();
    await expect(page.getByRole('heading', { name: 'Backtest Server' })).toBeVisible();
    await expect(page.getByText('Connected')).toBeVisible();
    await expect(page.getByText('trading25-bt')).toBeVisible();
  });
});
