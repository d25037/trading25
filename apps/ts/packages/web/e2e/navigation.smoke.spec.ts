import { expect, test } from '@playwright/test';

test.describe('navigation smoke', () => {
  test('@smoke syncs route state with URL and browser history', async ({ page }) => {
    await page.goto('/history');

    await expect(page.getByRole('heading', { name: 'Trading History' })).toBeVisible();

    await page.getByRole('button', { name: 'Market DB' }).click();
    await expect(page).toHaveURL(/\/market-db$/);
    await expect(page.getByRole('heading', { name: 'Market DB' })).toBeVisible();

    await page.goBack();
    await expect(page).toHaveURL(/\/history$/);
    await expect(page.getByRole('heading', { name: 'Trading History' })).toBeVisible();
  });
});
