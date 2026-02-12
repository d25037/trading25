import { expect, test } from '@playwright/test';

test.describe('navigation smoke', () => {
  test('@smoke syncs tab state with URL and browser history', async ({ page }) => {
    await page.goto('/?tab=history');

    await expect(page.getByRole('heading', { name: 'Trading History' })).toBeVisible();

    await page.getByRole('button', { name: 'Settings' }).click();
    await expect(page).toHaveURL(/tab=settings/);
    await expect(page.getByRole('heading', { name: 'Settings' })).toBeVisible();

    await page.goBack();
    await expect(page).toHaveURL(/tab=history/);
    await expect(page.getByRole('heading', { name: 'Trading History' })).toBeVisible();
  });
});
