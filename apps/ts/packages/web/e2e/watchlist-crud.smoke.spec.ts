import { expect, test, type Route } from '@playwright/test';

type MockWatchlist = {
  id: number;
  name: string;
  description?: string;
  createdAt: string;
  updatedAt: string;
};

function jsonResponse(body: unknown, status = 200) {
  return {
    status,
    contentType: 'application/json',
    body: JSON.stringify(body),
  };
}

function notFound(route: Route) {
  return route.fulfill(jsonResponse({ message: 'Not Found' }, 404));
}

function parseWatchlistId(pathname: string): number | null {
  const match = pathname.match(/^\/api\/watchlist\/(\d+)$/);
  return match ? Number.parseInt(match[1], 10) : null;
}

test.describe('watchlist CRUD smoke', () => {
  test.beforeEach(async ({ page }) => {
    const watchlists: MockWatchlist[] = [];
    let nextId = 1;

    await page.route('**/api/watchlist**', async (route) => {
      const request = route.request();
      const method = request.method();
      const url = new URL(request.url());
      const path = url.pathname;

      if (method === 'GET' && path === '/api/watchlist') {
        const summaries = watchlists.map((watchlist) => ({
          id: watchlist.id,
          name: watchlist.name,
          description: watchlist.description,
          stockCount: 0,
          createdAt: watchlist.createdAt,
          updatedAt: watchlist.updatedAt,
        }));
        await route.fulfill(jsonResponse({ watchlists: summaries }));
        return;
      }

      if (method === 'POST' && path === '/api/watchlist') {
        const payload = JSON.parse(request.postData() ?? '{}') as {
          name?: string;
          description?: string;
        };
        const now = new Date().toISOString();
        const created: MockWatchlist = {
          id: nextId,
          name: payload.name ?? `Watchlist ${nextId}`,
          description: payload.description,
          createdAt: now,
          updatedAt: now,
        };
        nextId += 1;
        watchlists.push(created);
        await route.fulfill(jsonResponse({ ...created, stockCount: 0 }, 201));
        return;
      }

      const detailId = parseWatchlistId(path);
      if (detailId !== null) {
        const id = detailId;
        const watchlist = watchlists.find((candidate) => candidate.id === id);
        if (!watchlist) {
          await notFound(route);
          return;
        }
        if (method === 'GET') {
          await route.fulfill(jsonResponse({ ...watchlist, items: [] }));
          return;
        }
        if (method === 'DELETE') {
          const index = watchlists.findIndex((candidate) => candidate.id === id);
          if (index >= 0) {
            watchlists.splice(index, 1);
          }
          await route.fulfill(jsonResponse({ success: true, message: 'Watchlist deleted successfully' }));
          return;
        }
      }

      await route.continue();
    });
  });

  test('@smoke creates and deletes a watchlist from the watchlist page', async ({ page }) => {
    const watchlistName = `Smoke Watchlist ${Date.now()}`;

    await page.goto('/watchlist');
    await expect(page.getByRole('button', { name: 'New Watchlist' })).toBeVisible();

    await page.getByRole('button', { name: 'New Watchlist' }).click();
    await page.getByLabel('Name').fill(watchlistName);
    await page.getByLabel('Description (optional)').fill('Playwright smoke');
    await page.getByRole('button', { name: 'Create' }).click();

    await expect(page.getByRole('combobox', { name: 'Watchlist' })).toContainText(watchlistName);

    await page.getByRole('button', { name: 'Manage Watchlist' }).click();
    const manageDialog = page.getByRole('dialog', { name: 'Manage Watchlist' });
    await expect(manageDialog).toBeVisible();
    await manageDialog.getByRole('button', { name: 'Delete Watchlist' }).click();
    await manageDialog.getByRole('button', { name: 'Confirm Delete' }).click();

    await expect(page.getByText('Select a watchlist to view details')).toBeVisible();
    await expect(page.getByRole('combobox', { name: 'Watchlist' })).toContainText('No watchlist');
  });
});
