import { expect, test, type Route } from '@playwright/test';

type MockPortfolio = {
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

function parsePortfolioId(pathname: string): number | null {
  const match = pathname.match(/^\/api\/portfolio\/(\d+)$/);
  return match ? Number.parseInt(match[1], 10) : null;
}

function parsePortfolioPerformanceId(pathname: string): number | null {
  const match = pathname.match(/^\/api\/portfolio\/(\d+)\/performance$/);
  return match ? Number.parseInt(match[1], 10) : null;
}

test.describe('portfolio CRUD smoke', () => {
  test.beforeEach(async ({ page }) => {
    const portfolios: MockPortfolio[] = [];
    let nextId = 1;

    await page.route('**/api/portfolio**', async (route) => {
      const request = route.request();
      const method = request.method();
      const url = new URL(request.url());
      const path = url.pathname;

      if (method === 'GET' && path === '/api/portfolio') {
        const summaries = portfolios.map((portfolio) => ({
          id: portfolio.id,
          name: portfolio.name,
          description: portfolio.description,
          stockCount: 0,
          totalShares: 0,
          createdAt: portfolio.createdAt,
          updatedAt: portfolio.updatedAt,
        }));
        await route.fulfill(jsonResponse({ portfolios: summaries }));
        return;
      }

      if (method === 'POST' && path === '/api/portfolio') {
        const payload = JSON.parse(request.postData() ?? '{}') as {
          name?: string;
          description?: string;
        };
        const now = new Date().toISOString();
        const created: MockPortfolio = {
          id: nextId,
          name: payload.name ?? `Portfolio ${nextId}`,
          description: payload.description,
          createdAt: now,
          updatedAt: now,
        };
        nextId += 1;
        portfolios.push(created);
        await route.fulfill(jsonResponse(created, 201));
        return;
      }

      const detailId = parsePortfolioId(path);
      if (detailId !== null) {
        const id = detailId;
        const portfolio = portfolios.find((candidate) => candidate.id === id);
        if (!portfolio) {
          await notFound(route);
          return;
        }
        if (method === 'GET') {
          await route.fulfill(jsonResponse({ ...portfolio, items: [] }));
          return;
        }
        if (method === 'DELETE') {
          const index = portfolios.findIndex((candidate) => candidate.id === id);
          if (index >= 0) {
            portfolios.splice(index, 1);
          }
          await route.fulfill(jsonResponse({ success: true, message: 'Portfolio deleted successfully' }));
          return;
        }
      }

      const performanceId = parsePortfolioPerformanceId(path);
      if (method === 'GET' && performanceId !== null) {
        const id = performanceId;
        const portfolio = portfolios.find((candidate) => candidate.id === id);
        if (!portfolio) {
          await notFound(route);
          return;
        }
        await route.fulfill(
          jsonResponse({
            portfolioId: id,
            portfolioName: portfolio.name,
            portfolioDescription: portfolio.description,
            summary: {
              totalCost: 0,
              currentValue: 0,
              totalPnL: 0,
              returnRate: 0,
            },
            holdings: [],
            timeSeries: [],
            benchmark: null,
            benchmarkTimeSeries: null,
            analysisDate: new Date().toISOString().slice(0, 10),
            dateRange: null,
            dataPoints: 0,
            warnings: [],
          })
        );
        return;
      }

      await route.continue();
    });
  });

  test('@smoke creates and deletes a portfolio from the portfolio page', async ({ page }) => {
    const portfolioName = `Smoke Portfolio ${Date.now()}`;

    await page.goto('/portfolio');
    await expect(page.getByRole('button', { name: 'New Portfolio' })).toBeVisible();

    await page.getByRole('button', { name: 'New Portfolio' }).click();
    await page.getByLabel('Name').fill(portfolioName);
    await page.getByLabel('Description (optional)').fill('Playwright smoke');
    await page.getByRole('button', { name: 'Create' }).click();

    await expect(page.getByRole('button', { name: `Select ${portfolioName} portfolio` })).toBeVisible();
    await expect(page.getByRole('heading', { level: 2, name: portfolioName })).toBeVisible();

    await page.getByRole('button', { name: 'Delete' }).click();
    const deleteDialog = page.getByRole('dialog', { name: 'Delete Portfolio' });
    await expect(deleteDialog).toBeVisible();
    await deleteDialog.getByRole('button', { name: 'Delete Portfolio' }).click();

    await expect(page.getByText('Select a portfolio to view details')).toBeVisible();
    await expect(page.getByRole('button', { name: `Select ${portfolioName} portfolio` })).toHaveCount(0);
  });
});
