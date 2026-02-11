/**
 * Script to fetch Toyota (7203) test data for TA unit tests
 * This can be run standalone or via CLI command
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import type { JQuantsDailyQuote } from '../../types/jquants';

export async function fetchToyotaTestData(days = 365): Promise<void> {
  // Calculate date range
  const to = new Date();
  const from = new Date();
  from.setDate(from.getDate() - days);
  const fromDate = from.toISOString().split('T')[0] ?? '';
  const toDate = to.toISOString().split('T')[0] ?? '';
  const apiBaseUrl = process.env.API_BASE_URL || 'http://localhost:3002';

  console.log(`Fetching Toyota (7203) data from ${fromDate} to ${toDate} via ${apiBaseUrl}`);

  try {
    const query = new URLSearchParams({
      code: '7203',
      from: fromDate,
      to: toDate,
    });
    const response = await fetch(`${apiBaseUrl}/api/jquants/daily-quotes?${query.toString()}`);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${await response.text()}`);
    }

    const payload = (await response.json()) as { data: JQuantsDailyQuote[] };

    if (payload.data && payload.data.length > 0) {
      // Convert to CSV format (using v2 field names)
      const headers = [
        'Date',
        'Code',
        'O',
        'H',
        'L',
        'C',
        'Vo',
        'Va',
        'AdjFactor',
        'AdjO',
        'AdjH',
        'AdjL',
        'AdjC',
        'AdjVo',
      ];

      const csvRows = payload.data.map((quote: JQuantsDailyQuote) => [
        quote.Date,
        quote.Code,
        quote.O ?? '',
        quote.H ?? '',
        quote.L ?? '',
        quote.C ?? '',
        quote.Vo ?? '',
        quote.Va ?? '',
        quote.AdjFactor,
        quote.AdjO ?? '',
        quote.AdjH ?? '',
        quote.AdjL ?? '',
        quote.AdjC ?? '',
        quote.AdjVo ?? '',
      ]);

      const csvContent = [headers.join(','), ...csvRows.map((row) => row.join(','))].join('\n');

      // Save to fixtures directory
      const fixturesDir = path.join(__dirname);
      const filePath = path.join(fixturesDir, 'toyota_7203_daily.csv');

      fs.writeFileSync(filePath, csvContent, 'utf-8');

      console.log(`✅ Saved ${payload.data.length} records to ${filePath}`);

      // Also save as JSON for easier testing
      const jsonPath = path.join(fixturesDir, 'toyota_7203_daily.json');
      fs.writeFileSync(jsonPath, JSON.stringify(payload.data, null, 2), 'utf-8');

      console.log(`✅ Also saved as JSON to ${jsonPath}`);

      // Display summary
      const firstQuote = payload.data[0];
      const lastQuote = payload.data[payload.data.length - 1];

      console.log('\n📊 Data Summary:');
      console.log(`Stock: Toyota Motor Corporation (7203)`);
      console.log(`Period: ${firstQuote?.Date} to ${lastQuote?.Date}`);
      console.log(`Records: ${payload.data.length}`);
      console.log(`First Close: ¥${firstQuote?.C?.toLocaleString()}`);
      console.log(`Last Close: ¥${lastQuote?.C?.toLocaleString()}`);
    } else {
      console.error('No data received from bt JQuants proxy API');
    }
  } catch (error) {
    console.error('Error fetching data:', error);
    throw error;
  }
}

// Run if executed directly
import { fileURLToPath } from 'node:url';

if (import.meta.url === `file://${process.argv[1]}` || fileURLToPath(import.meta.url) === process.argv[1]) {
  const days = process.argv[2] ? Number.parseInt(process.argv[2], 10) : 365;
  fetchToyotaTestData(days).catch((error) => {
    console.error('Failed to fetch test data:', error);
    process.exit(1);
  });
}
