/**
 * Script to fetch Toyota (7203) test data for TA unit tests
 * This can be run standalone or via CLI command
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { JQuantsClient } from '../../clients/JQuantsClient';
import type { JQuantsDailyQuote } from '../../types/jquants';

export async function fetchToyotaTestData(days = 365): Promise<void> {
  const client = new JQuantsClient({
    apiKey: process.env.JQUANTS_API_KEY,
  });

  // Calculate date range
  const to = new Date();
  const from = new Date();
  from.setDate(from.getDate() - days);

  console.log(
    `Fetching Toyota (7203) data from ${from.toISOString().split('T')[0]} to ${to.toISOString().split('T')[0]}`
  );

  try {
    const response = await client.getDailyQuotes({
      code: '7203',
      from: from.toISOString().split('T')[0],
      to: to.toISOString().split('T')[0],
    });

    if (response.data && response.data.length > 0) {
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

      const csvRows = response.data.map((quote: JQuantsDailyQuote) => [
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

      console.log(`✅ Saved ${response.data.length} records to ${filePath}`);

      // Also save as JSON for easier testing
      const jsonPath = path.join(fixturesDir, 'toyota_7203_daily.json');
      fs.writeFileSync(jsonPath, JSON.stringify(response.data, null, 2), 'utf-8');

      console.log(`✅ Also saved as JSON to ${jsonPath}`);

      // Display summary
      const firstQuote = response.data[0];
      const lastQuote = response.data[response.data.length - 1];

      console.log('\n📊 Data Summary:');
      console.log(`Stock: Toyota Motor Corporation (7203)`);
      console.log(`Period: ${firstQuote?.Date} to ${lastQuote?.Date}`);
      console.log(`Records: ${response.data.length}`);
      console.log(`First Close: ¥${firstQuote?.C?.toLocaleString()}`);
      console.log(`Last Close: ¥${lastQuote?.C?.toLocaleString()}`);
    } else {
      console.error('No data received from JQuants API');
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
