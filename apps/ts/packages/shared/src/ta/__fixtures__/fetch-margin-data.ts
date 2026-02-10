/**
 * Script to fetch Toyota (7203) margin interest test data for TA unit tests
 * This can be run standalone or via CLI command
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { JQuantsClient } from '@trading25/clients-ts/JQuantsClient';
import type { JQuantsWeeklyMarginInterest } from '../../types/jquants';

export async function fetchToyotaMarginData(days = 365): Promise<void> {
  const client = new JQuantsClient({
    apiKey: process.env.JQUANTS_API_KEY,
  });

  // Calculate date range
  const to = new Date();
  const from = new Date();
  from.setDate(from.getDate() - days);

  console.log(
    `Fetching Toyota (7203) margin data from ${from.toISOString().split('T')[0]} to ${to.toISOString().split('T')[0]}`
  );

  try {
    const response = await client.getWeeklyMarginInterest({
      code: '7203',
      from: from.toISOString().split('T')[0],
      to: to.toISOString().split('T')[0],
    });

    if (response.data && response.data.length > 0) {
      // Convert to CSV format (using v2 field names)
      const headers = [
        'Date',
        'Code',
        'ShrtVol',
        'LongVol',
        'ShrtNegVol',
        'LongNegVol',
        'ShrtStdVol',
        'LongStdVol',
        'IssType',
      ];

      const csvRows = response.data.map((margin: JQuantsWeeklyMarginInterest) => [
        margin.Date,
        margin.Code,
        margin.ShrtVol,
        margin.LongVol,
        margin.ShrtNegVol,
        margin.LongNegVol,
        margin.ShrtStdVol,
        margin.LongStdVol,
        margin.IssType,
      ]);

      const csvContent = [headers.join(','), ...csvRows.map((row) => row.join(','))].join('\n');

      // Save to fixtures directory
      const fixturesDir = path.join(__dirname);
      const filePath = path.join(fixturesDir, 'toyota_7203_margin.csv');

      fs.writeFileSync(filePath, csvContent, 'utf-8');

      console.log(`✅ Saved ${response.data.length} margin records to ${filePath}`);

      // Also save as JSON for easier testing
      const jsonPath = path.join(fixturesDir, 'toyota_7203_margin.json');
      fs.writeFileSync(jsonPath, JSON.stringify(response.data, null, 2), 'utf-8');

      console.log(`✅ Also saved as JSON to ${jsonPath}`);

      // Display summary
      const firstMargin = response.data[0];
      const lastMargin = response.data[response.data.length - 1];

      console.log('\n📊 Margin Data Summary:');
      console.log(`Stock: Toyota Motor Corporation (7203)`);
      console.log(`Period: ${firstMargin?.Date} to ${lastMargin?.Date}`);
      console.log(`Records: ${response.data.length}`);
      console.log(`Latest Short Volume: ${lastMargin?.ShrtVol?.toLocaleString()}`);
      console.log(`Latest Long Volume: ${lastMargin?.LongVol?.toLocaleString()}`);
      console.log(
        `Issue Type: ${lastMargin?.IssType === '1' ? 'Credit Issue' : lastMargin?.IssType === '2' ? 'Lending/Borrowing Issue' : 'Other'}`
      );
    } else {
      console.error('No margin data received from JQuants API');
    }
  } catch (error) {
    console.error('Error fetching margin data:', error);
    throw error;
  }
}

// Run if executed directly
import { fileURLToPath } from 'node:url';

if (import.meta.url === `file://${process.argv[1]}` || fileURLToPath(import.meta.url) === process.argv[1]) {
  const days = process.argv[2] ? Number.parseInt(process.argv[2], 10) : 365;
  fetchToyotaMarginData(days).catch((error) => {
    console.error('Failed to fetch margin test data:', error);
    process.exit(1);
  });
}
