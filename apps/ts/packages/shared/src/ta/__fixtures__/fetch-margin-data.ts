/**
 * Script to fetch Toyota (7203) margin interest test data for TA unit tests
 * This can be run standalone or via CLI command
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import type { JQuantsWeeklyMarginInterest } from '../../types/jquants';

interface BtMarginInterestItem {
  date: string;
  code: string;
  shortMarginTradeVolume: number;
  longMarginTradeVolume: number;
  shortMarginOutstandingBalance: number | null;
  longMarginOutstandingBalance: number | null;
}

export async function fetchToyotaMarginData(days = 365): Promise<void> {
  // Calculate date range
  const to = new Date();
  const from = new Date();
  from.setDate(from.getDate() - days);
  const fromDate = from.toISOString().split('T')[0] ?? '';
  const toDate = to.toISOString().split('T')[0] ?? '';
  const apiBaseUrl = process.env.API_BASE_URL || 'http://localhost:3002';

  console.log(`Fetching Toyota (7203) margin data from ${fromDate} to ${toDate} via ${apiBaseUrl}`);

  try {
    const query = new URLSearchParams({
      from: fromDate,
      to: toDate,
    });
    const response = await fetch(`${apiBaseUrl}/api/jquants/stocks/7203/margin-interest?${query.toString()}`);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${await response.text()}`);
    }

    const payload = (await response.json()) as { marginInterest: BtMarginInterestItem[] };
    const normalized = payload.marginInterest.map(
      (item): JQuantsWeeklyMarginInterest => ({
        Date: item.date,
        Code: item.code,
        ShrtVol: item.shortMarginOutstandingBalance ?? item.shortMarginTradeVolume,
        LongVol: item.longMarginOutstandingBalance ?? item.longMarginTradeVolume,
        ShrtNegVol: 0,
        LongNegVol: 0,
        ShrtStdVol: item.shortMarginTradeVolume,
        LongStdVol: item.longMarginTradeVolume,
        IssType: '',
      })
    );

    if (normalized.length > 0) {
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

      const csvRows = normalized.map((margin: JQuantsWeeklyMarginInterest) => [
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

      console.log(`✅ Saved ${normalized.length} margin records to ${filePath}`);

      // Also save as JSON for easier testing
      const jsonPath = path.join(fixturesDir, 'toyota_7203_margin.json');
      fs.writeFileSync(jsonPath, JSON.stringify(normalized, null, 2), 'utf-8');

      console.log(`✅ Also saved as JSON to ${jsonPath}`);

      // Display summary
      const firstMargin = normalized[0];
      const lastMargin = normalized[normalized.length - 1];

      console.log('\n📊 Margin Data Summary:');
      console.log(`Stock: Toyota Motor Corporation (7203)`);
      console.log(`Period: ${firstMargin?.Date} to ${lastMargin?.Date}`);
      console.log(`Records: ${normalized.length}`);
      console.log(`Latest Short Volume: ${lastMargin?.ShrtVol?.toLocaleString()}`);
      console.log(`Latest Long Volume: ${lastMargin?.LongVol?.toLocaleString()}`);
      console.log(
        `Issue Type: ${lastMargin?.IssType === '1' ? 'Credit Issue' : lastMargin?.IssType === '2' ? 'Lending/Borrowing Issue' : 'Other'}`
      );
    } else {
      console.error('No margin data received from bt JQuants proxy API');
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
