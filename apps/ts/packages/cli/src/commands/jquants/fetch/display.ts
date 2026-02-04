import type {
  JQuantsDailyQuote,
  JQuantsIndex,
  JQuantsListedInfo,
  JQuantsTOPIX,
  JQuantsWeeklyMarginInterest,
} from '@trading25/shared';
import chalk from 'chalk';

/**
 * Simplified types compatible with API responses
 */
type SimplifiedDailyQuote = Pick<JQuantsDailyQuote, 'Date' | 'Code' | 'O' | 'H' | 'L' | 'C'> & {
  Vo?: number | null;
};

type SimplifiedListedInfo = Pick<JQuantsListedInfo, 'Code' | 'CoName'> & {
  CoNameEn?: string;
  MktNm?: string;
  S33Nm?: string;
};

type SimplifiedMarginInterest = Pick<JQuantsWeeklyMarginInterest, 'Date' | 'Code'> & {
  ShrtVol: number;
  LongVol: number;
};

export function displayDailyQuotes(quotes: (JQuantsDailyQuote | SimplifiedDailyQuote)[]): void {
  console.log(chalk.cyan('\n📊 Daily Quotes'));
  console.log(chalk.white('━'.repeat(60)));

  for (const quote of quotes) {
    console.log(chalk.yellow(`\n${quote.Date} (${quote.Code})`));
    console.log(`  Open:  ¥${quote.O?.toLocaleString() || 'N/A'}`);
    console.log(`  High:  ¥${quote.H?.toLocaleString() || 'N/A'}`);
    console.log(`  Low:   ¥${quote.L?.toLocaleString() || 'N/A'}`);
    console.log(`  Close: ¥${quote.C?.toLocaleString() || 'N/A'}`);
    console.log(`  Volume: ${quote.Vo?.toLocaleString() || 'N/A'}`);
  }
}

export function displayListedInfo(stocks: (JQuantsListedInfo | SimplifiedListedInfo)[]): void {
  console.log(chalk.cyan('\n📈 Listed Stocks'));
  console.log(chalk.white('━'.repeat(60)));

  for (const stock of stocks) {
    console.log(chalk.yellow(`\n${stock.Code}: ${stock.CoName}`));
    console.log(`  English: ${stock.CoNameEn ?? 'N/A'}`);
    console.log(`  Market: ${stock.MktNm ?? 'N/A'}`);
    console.log(`  Sector: ${stock.S33Nm ?? 'N/A'}`);
  }
}

export function displayMarginInterest(records: (JQuantsWeeklyMarginInterest | SimplifiedMarginInterest)[]): void {
  console.log(chalk.cyan('\n💰 Margin Interest'));
  console.log(chalk.white('━'.repeat(60)));

  for (const record of records) {
    console.log(chalk.yellow(`\n${record.Date} (${record.Code})`));
    console.log(`  Short Margin: ${record.ShrtVol.toLocaleString()}`);
    console.log(`  Long Margin:  ${record.LongVol.toLocaleString()}`);
  }
}

export function displayIndices(indices: JQuantsIndex[]): void {
  console.log(chalk.cyan('\n📈 Index Data'));
  console.log(chalk.white('━'.repeat(60)));

  for (const index of indices) {
    console.log(chalk.yellow(`\n${index.Date}`));
    console.log(`  Open:  ${index.O}`);
    console.log(`  High:  ${index.H}`);
    console.log(`  Low:   ${index.L}`);
    console.log(`  Close: ${index.C}`);
  }
}

export function displayTOPIX(topixData: JQuantsTOPIX[]): void {
  console.log(chalk.cyan('\n📈 TOPIX Index'));
  console.log(chalk.white('━'.repeat(60)));

  for (const data of topixData) {
    console.log(chalk.yellow(`\n${data.Date}`));
    console.log(`  Open:  ${data.O.toLocaleString()}`);
    console.log(`  High:  ${data.H.toLocaleString()}`);
    console.log(`  Low:   ${data.L.toLocaleString()}`);
    console.log(`  Close: ${data.C.toLocaleString()}`);
  }
}
