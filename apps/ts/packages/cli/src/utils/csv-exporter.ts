import { existsSync, mkdirSync } from 'node:fs';
import * as path from 'node:path';
import type {
  JQuantsDailyQuote,
  JQuantsListedInfo,
  JQuantsTOPIX,
  JQuantsWeeklyMarginInterest,
} from '@trading25/shared';

/**
 * Input types that accept both full JQuants types and partial API responses
 */
type DailyQuoteInput = JQuantsDailyQuote | Partial<JQuantsDailyQuote>;
type ListedInfoInput = JQuantsListedInfo | Partial<JQuantsListedInfo>;
type MarginInterestInput = JQuantsWeeklyMarginInterest | Partial<JQuantsWeeklyMarginInterest>;

export class CsvExporter {
  private outputDir: string;

  constructor(outputDir = './data') {
    this.outputDir = outputDir;
    this.ensureOutputDir();
  }

  private ensureOutputDir(): void {
    if (!existsSync(this.outputDir)) {
      mkdirSync(this.outputDir, { recursive: true });
    }
  }

  private escapeCSV(value: unknown): string {
    if (value === null || value === undefined) return '';
    const str = String(value);
    if (str.includes(',') || str.includes('"') || str.includes('\n')) {
      return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
  }

  private getOptionalField(quote: DailyQuoteInput, field: string): string | number {
    if (field in quote) {
      const value = (quote as Record<string, unknown>)[field];
      if (typeof value === 'string' || typeof value === 'number') {
        return value;
      }
    }
    return '';
  }

  private mapQuoteToRow(quote: DailyQuoteInput): (string | number | null | undefined)[] {
    return [
      quote.Date ?? '',
      quote.Code ?? '',
      quote.O ?? '',
      quote.H ?? '',
      quote.L ?? '',
      quote.C ?? '',
      quote.Vo ?? '',
      this.getOptionalField(quote, 'Va'),
      this.getOptionalField(quote, 'AdjFactor'),
      this.getOptionalField(quote, 'AdjO'),
      this.getOptionalField(quote, 'AdjH'),
      this.getOptionalField(quote, 'AdjL'),
      this.getOptionalField(quote, 'AdjC'),
      this.getOptionalField(quote, 'AdjVo'),
    ];
  }

  async exportDailyQuotes(quotes: DailyQuoteInput[], filename: string): Promise<string> {
    const headers = [
      'Date',
      'Code',
      'Open',
      'High',
      'Low',
      'Close',
      'Volume',
      'TurnoverValue',
      'AdjustmentFactor',
      'AdjustmentOpen',
      'AdjustmentHigh',
      'AdjustmentLow',
      'AdjustmentClose',
      'AdjustmentVolume',
    ];

    const rows = quotes.map((quote) => this.mapQuoteToRow(quote));

    return this.writeCSV(headers, rows, filename);
  }

  async exportListedInfo(stocks: ListedInfoInput[], filename: string): Promise<string> {
    const headers = [
      'Date',
      'Code',
      'CompanyName',
      'CompanyNameEnglish',
      'MarketCode',
      'MarketCodeName',
      'Sector33Code',
      'Sector33CodeName',
      'Sector17Code',
      'Sector17CodeName',
      'ScaleCategory',
    ];

    const rows = stocks.map((stock) => [
      stock.Date ?? '',
      stock.Code ?? '',
      stock.CoName ?? '',
      stock.CoNameEn ?? '',
      stock.Mkt ?? '',
      stock.MktNm ?? '',
      stock.S33 ?? '',
      stock.S33Nm ?? '',
      stock.S17 ?? '',
      stock.S17Nm ?? '',
      stock.ScaleCat ?? '',
    ]);

    return this.writeCSV(headers, rows, filename);
  }

  async exportWeeklyMarginInterest(data: MarginInterestInput[], filename: string): Promise<string> {
    const headers = [
      'Date',
      'Code',
      'ShortMarginTradeVolume',
      'LongMarginTradeVolume',
      'ShortNegotiableMarginTradeVolume',
      'ShortStandardizedMarginTradeVolume',
      'LongNegotiableMarginTradeVolume',
      'LongStandardizedMarginTradeVolume',
      'IssueType',
    ];

    const rows = data.map((item) => [
      item.Date ?? '',
      item.Code ?? '',
      item.ShrtVol ?? '',
      item.LongVol ?? '',
      item.ShrtNegVol ?? '',
      item.ShrtStdVol ?? '',
      item.LongNegVol ?? '',
      item.LongStdVol ?? '',
      item.IssType ?? '',
    ]);

    return this.writeCSV(headers, rows, filename);
  }

  async exportTOPIX(topixData: JQuantsTOPIX[], filename: string): Promise<string> {
    const headers = ['Date', 'Open', 'High', 'Low', 'Close'];

    const rows = topixData.map((data) => [data.Date, data.O, data.H, data.L, data.C]);

    return this.writeCSV(headers, rows, filename);
  }

  private async writeCSV(headers: string[], rows: unknown[][], filename: string): Promise<string> {
    const csvContent = [
      headers.map((h) => this.escapeCSV(h)).join(','),
      ...rows.map((row) => row.map((cell) => this.escapeCSV(cell)).join(',')),
    ].join('\n');

    const filepath = path.join(this.outputDir, filename);
    await Bun.write(filepath, csvContent);
    return filepath;
  }

  async exportJSON(data: unknown, filename: string): Promise<string> {
    const filepath = path.join(this.outputDir, filename);
    await Bun.write(filepath, JSON.stringify(data, null, 2));
    return filepath;
  }
}
