import { JQuantsClient } from '@trading25/shared';
import chalk from 'chalk';
import { CsvExporter } from '../../utils/csv-exporter';

export async function setupJQuantsClient(): Promise<JQuantsClient> {
  // Create client with environment variables (will be loaded automatically)
  // API key is loaded from JQUANTS_API_KEY env var
  const client = new JQuantsClient();
  return client;
}

export async function exportData<T>(
  data: T[],
  exportType: 'csv' | 'json',
  outputDir: string,
  filename: string,
  csvExporter: (data: T[], filename: string) => Promise<string>
): Promise<string> {
  const exporter = new CsvExporter(outputDir);
  const timestamp = new Date().toISOString().split('T')[0];

  if (exportType === 'csv') {
    const csvFilename = `${filename}_${timestamp}.csv`;
    const filepath = await csvExporter(data, csvFilename);
    console.log(chalk.green(`✅ CSV exported to: ${filepath}`));
    return filepath;
  }

  const jsonFilename = `${filename}_${timestamp}.json`;
  const filepath = await exporter.exportJSON(data, jsonFilename);
  console.log(chalk.green(`✅ JSON exported to: ${filepath}`));
  return filepath;
}

export function displayDataSummary<T>(data: T[], displayFunction: (data: T[]) => void, limit = 5): void {
  displayFunction(data.slice(0, limit));

  if (data.length > limit) {
    console.log(chalk.gray(`\n... and ${data.length - limit} more records`));
  }
}
