/**
 * Display Helpers
 * Common display patterns for CLI commands
 */

import chalk from 'chalk';

const SEPARATOR_WIDTH = 60;

/**
 * Display a section header with separator lines
 */
export function displayHeader(title: string): void {
  console.log(`\n${chalk.bold('='.repeat(SEPARATOR_WIDTH))}`);
  console.log(chalk.bold.cyan(title));
  console.log(chalk.bold('='.repeat(SEPARATOR_WIDTH)));
}

/**
 * Display a section footer
 */
export function displayFooter(): void {
  console.log(`${chalk.bold('='.repeat(SEPARATOR_WIDTH))}\n`);
}

/**
 * Display a section title with emoji
 */
export function displaySection(emoji: string, title: string): void {
  console.log(chalk.white(`\n${emoji} ${title}:`));
}

/**
 * Display a key-value pair with optional indentation
 */
export function displayKeyValue(key: string, value: string, indent = 2): void {
  const spaces = ' '.repeat(indent);
  console.log(chalk.white(`${spaces}${key}: ${value}`));
}

/**
 * Display a list of items with bullet points
 */
export function displayList(items: string[], options?: { color?: 'red' | 'yellow' | 'gray'; maxItems?: number }): void {
  const { color = 'gray', maxItems = 10 } = options ?? {};
  const colorFn = color === 'red' ? chalk.red : color === 'yellow' ? chalk.yellow : chalk.gray;

  for (const item of items.slice(0, maxItems)) {
    console.log(colorFn(`  • ${item}`));
  }

  if (items.length > maxItems) {
    console.log(colorFn(`  ... and ${items.length - maxItems} more`));
  }
}

/**
 * Display no results message with tips
 */
export function displayNoResults(message: string, tips: string[]): void {
  console.log(chalk.yellow(`\n❌ ${message}`));
  console.log(chalk.gray('\n💡 Try:'));
  for (const tip of tips) {
    console.log(chalk.gray(`   • ${tip}`));
  }
}

/**
 * Display success message
 */
export function displaySuccess(message: string): void {
  console.log(chalk.green(`✓ ${message}`));
}

/**
 * Display warning message
 */
export function displayWarning(message: string): void {
  console.log(chalk.yellow(`⚠ ${message}`));
}

/**
 * Display error message
 */
export function displayError(message: string): void {
  console.log(chalk.red(`✗ ${message}`));
}
