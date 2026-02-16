/**
 * JQuants Auth - Deprecated Alias Command
 * Maintains backward compatibility for v1 command name.
 */

import { getProjectEnvPath } from '@trading25/shared/utils/find-project-root';
import chalk from 'chalk';
import { define } from 'gunshi';
import { CLITokenManager } from '../../../utils/cli-token-manager.js';

import { CLI_NAME } from '../../../utils/constants.js';
export async function statusAction(): Promise<void> {
  // Use the project root discovery utility to find .env file
  const envPath = getProjectEnvPath();
  const tokenManager = new CLITokenManager(envPath);

  // Display current API key status
  await tokenManager.displayStatus();

  const hasApiKey = await tokenManager.hasValidTokens();
  if (!hasApiKey) {
    console.log(chalk.yellow('\nTo configure JQuants API v2:'));
    console.log(chalk.gray('  Set JQUANTS_API_KEY in your .env file'));
  }
}

/**
 * Deprecated alias for status command
 */
export const refreshTokensCommand = define({
  name: 'refresh-tokens',
  description: 'Deprecated alias of "jquants auth status"',
  args: {},
  examples: `
# Deprecated alias (use jquants auth status)
${CLI_NAME} jquants auth refresh-tokens
  `.trim(),
  run: async () => {
    console.log(chalk.yellow(`[DEPRECATED] "${CLI_NAME} jquants auth refresh-tokens" is an alias of "status".`));
    await statusAction();
  },
});
