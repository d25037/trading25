/**
 * Analysis Commands - Entry Point
 * Financial analysis, screening, and ranking commands with direct imports for full help display
 */

import { cli, define } from 'gunshi';
import { CLI_NAME, CLI_VERSION } from '../../utils/constants.js';
import { factorRegressionCommand } from './factor-regression.js';
import { portfolioFactorRegressionCommand } from './portfolio-factor-regression.js';
import { rankingCommand } from './ranking.js';
import { roeCommand } from './roe.js';
import { screeningCommand } from './screening.js';

// Analysis group command definition (exported for help display)
export const analysisCommand = define({
  name: 'analysis',
  description: 'Financial analysis, screening, and ranking commands',
  run: (ctx) => {
    ctx.log('Available commands: factor-regression, roe, ranking, screening');
    ctx.log(`Use "${CLI_NAME} analysis <command> --help" for more information`);
  },
});

// Direct imports for full args display in help
const subCommands = {
  'factor-regression': factorRegressionCommand,
  'portfolio-factor-regression': portfolioFactorRegressionCommand,
  roe: roeCommand,
  ranking: rankingCommand,
  screening: screeningCommand,
};

// Export command runner for this group
export default async function analysisCommandRunner(args: string[]): Promise<void> {
  await cli(args, analysisCommand, {
    name: `${CLI_NAME} analysis`,
    version: CLI_VERSION,
    description: 'Financial analysis, screening, and ranking commands',
    subCommands,
  });
}
