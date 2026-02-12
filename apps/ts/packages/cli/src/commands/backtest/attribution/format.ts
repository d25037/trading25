import { CLIValidationError } from '../../../utils/error-handling.js';

export function parseTableJsonFormat(value: string): 'table' | 'json' {
  if (value === 'table' || value === 'json') {
    return value;
  }
  throw new CLIValidationError('format must be one of: table, json');
}
