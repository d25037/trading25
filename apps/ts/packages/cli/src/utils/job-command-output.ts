import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';

export interface CommandOutputContext {
  log: (message: string) => void;
}

export interface CommandOutputOptions {
  json?: boolean;
  output?: string | undefined;
}

interface EmitCommandOutputArgs<TPayload> {
  ctx: CommandOutputContext;
  payload: TPayload;
  options: CommandOutputOptions;
  renderTable?: (payload: TPayload) => void;
}

export function resolveWaitFlag(wait: boolean | undefined, noWait: boolean | undefined): boolean {
  if (noWait === true) {
    return false;
  }
  if (typeof wait === 'boolean') {
    return wait;
  }
  return true;
}

export async function emitCommandOutput<TPayload>({
  ctx,
  payload,
  options,
  renderTable,
}: EmitCommandOutputArgs<TPayload>): Promise<void> {
  if (options.json) {
    ctx.log(JSON.stringify(payload, null, 2));
  } else if (renderTable) {
    renderTable(payload);
  }

  if (!options.output) {
    return;
  }

  const absolutePath = resolve(options.output);
  await mkdir(dirname(absolutePath), { recursive: true });
  await writeFile(absolutePath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
  ctx.log(`Saved output: ${absolutePath}`);
}
