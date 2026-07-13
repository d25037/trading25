import { cp, mkdir, rm } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const outdir = resolve(packageRoot, 'dist');

await rm(outdir, { recursive: true, force: true });
await mkdir(outdir, { recursive: true });

const result = await Bun.build({
  entrypoints: [
    resolve(packageRoot, 'src/background.ts'),
    resolve(packageRoot, 'src/shikiho-content.ts'),
    resolve(packageRoot, 'src/localhost-content.ts'),
  ],
  outdir,
  target: 'browser',
  format: 'iife',
  naming: '[name].[ext]',
  minify: false,
  sourcemap: 'none',
});

if (!result.success) {
  throw new AggregateError(result.logs, 'Failed to build Shikiho extension');
}

await cp(resolve(packageRoot, 'manifest.json'), resolve(outdir, 'manifest.json'));
