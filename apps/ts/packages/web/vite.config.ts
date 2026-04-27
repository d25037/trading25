import { resolve } from 'node:path';
import { $ } from 'bun';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig, loadEnv, type PluginOption } from 'vite';
import { resolveBtApiUrl } from './src/lib/btApiUrl';

const WEB_PORT = 5173;
const REPO_ROOT = resolve(import.meta.dirname, '../../../..');
const MANUAL_CHUNK_GROUPS = [
	{ name: 'react-vendor', packages: ['react', 'react-dom'] },
	{ name: 'charts', packages: ['lightweight-charts'] },
	{
		name: 'ui-vendor',
		packages: ['@radix-ui/react-select', '@radix-ui/react-slot', '@radix-ui/react-switch'],
	},
	{ name: 'state-vendor', packages: ['@tanstack/react-query', 'zustand'] },
] as const;

function killPortPlugin(): PluginOption {
	let resolvedPort = WEB_PORT;

	return {
		name: 'kill-port-plugin',
		apply: 'serve',
		configResolved(config) {
			resolvedPort = config.server.port ?? WEB_PORT;
		},
		async buildStart() {
			await killPortProcess(resolvedPort);
		},
	};
}

async function killPortProcess(port: number): Promise<void> {
	try {
		const result = await $`lsof -ti :${port}`.text();
		const trimmed = result.trim();
		if (trimmed) {
			const pids = trimmed.split('\n').filter((pid) => pid.trim());
			if (pids.length > 0) {
				console.log(`[vite] Killing processes using port ${port}: ${pids.join(', ')}`);
				const proc = Bun.spawn({
					cmd: ['kill', '-9', ...pids],
					stdout: 'ignore',
					stderr: 'ignore',
				});
				await proc.exited;
				await Bun.sleep(300);
			}
		}
	} catch {
		// No processes found using the port
	}
}

function resolveManualChunk(id: string): string | undefined {
	if (!id.includes('node_modules')) {
		return undefined;
	}

	for (const group of MANUAL_CHUNK_GROUPS) {
		if (group.packages.some((packageName) => id.includes(`node_modules/${packageName}/`))) {
			return group.name;
		}
	}

	return undefined;
}

export default defineConfig(({ mode }) => {
	const env = { ...loadEnv(mode, REPO_ROOT, ''), ...process.env };
	const btApiUrl = resolveBtApiUrl(env);

	return {
		envDir: REPO_ROOT,
		plugins: [killPortPlugin(), react(), tailwindcss()],
		resolve: {
			alias: [
				{ find: /^@\//, replacement: `${import.meta.dirname}/src/` },
				{
					find: /^@trading25\/api-clients\/(.*)$/,
					replacement: `${import.meta.dirname}/../api-clients/src/$1`,
				},
				{ find: '@trading25/api-clients', replacement: `${import.meta.dirname}/../api-clients/src` },
				{
					find: /^@trading25\/contracts\/(.*)$/,
					replacement: `${import.meta.dirname}/../contracts/src/$1`,
				},
				{ find: '@trading25/contracts', replacement: `${import.meta.dirname}/../contracts/src` },
				{ find: /^@trading25\/utils\/(.*)$/, replacement: `${import.meta.dirname}/../utils/src/$1` },
				{ find: '@trading25/utils', replacement: `${import.meta.dirname}/../utils/src` },
			],
		},
		server: {
			host: '0.0.0.0',
			allowedHosts: ['mba-m4', 'hx90', 'hx90.tailb95be2.ts.net', 'mba-m4.tailb95be2.ts.net'],
			port: WEB_PORT,
			strictPort: true,
			proxy: {
				'/api': {
					target: btApiUrl,
					changeOrigin: true,
				},
			},
		},
		build: {
			outDir: 'dist',
			chunkSizeWarningLimit: 800,
			rollupOptions: {
				output: {
					manualChunks: resolveManualChunk,
				},
			},
		},
	};
});
