import { $ } from 'bun';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig, type PluginOption } from 'vite';

const WEB_PORT = 5173;

function killPortPlugin(): PluginOption {
	return {
		name: 'kill-port-plugin',
		apply: 'serve',
		async buildStart() {
			await killPortProcess(WEB_PORT);
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
				await $`kill -9 ${pids.join(' ')}`;
				await Bun.sleep(1000);
			}
		}
	} catch {
		// No processes found using the port
	}
}

export default defineConfig({
	plugins: [killPortPlugin(), react(), tailwindcss()],
	resolve: {
		alias: {
			'@': `${import.meta.dirname}/src`,
			'@trading25/clients-ts': `${import.meta.dirname}/../clients-ts/src`,
			'@trading25/market-db-ts': `${import.meta.dirname}/../market-db-ts/src`,
			'@trading25/dataset-db-ts': `${import.meta.dirname}/../dataset-db-ts/src`,
			'@trading25/portfolio-db-ts': `${import.meta.dirname}/../portfolio-db-ts/src`,
		},
	},
	server: {
		port: WEB_PORT,
		strictPort: true,
		proxy: {
			'/api': {
				target: 'http://localhost:3002',
				changeOrigin: true,
			},
		},
	},
	build: {
		outDir: 'dist',
		chunkSizeWarningLimit: 800,
		rollupOptions: {
			output: {
				manualChunks: {
					'react-vendor': ['react', 'react-dom'],
					charts: ['lightweight-charts'],
					'ui-vendor': [
						'@radix-ui/react-slot',
						'@radix-ui/react-select',
						'@radix-ui/react-switch',
					],
					'state-vendor': ['zustand', '@tanstack/react-query'],
				},
			},
		},
	},
});
