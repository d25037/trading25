import dns from 'node:dns';

function applyDnsPromisesCompat(): void {
  if (typeof dns.promises.getDefaultResultOrder !== 'function') {
    dns.promises.getDefaultResultOrder = dns.getDefaultResultOrder.bind(dns);
  }

  if (typeof dns.promises.setDefaultResultOrder !== 'function') {
    dns.promises.setDefaultResultOrder = dns.setDefaultResultOrder.bind(dns);
  }
}

async function main(): Promise<void> {
  applyDnsPromisesCompat();
  process.argv = [process.argv[0] ?? 'bun', 'vite', ...process.argv.slice(2)];
  await import('../node_modules/vite/bin/vite.js');
}

await main();
