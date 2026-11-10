// Every project command disables telemetry, including when npm inherits no env.
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
process.env.CARBON_TELEMETRY_DISABLED = '1';
process.env.IBM_TELEMETRY_DISABLED = 'true';
process.env.npm_config_ignore_scripts = 'true';
process.env.npm_config_audit = 'false';
process.env.npm_config_fund = 'false';
const [command, ...args] = process.argv.slice(2);
if (command === 'build') process.env.NODE_ENV = 'production';
const tools = {
  dev: ['vite/bin/vite.js'],
  build: ['vite/bin/vite.js', 'build'],
  check: ['svelte-check/bin/svelte-check', '--tsconfig', './tsconfig.json'],
  test: ['vitest/vitest.mjs', 'run'],
};
if (!(command in tools)) throw new Error('Unknown project command');
const [file, ...defaults] = tools[command];
const child = spawn(process.execPath, [fileURLToPath(new URL(`./node_modules/${file}`, import.meta.url)), ...defaults, ...args], { stdio: 'inherit', env: process.env });
child.on('error', (error) => { console.error(error.message); process.exitCode = 1; });
child.on('exit', (code) => { process.exitCode = code ?? 1; });
