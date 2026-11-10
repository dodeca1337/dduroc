import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import postcssUrl from 'postcss-url';
import { carbonFontUrl } from './src/font-assets.ts';
import { defineConfig } from 'vitest/config';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import { optimizeCss } from 'carbon-preprocess-svelte';
export default defineConfig({
  base: './',
  resolve: { conditions: ['browser'] },
  css: { postcss: { plugins: [postcssUrl({ url: carbonFontUrl(fileURLToPath(new URL('.', import.meta.url))) })] } },
  plugins: [
    {
      name: 'third-party-notices',
      generateBundle() { this.emitFile({ type: 'asset', fileName: 'LICENSE-THIRD-PARTY.txt', source: readFileSync(new URL('./LICENSE-THIRD-PARTY.txt', import.meta.url), 'utf8') }); },
    },
    { name: 'development-base', apply: 'serve', transformIndexHtml: html => html.replace('__DDUROC_BASE_HREF__', '/') },
    svelte(), optimizeCss(),
  ],
  build: { target: 'es2022', sourcemap: false },
  test: { environment: 'jsdom', include: ['tests/**/*.test.ts'], restoreMocks: true },
});
