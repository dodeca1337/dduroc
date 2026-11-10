import { readFileSync, existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import postcss from 'postcss';
import postcssUrl from 'postcss-url';
import { expect, it } from 'vitest';
import { carbonFontUrl, localCarbonFonts } from '../src/font-assets';

it('rebases the actual Carbon retained font URLs without changing font rules or Unicode ranges', async () => {
  const root = process.cwd();
  const file = resolve(root, 'node_modules/carbon-components-svelte/css/all.css');
  const original = postcss.parse(readFileSync(file, 'utf8'), { from: file });
  const transformed = await postcss([postcssUrl({ url: carbonFontUrl(root) })]).process(original.toString(), { from: file });
  const after = postcss.parse(transformed.css);
  // Every original selector, at-rule, declaration and Unicode range survives.
  const withoutSources = (tree: postcss.Root) => { const copy = tree.clone(); copy.walkDecls('src', declaration => { declaration.value = '<font asset URL>'; }); return copy.toString(); };
  expect(withoutSources(after)).toBe(withoutSources(original));
  let mapped = 0;
  after.walkDecls('src', declaration => {
    for (const font of localCarbonFonts) {
      if (declaration.value.includes(`@ibm/${font.package}/fonts/complete/woff2/${font.file}.woff2`)) {
        const url = declaration.value.match(/url\(([^)]+)\)/)?.[1]?.replaceAll('"', '').replaceAll("'", '');
        expect(url).toBeTruthy();
        expect(existsSync(resolve(dirname(file), url!))).toBe(true);
        mapped++;
      }
    }
  });
  expect(mapped).toBe(27); // Sans: 3 × (complete + 6 subsets); Mono: complete + 5.
});

it('leaves unrelated network assets and unsupported font styles unchanged', () => {
  const url = carbonFontUrl(process.cwd());
  for (const source of [
    'https://example.com/font.woff2',
    'https://1.www.s81c.com/common/carbon/plex/fonts/IBM-Plex-Sans/fonts/complete/woff2/IBMPlexSans-LightItalic.woff2',
    './local-image.svg',
  ]) expect(url({ url: source }, { file: '/styles/input.css' })).toBe(source);
});
