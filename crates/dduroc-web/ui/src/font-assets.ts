// Build-time asset resolution through postcss-url's documented custom URL API.
// Carbon's font rules and Unicode ranges remain intact; Vite emits hashed files.
import { relative, resolve, sep } from 'node:path';

export const localCarbonFonts = [
  { family: 'Sans', package: 'plex-sans', file: 'IBMPlexSans-Light' },
  { family: 'Sans', package: 'plex-sans', file: 'IBMPlexSans-Regular' },
  { family: 'Sans', package: 'plex-sans', file: 'IBMPlexSans-SemiBold' },
  { family: 'Mono', package: 'plex-mono', file: 'IBMPlexMono-Regular' },
] as const;
const subsets = ['Cyrillic', 'Pi', 'Latin1', 'Latin2', 'Latin3', 'Greek'];

export function carbonFontUrl(projectRoot: string) {
  const assets = new Map<string, string>();
  for (const font of localCarbonFonts) {
    const prefix = `https://1.www.s81c.com/common/carbon/plex/fonts/IBM-Plex-${font.family}/fonts/`;
    const target = resolve(projectRoot, 'node_modules', '@ibm', font.package, 'fonts/complete/woff2', `${font.file}.woff2`);
    assets.set(`${prefix}complete/woff2/${font.file}.woff2`, target);
    for (const subset of subsets) assets.set(`${prefix}split/woff2/${font.file}-${subset}.woff2`, target);
  }
  return (asset: { url: string }, directory: { file?: string }) => {
    const target = assets.get(asset.url);
    return target ? relative(directory.file ?? projectRoot, target).split(sep).join('/') : asset.url;
  };
}
