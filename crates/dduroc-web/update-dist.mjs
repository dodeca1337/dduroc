// Refresh only the checked-in generated assets from build.rs's actual OUT_DIR.
// Run from any directory: node crates/dduroc-web/update-dist.mjs
import { spawnSync } from 'node:child_process';
import { cpSync, existsSync, readFileSync, renameSync, rmSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const crate = dirname(fileURLToPath(import.meta.url));
const buildEnv = { ...process.env };
delete buildEnv.DDUROC_VERIFY_PREBUILT;
const result = spawnSync('cargo', ['build', '--locked', '-j', '2', '-p', 'dduroc-web', '--features', 'build-ui', '--message-format=json'], {
  cwd: crate, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024,
  env: buildEnv,
});
if (result.stderr) process.stderr.write(result.stderr);
const messages = result.stdout.split('\n').filter(Boolean).map((line) => JSON.parse(line));
for (const message of messages) { if (message.reason === 'compiler-message' && message.message?.rendered) process.stderr.write(message.message.rendered); }
if (result.error || result.status !== 0) throw result.error ?? new Error(`cargo build failed: ${result.status}`);
const build = messages.find((message) => message.reason === 'build-script-executed' && /(?:\/dduroc-web#|#dduroc-web@)/.test(message.package_id));
if (!build) throw new Error('Cargo did not report the dduroc-web build output');
const source = join(build.out_dir, 'dduroc-ui-dist');
const manifest = JSON.parse(readFileSync(join(source, 'build-manifest.json'), 'utf8'));
if (manifest.version !== 1 || !manifest.assets?.['index.html']) throw new Error('Invalid generated asset manifest');
const dist = join(crate, 'ui', 'dist');
const staging = join(crate, 'ui', '.dist-staging');
const backup = join(crate, 'ui', '.dist-backup');
if (existsSync(backup)) throw new Error('Previous dist backup exists; restore or remove it first');
rmSync(staging, { recursive: true, force: true });
cpSync(source, staging, { recursive: true, errorOnExist: true });
let moved = false;
try {
  if (existsSync(dist)) { renameSync(dist, backup); moved = true; }
  renameSync(staging, dist);
} catch (error) {
  if (moved && !existsSync(dist)) renameSync(backup, dist);
  throw error;
}
if (moved) rmSync(backup, { recursive: true, force: true });
console.log(`Updated ${dist} from ${source}`);
