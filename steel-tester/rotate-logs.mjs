import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const archiveRoot = path.join(__dirname, 'logs', 'archive');
const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
const archiveDir = path.join(archiveRoot, timestamp);

const entries = await fs.readdir(__dirname, { withFileTypes: true });
const logFiles = entries
  .filter((entry) => entry.isFile() && entry.name.toLowerCase().endsWith('.log'))
  .map((entry) => entry.name);

if (logFiles.length === 0) {
  console.log('[rotate-logs] No log files found.');
  process.exit(0);
}

await fs.mkdir(archiveDir, { recursive: true });

for (const fileName of logFiles) {
  const sourcePath = path.join(__dirname, fileName);
  const targetPath = path.join(archiveDir, fileName);
  await fs.rename(sourcePath, targetPath);
}

console.log(`[rotate-logs] Archived ${logFiles.length} log file(s) to ${archiveDir}`);
