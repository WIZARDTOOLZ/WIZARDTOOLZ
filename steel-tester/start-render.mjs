import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const children = new Map();
let shuttingDown = false;

function spawnService(name, script) {
  const child = spawn(process.execPath, [script], {
    cwd: __dirname,
    env: process.env,
    stdio: 'inherit',
  });

  children.set(name, child);

  child.on('exit', (code, signal) => {
    children.delete(name);
    if (shuttingDown) {
      return;
    }

    console.error(`[start-render] ${name} exited (${signal || code}). Shutting down parent.`);
    shuttingDown = true;
    for (const sibling of children.values()) {
      sibling.kill('SIGTERM');
    }
    process.exit(typeof code === 'number' ? code : 1);
  });

  return child;
}

function shutdown(signal) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;
  console.log(`[start-render] Received ${signal}. Stopping child services...`);
  for (const child of children.values()) {
    child.kill('SIGTERM');
  }
  setTimeout(() => process.exit(0), 1000).unref();
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

console.log('[start-render] Starting bot + worker stack...');
spawnService('bot', 'bot.js');
spawnService('worker', 'worker-bot.js');
