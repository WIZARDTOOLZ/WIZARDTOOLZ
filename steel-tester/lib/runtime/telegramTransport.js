import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import { webhookCallback } from 'grammy';
import { buildWebhookUrl } from '../config.js';

const LAUNCH_ASSETS_DIR = path.resolve('data', 'launch-assets');

function getContentTypeForAsset(filePath) {
  const extension = path.extname(filePath).toLowerCase();
  switch (extension) {
    case '.json':
      return 'application/json';
    case '.png':
      return 'image/png';
    case '.jpg':
    case '.jpeg':
      return 'image/jpeg';
    case '.webp':
      return 'image/webp';
    default:
      return 'application/octet-stream';
  }
}

export async function registerTelegramCommands(bot, commands) {
  try {
    await bot.api.setMyCommands(commands);
  } catch (error) {
    console.error('Failed to register Telegram slash commands:', error);
  }
}

export function startPaymentPollingLoop(intervalMs, pollPendingPayments) {
  setInterval(() => {
    void pollPendingPayments();
  }, intervalMs);
}

export async function startPollingTransport(bot) {
  try {
    await bot.api.deleteWebhook({ drop_pending_updates: false });
  } catch (error) {
    console.error('Failed to clear Telegram webhook before polling start:', error);
  }

  console.log('Telegram bot starting in polling mode...');
  await bot.start();
}

export async function startWebhookTransport(bot, cfg) {
  const webhookHandler = webhookCallback(bot, 'http', {
    secretToken: cfg.telegramWebhookSecret,
  });
  const webhookUrl = buildWebhookUrl(cfg);
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url || '/', `http://${req.headers.host || '127.0.0.1'}`);

    if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/healthz')) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        ok: true,
        service: 'telegram-bot',
        transport: cfg.telegramTransport,
        webhookPath: cfg.telegramWebhookPath,
      }));
      return;
    }

    if (req.method === 'GET' && url.pathname.startsWith('/launch-assets/')) {
      const relativePath = url.pathname.replace(/^\/launch-assets\/+/, '');
      const assetPath = path.resolve(LAUNCH_ASSETS_DIR, relativePath);
      if (!assetPath.startsWith(LAUNCH_ASSETS_DIR)) {
        res.writeHead(403, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: 'forbidden' }));
        return;
      }

      try {
        const data = await fs.readFile(assetPath);
        res.writeHead(200, { 'Content-Type': getContentTypeForAsset(assetPath) });
        res.end(data);
      } catch {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: 'not_found' }));
      }
      return;
    }

    if (req.method === 'POST' && url.pathname === cfg.telegramWebhookPath) {
      try {
        await webhookHandler(req, res);
      } catch (error) {
        console.error('Telegram webhook handler failed:', error);
        if (!res.headersSent) {
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false }));
        }
      }
      return;
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'not_found' }));
  });

  await new Promise((resolve) => {
    server.listen(cfg.telegramWebhookPort, () => {
      console.log(`Telegram bot webhook server listening on :${cfg.telegramWebhookPort}`);
      resolve();
    });
  });

  if (webhookUrl) {
    try {
      await bot.api.setWebhook(webhookUrl, {
        secret_token: cfg.telegramWebhookSecret,
        drop_pending_updates: false,
      });
      console.log(`Telegram webhook registered at ${webhookUrl}`);
    } catch (error) {
      console.error('Failed to register Telegram webhook:', error);
    }
  } else {
    console.warn('Webhook mode is enabled, but no TELEGRAM_WEBHOOK_BASE_URL or RENDER_EXTERNAL_URL is set. The webhook server is running, but Telegram webhook registration was skipped.');
  }

  const shutdown = (signal) => {
    console.log(`Telegram webhook server received ${signal}. Closing...`);
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 1000).unref();
  };

  process.once('SIGINT', () => shutdown('SIGINT'));
  process.once('SIGTERM', () => shutdown('SIGTERM'));
}
