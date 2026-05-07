/**
 * daemon.ts - Floatboat ↔ Claude Code Bridge
 *
 * 1. Connects to Floatboat Channel via MQTT
 * 2. Receives ChatMessageDown events
 * 3. Runs Claude Code via @anthropic-ai/claude-agent-sdk
 * 4. Streams response back to the same session_key
 */

import { query } from '@anthropic-ai/claude-agent-sdk';
import { loadConfig, ensureConfigDir } from './config.js';
import { FloatboatChannel } from './floatboat-channel.js';
import type { IncomingMessage } from './floatboat-channel.js';

// ─────────────────────────────────────────────────────────────────────────────
// Session state: track active Claude sessions per sessionKey
// ─────────────────────────────────────────────────────────────────────────────

interface ActiveSession {
  sessionKey: string;
  abort: AbortController;
  startedAt: number;
}

const activeSessions = new Map<string, ActiveSession>();

// ─────────────────────────────────────────────────────────────────────────────
// Process one incoming message
// ─────────────────────────────────────────────────────────────────────────────

async function handleMessage(
  channel: FloatboatChannel,
  cfg: ReturnType<typeof loadConfig>,
  msg: IncomingMessage,
): Promise<void> {
  const { sessionKey, text, senderAgentId } = msg;

  // Session filter
  if (cfg.allowedSessions.length > 0 && !cfg.allowedSessions.includes(sessionKey)) {
    console.debug(`[daemon] Ignoring session ${sessionKey} (not in allowlist)`);
    return;
  }

  // Concurrency guard
  if (activeSessions.size >= cfg.maxConcurrent) {
    console.warn(`[daemon] Max concurrent sessions (${cfg.maxConcurrent}) reached, dropping message.`);
    await channel.send(sessionKey, '⚠️ 当前任务已满，请稍后再试。');
    return;
  }

  // Cancel previous session on the same sessionKey if any
  const existing = activeSessions.get(sessionKey);
  if (existing) {
    console.info(`[daemon] Cancelling previous session for ${sessionKey}`);
    existing.abort.abort();
    activeSessions.delete(sessionKey);
  }

  const abort = new AbortController();
  activeSessions.set(sessionKey, { sessionKey, abort, startedAt: Date.now() });

  console.info(`[daemon] New query from ${senderAgentId} in ${sessionKey}: ${text.slice(0, 100)}`);

  const timeout = setTimeout(() => {
    console.warn(`[daemon] Session ${sessionKey} timed out after ${cfg.responseTimeoutMs}ms`);
    abort.abort();
  }, cfg.responseTimeoutMs);

  try {
    const responseChunks: string[] = [];

    const q = query({
      prompt: text,
      options: {
        abortController: abort,
        cwd: cfg.workDir,
        allowedTools: cfg.allowedTools.length > 0 ? cfg.allowedTools : undefined,
        canUseTool: cfg.autoApprove
          ? async () => ({ behavior: 'allow' as const })
          : undefined,
      },
    });

    for await (const event of q) {
      if (abort.signal.aborted) break;
      // Collect text from assistant messages
      if (
        event.type === 'assistant' &&
        Array.isArray((event as any).message?.content)
      ) {
        for (const block of (event as any).message.content) {
          if (block.type === 'text') {
            responseChunks.push(block.text);
          }
        }
      }
    }

    const response = responseChunks.join('').trim();
    if (response && !abort.signal.aborted) {
      // Split into chunks if too long (MQTT message size limit)
      const MAX_CHUNK = 4000;
      if (response.length <= MAX_CHUNK) {
        await channel.send(sessionKey, response);
      } else {
        for (let i = 0; i < response.length; i += MAX_CHUNK) {
          await channel.send(sessionKey, response.slice(i, i + MAX_CHUNK));
        }
      }
    }
  } catch (err: any) {
    if (err?.name === 'AbortError' || abort.signal.aborted) {
      console.info(`[daemon] Session ${sessionKey} aborted.`);
    } else {
      console.error(`[daemon] Error in session ${sessionKey}:`, err);
      try {
        await channel.send(sessionKey, `❌ 出错了: ${err?.message ?? String(err)}`);
      } catch {}
    }
  } finally {
    clearTimeout(timeout);
    activeSessions.delete(sessionKey);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  ensureConfigDir();
  const cfg = loadConfig();

  console.info('[daemon] claude-to-floatboat starting...');
  console.info(`[daemon] MQTT:     ${cfg.mqttUrl}`);
  console.info(`[daemon] AgentId:  ${cfg.agentId}`);
  console.info(`[daemon] Endpoint: ${cfg.endpointId}`);
  console.info(`[daemon] WorkDir:  ${cfg.workDir}`);

  const channel = new FloatboatChannel(cfg);

  channel.onMessage((msg: IncomingMessage) => {
    handleMessage(channel, cfg, msg).catch(e =>
      console.error('[daemon] Unhandled error in handleMessage:', e));
  });

  await channel.start();

  console.info('[daemon] Ready. Waiting for messages from Floatboat Channel...');

  // Graceful shutdown
  const shutdown = (sig: string) => {
    console.info(`[daemon] Received ${sig}, shutting down...`);
    // Abort all active sessions
    for (const s of activeSessions.values()) s.abort.abort();
    activeSessions.clear();
    channel.stop();
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT',  () => shutdown('SIGINT'));
  process.on('SIGHUP',  () => shutdown('SIGHUP'));

  process.on('unhandledRejection', (reason) => {
    console.error('[daemon] Unhandled rejection:', reason);
  });

  // Keep alive
  setInterval(() => {
    const n = activeSessions.size;
    if (n > 0) console.debug(`[daemon] Active sessions: ${n}`);
  }, 60_000).unref();
}

main().catch(err => {
  console.error('[daemon] Fatal:', err);
  process.exit(1);
});
