/**
 * config.ts
 * 从 ~/.claude-to-floatboat/config.env 或环境变量加载配置
 */

import fs from 'fs';
import path from 'path';
import os from 'os';

export interface BridgeConfig {
  // Floatboat Channel
  mqttUrl: string;
  agentId: string;
  endpointId: string;
  jwtToken: string;
  channelApi: string;
  // Claude Code SDK
  workDir: string;
  allowedTools: string[];
  autoApprove: boolean;
  // Behavior
  maxConcurrent: number;
  responseTimeoutMs: number;
  logLevel: 'debug' | 'info' | 'warn' | 'error';
  allowedSessions: string[];
}

export const CONFIG_DIR = path.join(os.homedir(), '.claude-to-floatboat');
const CONFIG_FILE = path.join(CONFIG_DIR, 'config.env');

function parseEnvFile(content: string): Record<string, string> {
  const result: Record<string, string> = {};
  for (const line of content.split('\n')) {
    const t = line.trim();
    if (!t || t.startsWith('#')) continue;
    const eq = t.indexOf('=');
    if (eq < 0) continue;
    result[t.slice(0, eq).trim()] = t.slice(eq + 1).trim().replace(/^["']|["']$/g, '');
  }
  return result;
}

function csv(v?: string): string[] {
  return v ? v.split(',').map(s => s.trim()).filter(Boolean) : [];
}

export function loadConfig(): BridgeConfig {
  let file: Record<string, string> = {};
  if (fs.existsSync(CONFIG_FILE)) {
    try { file = parseEnvFile(fs.readFileSync(CONFIG_FILE, 'utf8')); } catch {}
  }
  const get = (k: string) => process.env[k] ?? file[k];
  return {
    mqttUrl:          get('CTF_MQTT_URL')          ?? 'mqtt://localhost:1883',
    agentId:          get('CTF_AGENT_ID')           ?? 'claude-code-agent',
    endpointId:       get('CTF_ENDPOINT_ID')        ?? 'claude-code-desktop',
    jwtToken:         get('CTF_JWT_TOKEN')          ?? '',
    channelApi:       get('CTF_CHANNEL_API')        ?? 'http://localhost:9092',
    workDir:          get('CTF_WORK_DIR')           ?? process.cwd(),
    allowedTools:     csv(get('CTF_ALLOWED_TOOLS')),
    autoApprove:     (get('CTF_AUTO_APPROVE')       ?? 'true') !== 'false',
    maxConcurrent:   parseInt(get('CTF_MAX_CONCURRENT')       ?? '5', 10),
    responseTimeoutMs: parseInt(get('CTF_RESPONSE_TIMEOUT_MS') ?? '120000', 10),
    logLevel:        (get('CTF_LOG_LEVEL')          ?? 'info') as BridgeConfig['logLevel'],
    allowedSessions:  csv(get('CTF_ALLOWED_SESSIONS')),
  };
}

export function ensureConfigDir(): void {
  fs.mkdirSync(CONFIG_DIR, { recursive: true });
}
