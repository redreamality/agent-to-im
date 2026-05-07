/**
 * floatboat-channel.ts
 * Floatboat Channel MQTT + Protobuf transport layer.
 *
 * Incoming: Subscribe to down/{agentId}/{endpointId}
 *           Decode Protobuf Envelope → ChatMessageDown
 * Outgoing: Encode ChatMessageUp in Envelope
 *           Publish to up/{agentId}/{endpointId}
 */

import mqtt, { MqttClient } from 'mqtt';
import protobuf from 'protobufjs';
import { v4 as uuidv4 } from 'uuid';
import type { BridgeConfig } from './config.js';

const PROTO_PATH = '/Users/aoe/floatboat-channel/proto/aoe/v1/envelope.proto';
const PROTO_ROOT  = '/Users/aoe/floatboat-channel/proto';

export interface IncomingMessage {
  messageId: string;
  sessionKey: string;
  senderAgentId: string;
  senderEndpointId: string;
  contentType: string;
  text: string;
  createdAtMs: number;
}

export type MessageHandler = (msg: IncomingMessage) => void;

export class FloatboatChannel {
  private client: MqttClient | null = null;
  private EnvelopeType: protobuf.Type | null = null;
  private handlers: MessageHandler[] = [];
  private running = false;

  constructor(private cfg: BridgeConfig) {}

  // -----------------------------------------------------------------------
  // Lifecycle
  // -----------------------------------------------------------------------

  async start(): Promise<void> {
    await this.loadProto();
    await this.connect();
    this.running = true;
    console.info(`[channel] Started. Subscribed to down/${this.cfg.agentId}/${this.cfg.endpointId}`);
  }

  stop(): void {
    this.running = false;
    this.client?.end(true);
    this.client = null;
    console.info('[channel] Stopped.');
  }

  // -----------------------------------------------------------------------
  // Proto
  // -----------------------------------------------------------------------

  private async loadProto(): Promise<void> {
    const root = new protobuf.Root();
    root.resolvePath = (_origin: string, target: string) =>
      target.startsWith('/') ? target : `${PROTO_ROOT}/${target}`;
    await root.load(PROTO_PATH, { keepCase: false });
    this.EnvelopeType = root.lookupType('aoe.v1.Envelope');
    console.debug('[channel] Proto loaded.');
  }

  // -----------------------------------------------------------------------
  // MQTT
  // -----------------------------------------------------------------------

  private connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      const clientId = `claude-to-floatboat-${Date.now()}`;
      const c = mqtt.connect(this.cfg.mqttUrl, {
        clientId,
        clean: true,
        reconnectPeriod: 3000,
        connectTimeout: 15_000,
      });
      this.client = c;

      c.once('connect', () => {
        const topic = `down/${this.cfg.agentId}/${this.cfg.endpointId}`;
        c.subscribe(topic, { qos: 1 }, (err) => {
          if (err) return reject(err);
          resolve();
        });
      });

      c.on('message', (_topic: string, payload: Buffer) => {
        this.handleRawMessage(payload).catch(e =>
          console.error('[channel] handleRawMessage error:', e));
      });

      c.on('error', (err) => {
        console.error('[channel] MQTT error:', err.message);
        reject(err);
      });

      c.on('reconnect', () => console.debug('[channel] Reconnecting...'));
      c.on('close', () => {
        if (this.running) console.warn('[channel] MQTT connection closed, will reconnect...');
      });
    });
  }

  // -----------------------------------------------------------------------
  // Decode
  // -----------------------------------------------------------------------

  private async handleRawMessage(payload: Buffer): Promise<void> {
    if (!this.EnvelopeType) return;
    try {
      const envelope = this.EnvelopeType.decode(payload) as any;
      const down = envelope.chatMessageDown;
      if (!down) return; // Not a chat message, ignore

      const msg: IncomingMessage = {
        messageId:       down.messageId        ?? '',
        sessionKey:      down.sessionKey       ?? '',
        senderAgentId:   down.senderAgentId    ?? '',
        senderEndpointId: down.senderEndpointId ?? '',
        contentType:     down.contentType      ?? 'text/plain',
        text:            down.contentText      ?? '',
        createdAtMs:     Number(down.createdAtMs ?? 0),
      };

      // Ignore messages we sent ourselves
      if (msg.senderAgentId === this.cfg.agentId) return;
      // Ignore non-text
      if (!msg.text.trim()) return;

      for (const h of this.handlers) h(msg);
    } catch (e) {
      console.error('[channel] Failed to decode envelope:', e);
    }
  }

  // -----------------------------------------------------------------------
  // Send
  // -----------------------------------------------------------------------

  async send(sessionKey: string, text: string): Promise<void> {
    if (!this.EnvelopeType || !this.client) {
      throw new Error('Channel not started');
    }

    const envelope = {
      messageId:       uuidv4(),
      idempotencyKey:  uuidv4(),
      sentAtMs:        Date.now(),
      bearerToken:     this.cfg.jwtToken,
      route: {
        from: {
          type:    2,  // PRINCIPAL_ENDPOINT
          id:      this.cfg.endpointId,
          agentId: this.cfg.agentId,
        },
        sessionKey,
        intent: 2,  // INTENT_CHAT
      },
      delivery: { qos: 1, ttlMs: 60_000, requireAck: false },
      chatMessageUp: {
        sessionKey,
        contentType: 'text/plain',
        contentText: text,
      },
    };

    const errMsg = this.EnvelopeType.verify(envelope);
    if (errMsg) throw new Error(`Envelope invalid: ${errMsg}`);

    const msg = this.EnvelopeType.create(envelope);
    const buf = this.EnvelopeType.encode(msg).finish() as unknown as Buffer;

    const topic = `up/${this.cfg.agentId}/${this.cfg.endpointId}`;
    await new Promise<void>((resolve, reject) => {
      this.client!.publish(topic, buf, { qos: 1 }, err => err ? reject(err) : resolve());
    });

    console.debug(`[channel] Sent to ${sessionKey}: ${text.slice(0, 80)}...`);
  }

  // -----------------------------------------------------------------------
  // Subscription
  // -----------------------------------------------------------------------

  onMessage(handler: MessageHandler): void {
    this.handlers.push(handler);
  }
}
