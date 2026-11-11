import { randomUUID } from 'node:crypto'
import { hostname } from 'node:os'

const str = (name: string, def?: string): string => {
  const v = process.env[name]
  if (v === undefined || v === '') {
    if (def === undefined) throw new Error(`Missing required env var ${name}`)
    return def
  }
  return v
}
const int = (name: string, def: number): number => {
  const v = process.env[name]
  if (v === undefined || v === '') return def
  const n = Number(v)
  if (!Number.isFinite(n)) throw new Error(`Env var ${name} must be a number`)
  return n
}
const bool = (name: string, def: boolean): boolean => {
  const v = process.env[name]
  if (v === undefined || v === '') return def
  return ['1', 'true', 'yes', 'on'].includes(v.toLowerCase())
}

export const config = {
  instanceId: `${hostname()}-${randomUUID().slice(0, 8)}`,
  logLevel: str('LOG_LEVEL', 'info'),
  baileysLogLevel: str('BAILEYS_LOG_LEVEL', 'warn'),

  databaseUrl: str('DATABASE_URL', 'postgres://zapi:zapi@localhost:5432/zapi'),
  dbPoolSize: int('DB_POOL_SIZE', 10),
  redisUrl: str('REDIS_URL', 'redis://localhost:6379'),

  api: {
    port: int('PORT', 3000),
    host: str('HOST', '0.0.0.0'),
    adminKey: str('ADMIN_API_KEY', ''),
    bodyLimit: int('API_BODY_LIMIT_BYTES', 64 * 1024 * 1024),
    rpcTimeoutMs: int('RPC_TIMEOUT_MS', 30_000),
  },

  metricsPort: int('METRICS_PORT', 9100),

  s3: {
    endpoint: str('S3_ENDPOINT', 'http://localhost:9000'),
    /** endpoint used in URLs handed out to clients (webhook payloads, redirects) */
    publicEndpoint: str('S3_PUBLIC_ENDPOINT', process.env.S3_ENDPOINT || 'http://localhost:9000'),
    accessKey: str('S3_ACCESS_KEY', 'minioadmin'),
    secretKey: str('S3_SECRET_KEY', 'minioadmin'),
    bucket: str('S3_BUCKET', 'zapi-media'),
    region: str('S3_REGION', 'us-east-1'),
    urlTtlSeconds: int('MEDIA_URL_TTL_SECONDS', 7 * 24 * 3600),
  },

  worker: {
    maxDevices: int('WORKER_MAX_DEVICES', 200),
    leaseTtlMs: int('LEASE_TTL_MS', 30_000),
    reconcileIntervalMs: int('RECONCILE_INTERVAL_MS', 5_000),
    rebalance: bool('WORKER_REBALANCE', true),
    markOnlineOnConnect: bool('MARK_ONLINE_ON_CONNECT', false),
    ignoreStatusBroadcast: bool('IGNORE_STATUS_BROADCAST', true),
  },

  media: {
    download: bool('MEDIA_DOWNLOAD', true),
    maxBytes: int('MEDIA_MAX_BYTES', 100 * 1024 * 1024),
    timeoutMs: int('MEDIA_DOWNLOAD_TIMEOUT_MS', 45_000),
  },

  presence: {
    /** chats re-subscribed after each reconnect, most recently active first */
    resubscribeLimit: int('PRESENCE_RESUBSCRIBE_LIMIT', 300),
  },

  sticker: {
    ffmpegPath: str('FFMPEG_PATH', 'ffmpeg'),
    /** videos are cut to this length when turned into a sticker */
    maxSeconds: int('STICKER_MAX_SECONDS', 8),
    fps: int('STICKER_FPS', 12),
  },

  send: {
    maxAttempts: int('SEND_MAX_ATTEMPTS', 5),
    intervalMs: int('SEND_INTERVAL_MS', 300),
    timeoutMs: int('SEND_TIMEOUT_MS', 90_000),
    /** resolve phone numbers through WhatsApp before sending; unknown numbers fail immediately */
    checkNumber: bool('SEND_CHECK_NUMBER', true),
  },

  webhook: {
    maxAttempts: int('WEBHOOK_MAX_ATTEMPTS', 12),
    timeoutMs: int('WEBHOOK_TIMEOUT_MS', 10_000),
    concurrency: int('WEBHOOK_CONCURRENCY', 20),
    backoffBaseMs: int('WEBHOOK_BACKOFF_BASE_MS', 5_000),
    backoffMaxMs: int('WEBHOOK_BACKOFF_MAX_MS', 3_600_000),
  },

  retention: {
    eventDays: int('EVENT_RETENTION_DAYS', 30),
    messageDays: int('MESSAGE_RETENTION_DAYS', 0),
  },
}

export type Config = typeof config
