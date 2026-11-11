import { createHash, randomBytes, timingSafeEqual } from 'node:crypto'
import { Type, type TSchema } from '@sinclair/typebox'
import { config } from '../config.js'
import { query } from '../db.js'
import { channels, createRedis, keys } from '../redis.js'
import { RpcClient, RpcError } from '../rpc.js'

export const redis = createRedis('api')
const rpcSub = createRedis('api-rpc')
export const rpc = new RpcClient(redis, rpcSub)

export class HttpError extends Error {
  constructor(
    public statusCode: number,
    message: string,
  ) {
    super(message)
  }
}

export interface DeviceRow {
  id: string
  name: string
  desired_state: 'started' | 'stopped'
  status: string
  status_reason: string | null
  wa_jid: string | null
  wa_lid: string | null
  wa_name: string | null
  worker_id: string | null
  event_seq: number
  created_at: Date
  updated_at: Date
  connected_at: Date | null
  settings: { presenceEvents?: boolean }
}

export const DEVICE_COLUMNS =
  'id, name, desired_state, status, status_reason, wa_jid, wa_lid, wa_name, worker_id, event_seq, created_at, updated_at, connected_at, settings'

export function presentDevice(d: DeviceRow) {
  return {
    id: d.id,
    name: d.name,
    desiredState: d.desired_state,
    status: d.status,
    statusReason: d.status_reason,
    jid: d.wa_jid,
    lid: d.wa_lid,
    phone: d.wa_jid?.split('@')[0]?.split(':')[0] ?? null,
    profileName: d.wa_name,
    workerId: d.worker_id,
    lastEventSeq: d.event_seq,
    createdAt: d.created_at,
    updatedAt: d.updated_at,
    connectedAt: d.connected_at,
    settings: { presenceEvents: false, ...d.settings },
  }
}

export const hashKey = (key: string) => createHash('sha256').update(key).digest('hex')
export const newApiKey = () => `zk_${randomBytes(32).toString('base64url')}`

export function isAdminKey(key: string): boolean {
  const a = Buffer.from(hashKey(key))
  const b = Buffer.from(hashKey(config.api.adminKey))
  return timingSafeEqual(a, b)
}

/** Tells every worker that the set of devices that should run has changed. */
export const notifyWorkers = () => redis.publish(channels.control, 'changed').catch(() => 0)

export const isRunning = async (deviceId: string) => !!(await redis.get(keys.lease(deviceId)))

/** Waits until no worker holds the device (used by restart/logout). */
export async function waitReleased(deviceId: string, timeoutMs = 15_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (!(await isRunning(deviceId))) return true
    await new Promise((r) => setTimeout(r, 250))
  }
  return false
}

/** Runs a command on the worker that owns the device; fails fast when nobody does. */
export async function callDevice<T = unknown>(deviceId: string, method: string, params: unknown = {}): Promise<T> {
  if (!(await isRunning(deviceId))) {
    throw new HttpError(409, 'device is not running; start it with POST /v1/devices/{deviceId}/connect')
  }
  try {
    return await rpc.call<T>(deviceId, method, params)
  } catch (err) {
    if (err instanceof RpcError) throw new HttpError(err.statusCode, err.message)
    throw err
  }
}

export async function setDesiredState(deviceId: string, state: 'started' | 'stopped'): Promise<void> {
  await query('UPDATE devices SET desired_state = $2, updated_at = now() WHERE id = $1', [deviceId, state])
  await notifyWorkers()
}

// ───────────── shared schemas ─────────────

export const DeviceParams = Type.Object({ deviceId: Type.String({ format: 'uuid' }) })
export const ErrorSchema = Type.Object({ statusCode: Type.Integer(), error: Type.String(), message: Type.String() })
export const Any = Type.Any()
export const Nullable = <T extends TSchema>(schema: T) => Type.Union([schema, Type.Null()])
export const OkSchema = Type.Object({ ok: Type.Boolean() })
