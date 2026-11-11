import { randomUUID } from 'node:crypto'
import { Queue } from 'bullmq'
import { config } from './config.js'
import { tx, toJsonb, type Queryable } from './db.js'
import { logger } from './logger.js'
import { metrics } from './metrics.js'
import { createRedis } from './redis.js'

export const EVENT_TYPES = [
  'connection.update',
  'qr.updated',
  'message.received',
  'message.sent',
  'message.status',
  'message.receipt',
  'message.edited',
  'message.deleted',
  'message.reaction',
  'message.poll_vote',
  'message.failed',
  'group.updated',
  'group.participants',
  'group.join_request',
  'call.update',
  'presence.update',
] as const

export type EventType = (typeof EVENT_TYPES)[number]

export const WEBHOOK_QUEUE = 'webhooks'
export const QUEUE_PREFIX = 'zapi:bull'

let queue: Queue | undefined
export function webhookQueue(): Queue {
  queue ??= new Queue(WEBHOOK_QUEUE, {
    connection: createRedis('bullmq-queue'),
    prefix: QUEUE_PREFIX,
    defaultJobOptions: {
      attempts: config.webhook.maxAttempts,
      backoff: { type: 'custom' },
      // the delivery row in Postgres is the durable record; finished jobs free their id for replays
      removeOnComplete: true,
      removeOnFail: true,
    },
  })
  return queue
}

export interface EmittedEvent {
  id: string
  seq: number
  deliveryIds: string[]
}

/**
 * Transactional outbox: must run inside the same transaction as the state change it describes.
 * Writes the event and one pending delivery per subscribed webhook. Nothing reaches Redis here,
 * so a commit means the event will eventually be delivered even if Redis is down right now.
 */
export async function emitEvent(
  client: Queryable,
  deviceId: string,
  type: EventType,
  data: unknown,
): Promise<EmittedEvent> {
  // the row lock on the device serializes its events: seq order == commit order
  const seqRes = await client.query(
    'UPDATE devices SET event_seq = event_seq + 1 WHERE id = $1 RETURNING event_seq',
    [deviceId],
  )
  if (!seqRes.rowCount) throw new Error(`device ${deviceId} not found`)
  const seq: number = seqRes.rows[0].event_seq
  const id = randomUUID()
  await client.query('INSERT INTO events (id, device_id, seq, type, payload) VALUES ($1, $2, $3, $4, $5::jsonb)', [
    id,
    deviceId,
    seq,
    type,
    toJsonb(data),
  ])
  const deliveries = await client.query(
    `INSERT INTO webhook_deliveries (id, event_id, webhook_id, device_id)
     SELECT gen_random_uuid(), $1, w.id, w.device_id
       FROM webhooks w
      WHERE w.device_id = $2 AND w.enabled AND (cardinality(w.events) = 0 OR $3 = ANY (w.events))
     RETURNING id`,
    [id, deviceId, type],
  )
  metrics.eventsEmitted.inc({ type })
  return { id, seq, deliveryIds: deliveries.rows.map((r) => r.id) }
}

/**
 * Hands committed deliveries to the dispatcher. Best effort by design:
 * the dispatcher's reconciler re-enqueues any pending delivery that never made it to Redis.
 */
export async function enqueueDeliveries(deliveryIds: string[]): Promise<void> {
  if (!deliveryIds.length) return
  try {
    await webhookQueue().addBulk(
      deliveryIds.map((id) => ({ name: 'deliver', data: { deliveryId: id }, opts: { jobId: id } })),
    )
  } catch (err) {
    logger.warn({ err, count: deliveryIds.length }, 'could not enqueue deliveries now; reconciler will pick them up')
  }
}

/** Emits a standalone event (own transaction) and dispatches it. */
export async function emitAndDispatch(deviceId: string, type: EventType, data: unknown): Promise<EmittedEvent> {
  const emitted = await tx((client) => emitEvent(client, deviceId, type, data))
  await enqueueDeliveries(emitted.deliveryIds)
  return emitted
}
