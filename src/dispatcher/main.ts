import { Worker, type Job } from 'bullmq'
import { config } from '../config.js'
import { pool, query } from '../db.js'
import { QUEUE_PREFIX, WEBHOOK_QUEUE, webhookQueue } from '../events.js'
import { logger } from '../logger.js'
import { metrics, startMetricsServer } from '../metrics.js'
import { createRedis } from '../redis.js'
import { sign } from '../signature.js'

/**
 * Delivers webhooks at-least-once.
 *
 * Postgres (webhook_deliveries) is the durable record; BullMQ only schedules attempts.
 * If Redis loses its data, the reconciler below rebuilds the queue from the pending rows.
 */

async function deliver(job: Job<{ deliveryId: string }>): Promise<void> {
  const { deliveryId } = job.data
  const res = await query(
    `SELECT d.status, d.attempts, e.id AS event_id, e.seq, e.type, e.payload, e.created_at, e.device_id,
            w.url, w.secret, w.headers, w.enabled
       FROM webhook_deliveries d
       JOIN events e ON e.id = d.event_id
       JOIN webhooks w ON w.id = d.webhook_id
      WHERE d.id = $1`,
    [deliveryId],
  )
  const row = res.rows[0]
  if (!row || row.status !== 'pending') return // webhook/device deleted, or already delivered

  if (!row.enabled) {
    await query(
      `UPDATE webhook_deliveries SET status = 'failed', last_error = 'webhook disabled', updated_at = now() WHERE id = $1`,
      [deliveryId],
    )
    return
  }

  const attempt = row.attempts + 1
  const body = JSON.stringify({
    id: row.event_id,
    seq: row.seq,
    deviceId: row.device_id,
    type: row.type,
    timestamp: row.created_at,
    data: row.payload,
  })
  const headers: Record<string, string> = {
    ...(row.headers ?? {}),
    'content-type': 'application/json',
    'user-agent': 'jingg-zapi-webhook/1',
    'x-zapi-event': row.type,
    'x-zapi-event-id': row.event_id,
    'x-zapi-delivery-id': deliveryId,
    'x-zapi-device-id': row.device_id,
    'x-zapi-attempt': String(attempt),
  }
  if (row.secret) headers['x-zapi-signature'] = sign(row.secret, body)

  let statusCode: number | null = null
  let error: string | null = null
  const stop = metrics.webhookDuration.startTimer()
  try {
    const response = await fetch(row.url, {
      method: 'POST',
      headers,
      body,
      signal: AbortSignal.timeout(config.webhook.timeoutMs),
      redirect: 'follow',
    })
    statusCode = response.status
    // drain so the connection can be reused
    await response.arrayBuffer().catch(() => {})
    if (!response.ok) error = `HTTP ${response.status}`
  } catch (err: any) {
    error = err?.name === 'TimeoutError' ? `timeout after ${config.webhook.timeoutMs}ms` : (err?.cause?.message ?? err?.message ?? String(err))
  } finally {
    stop()
  }

  if (!error) {
    await query(
      `UPDATE webhook_deliveries
          SET status = 'delivered', attempts = $2, last_status_code = $3, last_error = NULL, delivered_at = now(), updated_at = now()
        WHERE id = $1`,
      [deliveryId, attempt, statusCode],
    )
    metrics.webhookDeliveries.inc({ outcome: 'delivered' })
    return
  }

  const final = job.attemptsMade + 1 >= (job.opts.attempts ?? 1)
  await query(
    `UPDATE webhook_deliveries
        SET status = $5, attempts = $2, last_status_code = $3, last_error = $4, updated_at = now()
      WHERE id = $1`,
    [deliveryId, attempt, statusCode, error, final ? 'failed' : 'pending'],
  )
  metrics.webhookDeliveries.inc({ outcome: final ? 'failed' : 'retry' })
  throw new Error(error) // lets BullMQ schedule the next attempt
}

/** Re-enqueues pending deliveries that have no job (enqueue failed after commit, or Redis lost data). */
async function reconcile(): Promise<void> {
  const queue = webhookQueue()
  let cursor = '00000000-0000-0000-0000-000000000000'
  let requeued = 0
  for (let page = 0; page < 50; page++) {
    const res = await query<{ id: string }>(
      `SELECT id FROM webhook_deliveries
        WHERE status = 'pending' AND created_at < now() - interval '20 seconds' AND id > $1
        ORDER BY id LIMIT 500`,
      [cursor],
    )
    if (!res.rowCount) break
    for (const { id } of res.rows) {
      if (await queue.getJob(id)) continue
      await queue.add('deliver', { deliveryId: id }, { jobId: id })
      requeued++
    }
    cursor = res.rows[res.rows.length - 1].id
    if (res.rowCount < 500) break
  }
  if (requeued) logger.warn({ requeued }, 'reconciler re-enqueued orphaned deliveries')

  const pending = await query(`SELECT count(*)::int AS n FROM webhook_deliveries WHERE status = 'pending'`)
  metrics.webhookPending.set(pending.rows[0].n)
}

async function cleanup(): Promise<void> {
  if (config.retention.eventDays > 0) {
    // events still owed to a webhook are kept regardless of age
    const events = await query(
      `DELETE FROM events e
        WHERE e.created_at < now() - make_interval(days => $1)
          AND NOT EXISTS (SELECT 1 FROM webhook_deliveries d WHERE d.event_id = e.id AND d.status = 'pending')`,
      [config.retention.eventDays],
    )
    const outbound = await query(
      `DELETE FROM outbound_messages
        WHERE status IN ('sent', 'failed', 'cancelled') AND created_at < now() - make_interval(days => $1)`,
      [config.retention.eventDays],
    )
    if (events.rowCount || outbound.rowCount) logger.info({ events: events.rowCount, outbound: outbound.rowCount }, 'retention cleanup')
  }
  if (config.retention.messageDays > 0) {
    await query(`DELETE FROM messages WHERE NOT pending_event AND created_at < now() - make_interval(days => $1)`, [
      config.retention.messageDays,
    ])
  }
}

const connection = createRedis('bullmq-worker')
const worker = new Worker(WEBHOOK_QUEUE, deliver, {
  connection,
  prefix: QUEUE_PREFIX,
  concurrency: config.webhook.concurrency,
  settings: {
    backoffStrategy: (attemptsMade: number) =>
      Math.min(config.webhook.backoffBaseMs * 2 ** Math.max(attemptsMade - 1, 0), config.webhook.backoffMaxMs),
  },
})
worker.on('error', (err) => logger.error({ err }, 'webhook worker error'))

const tick = (fn: () => Promise<void>, name: string) => () => fn().catch((err) => logger.error({ err }, `${name} failed`))
const timers = [setInterval(tick(reconcile, 'reconcile'), 30_000), setInterval(tick(cleanup, 'cleanup'), 3600_000)]
void tick(reconcile, 'reconcile')()
void tick(cleanup, 'cleanup')()

const metricsServer = startMetricsServer(config.metricsPort, async () => {
  await pool.query('SELECT 1')
  return connection.status === 'ready'
})
logger.info({ concurrency: config.webhook.concurrency }, 'webhook dispatcher started')

let closing = false
const shutdown = async (signal: string) => {
  if (closing) return
  closing = true
  logger.info({ signal }, 'dispatcher shutting down')
  timers.forEach(clearInterval)
  await worker.close() // waits for in-flight deliveries
  await webhookQueue().close()
  metricsServer.close()
  await pool.end().catch(() => {})
  process.exit(0)
}
process.on('SIGTERM', () => void shutdown('SIGTERM'))
process.on('SIGINT', () => void shutdown('SIGINT'))
process.on('unhandledRejection', (err) => logger.error({ err }, 'unhandled rejection'))
