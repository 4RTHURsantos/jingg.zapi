import { createServer, type Server } from 'node:http'
import client from 'prom-client'
import { logger } from './logger.js'

export const registry = new client.Registry()
client.collectDefaultMetrics({ register: registry, prefix: 'zapi_' })

export const metrics = {
  httpDuration: new client.Histogram({
    name: 'zapi_http_request_duration_seconds',
    help: 'API request duration',
    labelNames: ['method', 'route', 'status'],
    buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30],
    registers: [registry],
  }),
  sessions: new client.Gauge({
    name: 'zapi_worker_sessions',
    help: 'WhatsApp sessions held by this worker, by connection state',
    labelNames: ['state'],
    registers: [registry],
  }),
  messagesReceived: new client.Counter({
    name: 'zapi_messages_received_total',
    help: 'Messages stored from WhatsApp (received or sent from another device)',
    labelNames: ['type'],
    registers: [registry],
  }),
  messagesSent: new client.Counter({
    name: 'zapi_messages_sent_total',
    help: 'Outbound messages handed to WhatsApp',
    labelNames: ['type'],
    registers: [registry],
  }),
  messagesFailed: new client.Counter({
    name: 'zapi_messages_failed_total',
    help: 'Outbound messages that exhausted their attempts',
    registers: [registry],
  }),
  sendAttemptsFailed: new client.Counter({
    name: 'zapi_send_attempts_failed_total',
    help: 'Individual send attempts that failed (will be retried)',
    registers: [registry],
  }),
  mediaDownloads: new client.Counter({
    name: 'zapi_media_downloads_total',
    help: 'Inbound media downloads',
    labelNames: ['result'],
    registers: [registry],
  }),
  eventsEmitted: new client.Counter({
    name: 'zapi_events_emitted_total',
    help: 'Events written to the outbox',
    labelNames: ['type'],
    registers: [registry],
  }),
  reconnects: new client.Counter({
    name: 'zapi_session_reconnects_total',
    help: 'Socket closures by reason code',
    labelNames: ['code'],
    registers: [registry],
  }),
  webhookDeliveries: new client.Counter({
    name: 'zapi_webhook_deliveries_total',
    help: 'Webhook delivery attempts by outcome',
    labelNames: ['outcome'],
    registers: [registry],
  }),
  webhookDuration: new client.Histogram({
    name: 'zapi_webhook_duration_seconds',
    help: 'Webhook HTTP call duration',
    buckets: [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
    registers: [registry],
  }),
  webhookPending: new client.Gauge({
    name: 'zapi_webhook_deliveries_pending',
    help: 'Deliveries still pending in Postgres',
    registers: [registry],
  }),
}

/** Minimal /metrics + /health endpoint for processes that have no HTTP API of their own. */
export function startMetricsServer(port: number, healthy: () => Promise<boolean> | boolean): Server {
  const server = createServer(async (req, res) => {
    try {
      if (req.url === '/metrics') {
        res.writeHead(200, { 'content-type': registry.contentType })
        res.end(await registry.metrics())
      } else if (req.url === '/health') {
        const ok = await healthy()
        res.writeHead(ok ? 200 : 503, { 'content-type': 'application/json' })
        res.end(JSON.stringify({ status: ok ? 'ok' : 'unhealthy' }))
      } else {
        res.writeHead(404).end()
      }
    } catch (err) {
      res.writeHead(500).end()
    }
  })
  server.listen(port, () => logger.info({ port }, 'metrics server listening'))
  return server
}
