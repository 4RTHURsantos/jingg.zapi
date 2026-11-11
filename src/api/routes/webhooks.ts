import { randomUUID } from 'node:crypto'
import { Type } from '@sinclair/typebox'
import { config } from '../../config.js'
import { query } from '../../db.js'
import { enqueueDeliveries, EVENT_TYPES } from '../../events.js'
import { sign } from '../../signature.js'
import { Any, DeviceParams, HttpError } from '../context.js'
import type { App } from '../server.js'

const tags = ['Webhooks']

const WebhookParams = Type.Object({ deviceId: Type.String({ format: 'uuid' }), webhookId: Type.String({ format: 'uuid' }) })
const EventList = Type.Array(Type.String({ enum: [...EVENT_TYPES] }), {
  description: 'Eventos assinados. Vazio = todos.',
})

const present = (w: any) => ({
  id: w.id,
  url: w.url,
  events: w.events,
  headers: w.headers,
  enabled: w.enabled,
  hasSecret: !!w.secret,
  createdAt: w.created_at,
  updatedAt: w.updated_at,
})

const presentDelivery = (d: any) => ({
  id: d.id,
  webhookId: d.webhook_id,
  eventId: d.event_id,
  eventType: d.type,
  eventSeq: d.seq,
  status: d.status,
  attempts: d.attempts,
  lastStatusCode: d.last_status_code,
  lastError: d.last_error,
  createdAt: d.created_at,
  updatedAt: d.updated_at,
  deliveredAt: d.delivered_at,
})

export async function webhookRoutes(app: App) {
  app.get(
    '/devices/:deviceId/webhooks',
    { schema: { tags, summary: 'Lista os webhooks do dispositivo', params: DeviceParams, response: { 200: Any } } },
    async (req) => {
      const res = await query('SELECT * FROM webhooks WHERE device_id = $1 ORDER BY created_at', [req.device.id])
      return res.rows.map(present)
    },
  )

  app.post(
    '/devices/:deviceId/webhooks',
    {
      schema: {
        tags,
        summary: 'Cadastra um webhook',
        description:
          'Cada evento vira um POST JSON `{id, seq, deviceId, type, timestamp, data}`. Responda 2xx em até ' +
          `${config.webhook.timeoutMs / 1000}s; qualquer outra coisa é reenviada com backoff exponencial (até ${config.webhook.maxAttempts} tentativas). ` +
          'Com `secret`, o header `X-Zapi-Signature` traz `sha256=<HMAC-SHA256 do corpo bruto>`.',
        params: DeviceParams,
        body: Type.Object({
          url: Type.String({ format: 'uri' }),
          secret: Type.Optional(Type.String()),
          events: Type.Optional(EventList),
          headers: Type.Optional(Type.Record(Type.String(), Type.String(), { description: 'Headers extras enviados em cada chamada' })),
          enabled: Type.Optional(Type.Boolean({ default: true })),
        }),
        response: { 201: Any },
      },
    },
    async (req, reply) => {
      const { url, secret, events, headers, enabled = true } = req.body
      if (!/^https?:\/\//i.test(url)) throw new HttpError(400, 'webhook url must be http(s)')
      const res = await query(
        'INSERT INTO webhooks (id, device_id, url, secret, events, headers, enabled) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7) RETURNING *',
        [randomUUID(), req.device.id, url, secret ?? null, events ?? [], JSON.stringify(headers ?? {}), enabled],
      )
      return reply.status(201).send(present(res.rows[0]))
    },
  )

  app.patch(
    '/devices/:deviceId/webhooks/:webhookId',
    {
      schema: {
        tags,
        summary: 'Altera um webhook',
        params: WebhookParams,
        body: Type.Object({
          url: Type.Optional(Type.String({ format: 'uri' })),
          secret: Type.Optional(Type.Union([Type.String(), Type.Null()])),
          events: Type.Optional(EventList),
          headers: Type.Optional(Type.Record(Type.String(), Type.String())),
          enabled: Type.Optional(Type.Boolean()),
        }),
        response: { 200: Any },
      },
    },
    async (req) => {
      const b = req.body
      const res = await query(
        `UPDATE webhooks SET
            url = COALESCE($3, url),
            secret = CASE WHEN $4 THEN $5 ELSE secret END,
            events = COALESCE($6, events),
            headers = COALESCE($7::jsonb, headers),
            enabled = COALESCE($8, enabled),
            updated_at = now()
          WHERE id = $1 AND device_id = $2 RETURNING *`,
        [
          req.params.webhookId,
          req.device.id,
          b.url ?? null,
          b.secret !== undefined,
          b.secret ?? null,
          b.events ?? null,
          b.headers ? JSON.stringify(b.headers) : null,
          b.enabled ?? null,
        ],
      )
      if (!res.rowCount) throw new HttpError(404, 'webhook not found')
      return present(res.rows[0])
    },
  )

  app.delete(
    '/devices/:deviceId/webhooks/:webhookId',
    {
      schema: {
        tags,
        summary: 'Remove um webhook (entregas pendentes dele são descartadas)',
        params: WebhookParams,
        response: { 204: Type.Null() },
      },
    },
    async (req, reply) => {
      const res = await query('DELETE FROM webhooks WHERE id = $1 AND device_id = $2', [req.params.webhookId, req.device.id])
      if (!res.rowCount) throw new HttpError(404, 'webhook not found')
      return reply.status(204).send(null)
    },
  )

  app.post(
    '/devices/:deviceId/webhooks/:webhookId/test',
    {
      schema: {
        tags,
        summary: 'Envia um evento de teste (`ping`) agora e devolve a resposta do seu servidor',
        params: WebhookParams,
        response: { 200: Any },
      },
    },
    async (req) => {
      const res = await query('SELECT * FROM webhooks WHERE id = $1 AND device_id = $2', [req.params.webhookId, req.device.id])
      const hook = res.rows[0]
      if (!hook) throw new HttpError(404, 'webhook not found')
      const body = JSON.stringify({
        id: randomUUID(),
        seq: 0,
        deviceId: req.device.id,
        type: 'ping',
        timestamp: new Date().toISOString(),
        data: { message: 'webhook test' },
      })
      const started = Date.now()
      try {
        const response = await fetch(hook.url, {
          method: 'POST',
          headers: {
            ...(hook.headers ?? {}),
            'content-type': 'application/json',
            'user-agent': 'jingg-zapi-webhook/1',
            'x-zapi-event': 'ping',
            'x-zapi-device-id': req.device.id,
            ...(hook.secret ? { 'x-zapi-signature': sign(hook.secret, body) } : {}),
          },
          body,
          signal: AbortSignal.timeout(config.webhook.timeoutMs),
        })
        const text = (await response.text().catch(() => '')).slice(0, 2000)
        return { ok: response.ok, statusCode: response.status, durationMs: Date.now() - started, responseBody: text }
      } catch (err: any) {
        return { ok: false, statusCode: null, durationMs: Date.now() - started, error: err?.cause?.message ?? err?.message ?? String(err) }
      }
    },
  )

  // ───────────── deliveries ─────────────

  app.get(
    '/devices/:deviceId/webhook-deliveries',
    {
      schema: {
        tags,
        summary: 'Histórico de entregas de webhook',
        params: DeviceParams,
        querystring: Type.Object({
          status: Type.Optional(Type.String({ enum: ['pending', 'delivered', 'failed'] })),
          webhookId: Type.Optional(Type.String({ format: 'uuid' })),
          limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 500, default: 100 })),
        }),
        response: { 200: Any },
      },
    },
    async (req) => {
      const { status, webhookId, limit = 100 } = req.query
      const res = await query(
        `SELECT d.*, e.type, e.seq FROM webhook_deliveries d JOIN events e ON e.id = d.event_id
          WHERE d.device_id = $1 AND ($2::text IS NULL OR d.status = $2) AND ($3::uuid IS NULL OR d.webhook_id = $3)
          ORDER BY d.created_at DESC LIMIT $4`,
        [req.device.id, status ?? null, webhookId ?? null, limit],
      )
      return res.rows.map(presentDelivery)
    },
  )

  app.post(
    '/devices/:deviceId/webhook-deliveries/:deliveryId/retry',
    {
      schema: {
        tags,
        summary: 'Reenvia uma entrega (falha definitiva ou já entregue)',
        params: Type.Object({ deviceId: Type.String({ format: 'uuid' }), deliveryId: Type.String({ format: 'uuid' }) }),
        response: { 202: Any },
      },
    },
    async (req, reply) => {
      const res = await query(
        `UPDATE webhook_deliveries SET status = 'pending', attempts = 0, last_error = NULL, updated_at = now()
          WHERE id = $1 AND device_id = $2 RETURNING id`,
        [req.params.deliveryId, req.device.id],
      )
      if (!res.rowCount) throw new HttpError(404, 'delivery not found')
      await enqueueDeliveries([req.params.deliveryId])
      return reply.status(202).send({ ok: true, requeued: 1 })
    },
  )

  app.post(
    '/devices/:deviceId/webhook-deliveries/retry-failed',
    {
      schema: {
        tags,
        summary: 'Reenvia todas as entregas com falha definitiva',
        description: 'Útil depois que o seu endpoint ficou fora do ar por mais tempo do que a janela de retry.',
        params: DeviceParams,
        response: { 202: Any },
      },
    },
    async (req, reply) => {
      const res = await query(
        `UPDATE webhook_deliveries SET status = 'pending', attempts = 0, last_error = NULL, updated_at = now()
          WHERE device_id = $1 AND status = 'failed' RETURNING id`,
        [req.device.id],
      )
      const ids = res.rows.map((r) => r.id)
      for (let i = 0; i < ids.length; i += 500) await enqueueDeliveries(ids.slice(i, i + 500))
      return reply.status(202).send({ ok: true, requeued: ids.length })
    },
  )
}
