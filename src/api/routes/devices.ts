import { randomUUID } from 'node:crypto'
import { Type } from '@sinclair/typebox'
import QRCode from 'qrcode'
import { clearAuthState } from '../../auth-state.js'
import { query, tx } from '../../db.js'
import { emitEvent, enqueueDeliveries, EVENT_TYPES } from '../../events.js'
import { keys } from '../../redis.js'
import {
  Any,
  callDevice,
  DEVICE_COLUMNS,
  DeviceParams,
  hashKey,
  HttpError,
  isRunning,
  newApiKey,
  notifyWorkers,
  presentDevice,
  redis,
  setDesiredState,
  waitReleased,
  type DeviceRow,
} from '../context.js'
import type { App } from '../server.js'

const tags = ['Dispositivos']

export async function deviceRoutes(app: App) {
  app.post(
    '/devices',
    {
      schema: {
        tags,
        summary: 'Cria um dispositivo (requer chave de administrador)',
        description: 'Devolve a `apiKey` do dispositivo — ela só é exibida aqui. Com `connect: true` (padrão) o dispositivo já começa a gerar QR code.',
        body: Type.Object({
          name: Type.String({ minLength: 1, maxLength: 120 }),
          connect: Type.Optional(Type.Boolean({ default: true })),
          webhook: Type.Optional(
            Type.Object({
              url: Type.String({ format: 'uri' }),
              secret: Type.Optional(Type.String()),
              events: Type.Optional(Type.Array(Type.String({ enum: [...EVENT_TYPES] }))),
            }),
          ),
        }),
        response: { 201: Any },
      },
    },
    async (req, reply) => {
      if (!req.auth.admin) throw new HttpError(403, 'admin key required')
      const { name, connect = true, webhook } = req.body
      const id = randomUUID()
      const apiKey = newApiKey()
      const device = await tx(async (client) => {
        const res = await client.query<DeviceRow>(
          `INSERT INTO devices (id, name, api_key_hash, desired_state) VALUES ($1, $2, $3, $4) RETURNING ${DEVICE_COLUMNS}`,
          [id, name, hashKey(apiKey), connect ? 'started' : 'stopped'],
        )
        if (webhook) {
          await client.query('INSERT INTO webhooks (id, device_id, url, secret, events) VALUES ($1, $2, $3, $4, $5)', [
            randomUUID(),
            id,
            webhook.url,
            webhook.secret ?? null,
            webhook.events ?? [],
          ])
        }
        return res.rows[0]
      })
      if (connect) await notifyWorkers()
      return reply.status(201).send({ ...presentDevice(device), apiKey })
    },
  )

  app.get('/devices', { schema: { tags, summary: 'Lista dispositivos', response: { 200: Any } } }, async (req) => {
    const res = req.auth.admin
      ? await query<DeviceRow>(`SELECT ${DEVICE_COLUMNS} FROM devices ORDER BY created_at`)
      : await query<DeviceRow>(`SELECT ${DEVICE_COLUMNS} FROM devices WHERE id = $1`, [req.auth.deviceId])
    return res.rows.map(presentDevice)
  })

  app.get(
    '/devices/:deviceId',
    { schema: { tags, summary: 'Detalhes e status do dispositivo', params: DeviceParams, response: { 200: Any } } },
    async (req) => {
      const queue = await query(
        `SELECT count(*) FILTER (WHERE status IN ('queued', 'sending'))::int AS queued,
                count(*) FILTER (WHERE status = 'failed')::int AS failed
           FROM outbound_messages WHERE device_id = $1`,
        [req.device.id],
      )
      const deliveries = await query(
        `SELECT count(*) FILTER (WHERE status = 'pending')::int AS pending,
                count(*) FILTER (WHERE status = 'failed')::int AS failed
           FROM webhook_deliveries WHERE device_id = $1`,
        [req.device.id],
      )
      return {
        ...presentDevice(req.device),
        running: await isRunning(req.device.id),
        outbound: queue.rows[0],
        webhookDeliveries: deliveries.rows[0],
      }
    },
  )

  app.patch(
    '/devices/:deviceId',
    {
      schema: {
        tags,
        summary: 'Renomeia o dispositivo',
        params: DeviceParams,
        body: Type.Object({ name: Type.String({ minLength: 1, maxLength: 120 }) }),
        response: { 200: Any },
      },
    },
    async (req) => {
      const res = await query<DeviceRow>(`UPDATE devices SET name = $2, updated_at = now() WHERE id = $1 RETURNING ${DEVICE_COLUMNS}`, [
        req.device.id,
        req.body.name,
      ])
      return presentDevice(res.rows[0])
    },
  )

  app.delete(
    '/devices/:deviceId',
    {
      schema: {
        tags,
        summary: 'Remove o dispositivo e todos os seus dados (requer chave de administrador)',
        description: 'Se estiver conectado, a sessão é deslogada do WhatsApp antes da remoção.',
        params: DeviceParams,
        response: { 204: Type.Null() },
      },
    },
    async (req, reply) => {
      if (!req.auth.admin) throw new HttpError(403, 'admin key required')
      if (await isRunning(req.device.id)) {
        await callDevice(req.device.id, 'logout').catch(() => {})
        await setDesiredState(req.device.id, 'stopped')
        await waitReleased(req.device.id, 10_000)
      }
      await query('DELETE FROM devices WHERE id = $1', [req.device.id])
      await redis.del(keys.qr(req.device.id))
      await notifyWorkers()
      return reply.status(204).send(null)
    },
  )

  app.post(
    '/devices/:deviceId/connect',
    {
      schema: {
        tags,
        summary: 'Inicia a conexão',
        description: 'Sem sessão salva, passa a gerar QR codes (`GET /qr`) ou aceita código de pareamento (`POST /pairing-code`).',
        params: DeviceParams,
        response: { 200: Any },
      },
    },
    async (req) => {
      await setDesiredState(req.device.id, 'started')
      return { ...presentDevice(req.device), desiredState: 'started' }
    },
  )

  app.post(
    '/devices/:deviceId/disconnect',
    {
      schema: {
        tags,
        summary: 'Fecha a conexão mantendo a sessão',
        description: 'O dispositivo continua pareado; `POST /connect` reconecta sem novo QR. Mensagens enfileiradas aguardam a reconexão.',
        params: DeviceParams,
        response: { 200: Any },
      },
    },
    async (req) => {
      await setDesiredState(req.device.id, 'stopped')
      return { ...presentDevice(req.device), desiredState: 'stopped' }
    },
  )

  app.post(
    '/devices/:deviceId/restart',
    { schema: { tags, summary: 'Reinicia a conexão', params: DeviceParams, response: { 200: Any } } },
    async (req) => {
      await setDesiredState(req.device.id, 'stopped')
      await waitReleased(req.device.id)
      await setDesiredState(req.device.id, 'started')
      return { ...presentDevice(req.device), desiredState: 'started' }
    },
  )

  app.post(
    '/devices/:deviceId/logout',
    {
      schema: {
        tags,
        summary: 'Desloga do WhatsApp e apaga a sessão',
        description: 'Depois disso é preciso parear de novo (QR ou código).',
        params: DeviceParams,
        response: { 200: Any },
      },
    },
    async (req) => {
      const id = req.device.id
      // connected: a real logout unlinks the device on the phone too, and the worker cleans up
      const loggedOut = (await isRunning(id)) && (await callDevice(id, 'logout').then(() => true, () => false))
      if (loggedOut) {
        await waitReleased(id, 10_000)
      } else {
        await setDesiredState(id, 'stopped')
        await waitReleased(id, 10_000)
        await clearAuthState(id)
        const emitted = await tx(async (client) => {
          await client.query(
            `UPDATE devices SET status = 'logged_out', status_reason = 'logged_out', worker_id = NULL, updated_at = now() WHERE id = $1`,
            [id],
          )
          return emitEvent(client, id, 'connection.update', { status: 'logged_out', reason: 'logged_out' })
        })
        await enqueueDeliveries(emitted.deliveryIds)
      }
      await redis.del(keys.qr(id))
      return { ok: true }
    },
  )

  app.post(
    '/devices/:deviceId/rotate-key',
    {
      schema: {
        tags,
        summary: 'Gera uma nova chave para o dispositivo (a anterior deixa de valer)',
        params: DeviceParams,
        response: { 200: Type.Object({ apiKey: Type.String() }) },
      },
    },
    async (req) => {
      const apiKey = newApiKey()
      await query('UPDATE devices SET api_key_hash = $2, updated_at = now() WHERE id = $1', [req.device.id, hashKey(apiKey)])
      return { apiKey }
    },
  )

  app.get(
    '/devices/:deviceId/qr',
    {
      schema: {
        tags,
        summary: 'QR code atual para pareamento',
        description:
          'JSON com o conteúdo do QR e uma imagem em data URL. Com `?format=png` devolve a imagem diretamente. O QR muda a cada ~20-60s; também chega pelo evento `qr.updated`.',
        params: DeviceParams,
        querystring: Type.Object({ format: Type.Optional(Type.String({ enum: ['json', 'png'] })) }),
        response: { 200: Any },
      },
    },
    async (req, reply) => {
      const raw = await redis.get(keys.qr(req.device.id))
      if (!raw) {
        if (req.device.status === 'connected') throw new HttpError(409, 'device is already connected')
        if (req.device.desired_state !== 'started') throw new HttpError(409, 'device is stopped; call POST /connect first')
        throw new HttpError(404, 'no QR code available yet; retry in a moment')
      }
      const { qr, at } = JSON.parse(raw) as { qr: string; at: number }
      if (req.query.format === 'png') {
        return reply.header('content-type', 'image/png').header('cache-control', 'no-store').send(await QRCode.toBuffer(qr, { width: 400, margin: 2 }))
      }
      return { qr, image: await QRCode.toDataURL(qr, { width: 400, margin: 2 }), generatedAt: new Date(at).toISOString() }
    },
  )

  app.post(
    '/devices/:deviceId/pairing-code',
    {
      schema: {
        tags,
        summary: 'Código de pareamento (alternativa ao QR)',
        description: 'Informe o número do WhatsApp que será conectado. No celular: Dispositivos conectados → Conectar com número de telefone.',
        params: DeviceParams,
        body: Type.Object({ phone: Type.String({ minLength: 8, description: 'Número com DDI, ex. 5511999990000' }) }),
        response: { 200: Type.Object({ code: Type.String() }) },
      },
    },
    async (req) => callDevice<{ code: string }>(req.device.id, 'pairingCode', req.body),
  )

  app.patch(
    '/devices/:deviceId/profile',
    {
      schema: {
        tags,
        summary: 'Atualiza nome e/ou recado do perfil do WhatsApp',
        params: DeviceParams,
        body: Type.Object({ name: Type.Optional(Type.String()), status: Type.Optional(Type.String()) }),
        response: { 200: Any },
      },
    },
    async (req) => callDevice(req.device.id, 'updateProfile', req.body),
  )
}
