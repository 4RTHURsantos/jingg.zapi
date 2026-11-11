import { randomUUID } from 'node:crypto'
import { Type } from '@sinclair/typebox'
import { query } from '../../db.js'
import { toJid } from '../../jid.js'
import { channels } from '../../redis.js'
import { presignedUrl, putObject } from '../../storage.js'
import { extensionFor } from '../../worker/normalize.js'
import type { OutboundContent } from '../../worker/outbound.js'
import { Any, callDevice, DeviceParams, HttpError, redis } from '../context.js'
import type { App } from '../server.js'

const tags = ['Mensagens']

const MESSAGE_TYPES = [
  'text',
  'image',
  'video',
  'audio',
  'document',
  'sticker',
  'location',
  'contact',
  'poll',
  'reaction',
  'edit',
  'delete',
] as const
const MEDIA_TYPES = new Set(['image', 'video', 'audio', 'document', 'sticker'])

const SendBody = Type.Object({
  to: Type.String({ minLength: 3, description: 'Número com DDI (5511999990000) ou JID (…@s.whatsapp.net, …@g.us, …@lid)' }),
  type: Type.String({ enum: [...MESSAGE_TYPES], default: 'text' }),
  text: Type.Optional(Type.String({ description: 'text, edit' })),
  media: Type.Optional(
    Type.Object(
      {
        url: Type.Optional(Type.String({ format: 'uri', description: 'URL pública de onde o worker baixa o arquivo' })),
        base64: Type.Optional(Type.String({ description: 'Conteúdo em base64 (aceita prefixo data:…;base64,)' })),
      },
      {
        description:
          'image, video, audio, document, sticker — informe url OU base64. ' +
          'Para sticker aceita PNG, JPEG, WebP, GIF ou vídeo curto: a API converte para WebP 512×512 (animado quando a origem é).',
      },
    ),
  ),
  caption: Type.Optional(Type.String({ description: 'image, video, document' })),
  mimetype: Type.Optional(Type.String()),
  fileName: Type.Optional(Type.String({ description: 'document' })),
  ptt: Type.Optional(Type.Boolean({ description: 'audio: enviar como mensagem de voz (use ogg/opus)' })),
  gif: Type.Optional(Type.Boolean({ description: 'video: reproduzir como GIF' })),
  viewOnce: Type.Optional(Type.Boolean({ description: 'image, video, audio: visualização única' })),
  latitude: Type.Optional(Type.Number({ description: 'location' })),
  longitude: Type.Optional(Type.Number({ description: 'location' })),
  name: Type.Optional(Type.String({ description: 'location: nome do local' })),
  address: Type.Optional(Type.String({ description: 'location' })),
  contacts: Type.Optional(
    Type.Array(
      Type.Object({ name: Type.String(), phone: Type.String(), organization: Type.Optional(Type.String()) }),
      { minItems: 1, description: 'contact' },
    ),
  ),
  poll: Type.Optional(
    Type.Object({
      name: Type.String(),
      options: Type.Array(Type.String(), { minItems: 2, maxItems: 12 }),
      selectableCount: Type.Optional(Type.Integer({ minimum: 0, default: 1 })),
    }),
  ),
  sticker: Type.Optional(
    Type.Object(
      {
        pack: Type.Optional(Type.String({ maxLength: 128, description: 'Nome do pacote' })),
        author: Type.Optional(Type.String({ maxLength: 128 })),
        emojis: Type.Optional(Type.Array(Type.String(), { maxItems: 3, description: 'Emojis associados, usados na busca de figurinhas' })),
      },
      { description: 'sticker: dados exibidos quando o destinatário toca na figurinha' },
    ),
  ),
  messageId: Type.Optional(Type.String({ description: 'reaction, edit, delete: id da mensagem alvo' })),
  emoji: Type.Optional(Type.String({ description: 'reaction: emoji; string vazia remove a reação' })),
  quotedMessageId: Type.Optional(Type.String({ description: 'Responde citando esta mensagem' })),
  mentions: Type.Optional(Type.Array(Type.String(), { description: 'Números/JIDs mencionados (inclua @numero no texto)' })),
  mentionAll: Type.Optional(Type.Boolean({ description: 'Grupos: menciona todos os participantes' })),
  idempotencyKey: Type.Optional(
    Type.String({ maxLength: 200, description: 'Repetir a chamada com a mesma chave devolve a mensagem original em vez de enviar de novo. Também aceito no header Idempotency-Key.' }),
  ),
})

const presentOutbound = (r: any) => ({
  id: r.id,
  to: r.to_jid,
  type: r.type,
  status: r.status,
  ack: r.ack,
  messageId: r.wa_message_id,
  attempts: r.attempts,
  lastError: r.last_error,
  idempotencyKey: r.idempotency_key,
  createdAt: r.created_at,
  sentAt: r.sent_at,
  nextAttemptAt: r.status === 'queued' ? r.next_attempt_at : null,
})

function validate(type: string, b: any): void {
  const need = (cond: unknown, message: string) => {
    if (!cond) throw new HttpError(400, `${type}: ${message}`)
  }
  if (MEDIA_TYPES.has(type)) need(b.media?.url || b.media?.base64, 'media.url or media.base64 is required')
  switch (type) {
    case 'text':
      return need(b.text, 'text is required')
    case 'location':
      return need(typeof b.latitude === 'number' && typeof b.longitude === 'number', 'latitude and longitude are required')
    case 'contact':
      return need(b.contacts?.length, 'contacts is required')
    case 'poll':
      return need(b.poll, 'poll is required')
    case 'reaction':
      return need(b.messageId && typeof b.emoji === 'string', 'messageId and emoji are required')
    case 'edit':
      return need(b.messageId && b.text, 'messageId and text are required')
    case 'delete':
      return need(b.messageId, 'messageId is required')
  }
}

export async function messageRoutes(app: App) {
  app.post(
    '/devices/:deviceId/messages',
    {
      schema: {
        tags,
        summary: 'Enfileira uma mensagem para envio',
        description:
          'A mensagem é gravada no Postgres e a resposta (202) só sai depois disso — a partir daí ela não se perde. ' +
          'O envio é assíncrono e em ordem por dispositivo; se o dispositivo estiver offline, aguarda a reconexão. ' +
          'Acompanhe por `GET /messages/outbound/{id}` ou pelos eventos `message.sent`, `message.status` e `message.failed`.',
        params: DeviceParams,
        body: SendBody,
        response: { 202: Any, 200: Any },
      },
    },
    async (req, reply) => {
      const b = req.body
      const type = b.type ?? 'text'
      validate(type, b)
      let to: string
      try {
        to = toJid(b.to)
      } catch (err: any) {
        throw new HttpError(400, err.message)
      }
      const headerKey = req.headers['idempotency-key']
      const idempotencyKey = b.idempotencyKey ?? (Array.isArray(headerKey) ? headerKey[0] : headerKey) ?? null

      if (idempotencyKey) {
        const existing = await query('SELECT * FROM outbound_messages WHERE device_id = $1 AND idempotency_key = $2', [
          req.device.id,
          idempotencyKey,
        ])
        if (existing.rowCount) return reply.status(200).send({ ...presentOutbound(existing.rows[0]), duplicate: true })
      }

      const id = randomUUID()
      const { media, idempotencyKey: _k, to: _to, type: _type, ...rest } = b
      const content: OutboundContent = { ...rest }
      if (media?.base64) {
        // inline payloads go to object storage so the queue row stays small
        const match = media.base64.match(/^data:([^;,]+)[^,]*,(.*)$/s)
        const mimetype = b.mimetype ?? match?.[1]
        const buffer = Buffer.from(match ? match[2] : media.base64, 'base64')
        if (!buffer.length) throw new HttpError(400, 'media.base64 is not valid base64')
        const key = `outbound/${req.device.id}/${id}.${extensionFor(mimetype, b.fileName)}`
        await putObject(key, buffer, mimetype?.split(';')[0], buffer.length)
        content.media = { key }
        if (mimetype && !content.mimetype) content.mimetype = mimetype
      } else if (media?.url) {
        content.media = { url: media.url }
      }

      const inserted = await query(
        `INSERT INTO outbound_messages (id, device_id, to_jid, type, content, idempotency_key)
         VALUES ($1, $2, $3, $4, $5::jsonb, $6)
         ON CONFLICT (device_id, idempotency_key) DO NOTHING
         RETURNING *`,
        [id, req.device.id, to, type, JSON.stringify(content), idempotencyKey],
      )
      if (!inserted.rowCount) {
        // lost a race against a concurrent request with the same idempotency key
        const existing = await query('SELECT * FROM outbound_messages WHERE device_id = $1 AND idempotency_key = $2', [
          req.device.id,
          idempotencyKey,
        ])
        return reply.status(200).send({ ...presentOutbound(existing.rows[0]), duplicate: true })
      }
      // only a nudge: the worker also polls, so a lost publish just adds a few seconds
      await redis.publish(channels.wake, req.device.id).catch(() => 0)
      return reply.status(202).send({ ...presentOutbound(inserted.rows[0]), deviceStatus: req.device.status })
    },
  )

  app.get(
    '/devices/:deviceId/messages/outbound',
    {
      schema: {
        tags,
        summary: 'Fila de envio e histórico de mensagens enviadas pela API',
        params: DeviceParams,
        querystring: Type.Object({
          status: Type.Optional(Type.String({ enum: ['queued', 'sending', 'sent', 'failed', 'cancelled'] })),
          limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 500, default: 100 })),
        }),
        response: { 200: Any },
      },
    },
    async (req) => {
      const res = await query(
        `SELECT * FROM outbound_messages WHERE device_id = $1 AND ($2::text IS NULL OR status = $2) ORDER BY seq DESC LIMIT $3`,
        [req.device.id, req.query.status ?? null, req.query.limit ?? 100],
      )
      return res.rows.map(presentOutbound)
    },
  )

  const OutboundParams = Type.Object({ deviceId: Type.String({ format: 'uuid' }), outboundId: Type.String({ format: 'uuid' }) })

  app.get(
    '/devices/:deviceId/messages/outbound/:outboundId',
    { schema: { tags, summary: 'Status de uma mensagem enviada pela API', params: OutboundParams, response: { 200: Any } } },
    async (req) => {
      const res = await query('SELECT * FROM outbound_messages WHERE id = $1 AND device_id = $2', [req.params.outboundId, req.device.id])
      if (!res.rowCount) throw new HttpError(404, 'outbound message not found')
      return presentOutbound(res.rows[0])
    },
  )

  app.delete(
    '/devices/:deviceId/messages/outbound/:outboundId',
    {
      schema: {
        tags,
        summary: 'Cancela uma mensagem que ainda está na fila',
        params: OutboundParams,
        response: { 200: Any },
      },
    },
    async (req) => {
      const res = await query(
        `UPDATE outbound_messages SET status = 'cancelled' WHERE id = $1 AND device_id = $2 AND status = 'queued' RETURNING *`,
        [req.params.outboundId, req.device.id],
      )
      if (!res.rowCount) throw new HttpError(409, 'message not found or no longer queued')
      return presentOutbound(res.rows[0])
    },
  )

  app.post(
    '/devices/:deviceId/messages/outbound/:outboundId/retry',
    {
      schema: { tags, summary: 'Recoloca na fila uma mensagem que falhou', params: OutboundParams, response: { 202: Any } },
    },
    async (req, reply) => {
      const res = await query(
        `UPDATE outbound_messages SET status = 'queued', attempts = 0, next_attempt_at = now(), last_error = NULL
          WHERE id = $1 AND device_id = $2 AND status IN ('failed', 'cancelled') RETURNING *`,
        [req.params.outboundId, req.device.id],
      )
      if (!res.rowCount) throw new HttpError(409, 'message not found or not in a failed state')
      await redis.publish(channels.wake, req.device.id).catch(() => 0)
      return reply.status(202).send(presentOutbound(res.rows[0]))
    },
  )

  app.get(
    '/devices/:deviceId/messages',
    {
      schema: {
        tags,
        summary: 'Histórico de mensagens armazenadas (recebidas e enviadas)',
        params: DeviceParams,
        querystring: Type.Object({
          chat: Type.Optional(Type.String({ description: 'Número ou JID da conversa' })),
          before: Type.Optional(Type.String({ format: 'date-time', description: 'Somente mensagens anteriores a este instante' })),
          limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 500, default: 50 })),
        }),
        response: { 200: Any },
      },
    },
    async (req) => {
      const { chat, before, limit = 50 } = req.query
      const res = await query(
        `SELECT payload, status FROM messages
          WHERE device_id = $1 AND NOT pending_event
            AND ($2::text IS NULL OR chat_jid = $2 OR payload->>'chatAlt' = $2)
            AND ($3::timestamptz IS NULL OR ts < $3)
          ORDER BY ts DESC, id DESC LIMIT $4`,
        [req.device.id, chat ? toJid(chat) : null, before ?? null, limit],
      )
      return res.rows.map((r) => ({ ...r.payload, status: r.status }))
    },
  )

  const MessageParams = Type.Object({ deviceId: Type.String({ format: 'uuid' }), messageId: Type.String() })

  app.get(
    '/devices/:deviceId/messages/:messageId',
    {
      schema: {
        tags,
        summary: 'Uma mensagem armazenada',
        params: MessageParams,
        querystring: Type.Object({ raw: Type.Optional(Type.Boolean({ description: 'Inclui o objeto bruto do Baileys' })) }),
        response: { 200: Any },
      },
    },
    async (req) => {
      const res = await query('SELECT payload, status, raw FROM messages WHERE device_id = $1 AND message_id = $2 ORDER BY id LIMIT 1', [
        req.device.id,
        req.params.messageId,
      ])
      if (!res.rowCount) throw new HttpError(404, 'message not found')
      const row = res.rows[0]
      return { ...row.payload, status: row.status, ...(req.query.raw ? { raw: row.raw } : {}) }
    },
  )

  app.get(
    '/devices/:deviceId/messages/:messageId/media',
    {
      schema: {
        tags,
        summary: 'Mídia de uma mensagem',
        description:
          'Redireciona (302) para uma URL temporária nova. Com `?redirect=false` devolve JSON com a URL. ' +
          'Se o download original falhou, tenta de novo agora (o dispositivo precisa estar conectado).',
        params: MessageParams,
        querystring: Type.Object({ redirect: Type.Optional(Type.Boolean({ default: true })) }),
        response: { 200: Any },
      },
    },
    async (req, reply) => {
      const res = await query('SELECT payload FROM messages WHERE device_id = $1 AND message_id = $2 ORDER BY id LIMIT 1', [
        req.device.id,
        req.params.messageId,
      ])
      if (!res.rowCount) throw new HttpError(404, 'message not found')
      let media = res.rows[0].payload.media
      if (!media) throw new HttpError(404, 'message has no media')
      if (media.status !== 'stored' || !media.key) {
        media = await callDevice<any>(req.device.id, 'downloadMedia', { messageId: req.params.messageId })
      }
      const url = await presignedUrl(media.key, 3600)
      if (req.query.redirect === false) return { url, mimetype: media.mimetype, size: media.size, fileName: media.fileName }
      return reply.redirect(url)
    },
  )

  app.post(
    '/devices/:deviceId/messages/read',
    {
      schema: {
        tags,
        summary: 'Marca mensagens como lidas',
        params: DeviceParams,
        body: Type.Object({
          messages: Type.Array(
            Type.Object({
              chat: Type.String(),
              id: Type.String(),
              participant: Type.Optional(Type.String({ description: 'Autor, obrigatório em grupos' })),
            }),
            { minItems: 1 },
          ),
        }),
        response: { 200: Any },
      },
    },
    async (req) => callDevice(req.device.id, 'markRead', req.body),
  )
}
