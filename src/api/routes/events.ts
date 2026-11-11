import { Type } from '@sinclair/typebox'
import { query } from '../../db.js'
import { Any, DeviceParams } from '../context.js'
import type { App } from '../server.js'

export async function eventRoutes(app: App) {
  app.get(
    '/devices/:deviceId/events',
    {
      schema: {
        tags: ['Eventos'],
        summary: 'Log de eventos do dispositivo (alternativa a webhooks)',
        description:
          'Mesmos eventos e mesmo formato dos webhooks. `seq` é sequencial e sem buracos por dispositivo: ' +
          'guarde o último `seq` processado e peça `?after=<seq>` para continuar de onde parou, sem perder nem repetir nada.',
        params: DeviceParams,
        querystring: Type.Object({
          after: Type.Optional(Type.Integer({ minimum: 0, default: 0 })),
          limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 1000, default: 100 })),
          types: Type.Optional(Type.String({ description: 'Lista separada por vírgula, ex. message.received,message.status' })),
        }),
        response: { 200: Any },
      },
    },
    async (req) => {
      const { after = 0, limit = 100, types } = req.query
      const list = types ? types.split(',').map((t) => t.trim()).filter(Boolean) : null
      const res = await query(
        `SELECT id, seq, type, payload, created_at FROM events
          WHERE device_id = $1 AND seq > $2 AND ($3::text[] IS NULL OR type = ANY ($3))
          ORDER BY seq LIMIT $4`,
        [req.device.id, after, list, limit],
      )
      const events = res.rows.map((e) => ({
        id: e.id,
        seq: e.seq,
        deviceId: req.device.id,
        type: e.type,
        timestamp: e.created_at,
        data: e.payload,
      }))
      return { events, lastSeq: events.length ? events[events.length - 1].seq : after, hasMore: events.length === limit }
    },
  )
}
