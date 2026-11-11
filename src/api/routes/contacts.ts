import { Type } from '@sinclair/typebox'
import { query } from '../../db.js'
import { toJid } from '../../jid.js'
import { Any, callDevice, DeviceParams } from '../context.js'
import type { App } from '../server.js'

const tags = ['Contatos']
const JidParams = Type.Object({
  deviceId: Type.String({ format: 'uuid' }),
  jid: Type.String({ description: 'Número com DDI ou JID' }),
})

export async function contactRoutes(app: App) {
  app.post(
    '/devices/:deviceId/contacts/check',
    {
      schema: {
        tags,
        summary: 'Verifica quais números têm WhatsApp',
        params: DeviceParams,
        body: Type.Object({ numbers: Type.Array(Type.String(), { minItems: 1, maxItems: 100 }) }),
        response: { 200: Any },
      },
    },
    async (req) => callDevice(req.device.id, 'checkNumbers', req.body),
  )

  app.get(
    '/devices/:deviceId/contacts/:jid/picture',
    { schema: { tags, summary: 'URL da foto de perfil', params: JidParams, response: { 200: Any } } },
    async (req) => callDevice(req.device.id, 'profilePicture', { jid: req.params.jid }),
  )

  app.get(
    '/devices/:deviceId/contacts/:jid/status',
    { schema: { tags, summary: 'Recado (status de texto) do contato', params: JidParams, response: { 200: Any } } },
    async (req) => callDevice(req.device.id, 'contactStatus', { jid: req.params.jid }),
  )

  app.get(
    '/devices/:deviceId/contacts/:jid/business',
    { schema: { tags, summary: 'Perfil comercial do contato', params: JidParams, response: { 200: Any } } },
    async (req) => callDevice(req.device.id, 'businessProfile', { jid: req.params.jid }),
  )

  app.post(
    '/devices/:deviceId/contacts/:jid/block',
    {
      schema: {
        tags,
        summary: 'Bloqueia ou desbloqueia um contato',
        params: JidParams,
        body: Type.Object({ action: Type.String({ enum: ['block', 'unblock'] }) }),
        response: { 200: Any },
      },
    },
    async (req) => callDevice(req.device.id, 'block', { jid: req.params.jid, action: req.body.action }),
  )

  app.post(
    '/devices/:deviceId/presence',
    {
      schema: {
        tags,
        summary: 'Envia presença (digitando, gravando, online)',
        description: '`composing`/`recording`/`paused` exigem `to`. `available`/`unavailable` valem para a conta.',
        params: DeviceParams,
        body: Type.Object({
          state: Type.String({ enum: ['composing', 'recording', 'paused', 'available', 'unavailable'] }),
          to: Type.Optional(Type.String()),
        }),
        response: { 200: Any },
      },
    },
    async (req) => callDevice(req.device.id, 'presence', req.body),
  )

  app.post(
    '/devices/:deviceId/contacts/:jid/presence/subscribe',
    {
      schema: {
        tags,
        summary: 'Passa a receber `presence.update` deste contato ou grupo',
        description:
          'A assinatura é lembrada e refeita a cada reconexão. O WhatsApp só envia "digitando" para contas online: ' +
          'ligue `settings.presenceEvents` no dispositivo (que também assina sozinho cada conversa) ou envie `available` em `POST …/presence`.',
        params: JidParams,
        response: { 200: Any },
      },
    },
    async (req) => callDevice(req.device.id, 'presenceSubscribe', { jid: req.params.jid }),
  )

  app.delete(
    '/devices/:deviceId/contacts/:jid/presence/subscribe',
    {
      schema: {
        tags,
        summary: 'Deixa de acompanhar a presença deste contato',
        description: 'O WhatsApp não tem cancelamento: a assinatura atual dura até a próxima reconexão, e não é refeita.',
        params: JidParams,
        response: { 200: Any },
      },
    },
    async (req) => {
      const res = await query('DELETE FROM presence_subscriptions WHERE device_id = $1 AND jid = $2', [req.device.id, toJid(req.params.jid)])
      return { ok: true, removed: !!res.rowCount }
    },
  )

  app.post(
    '/devices/:deviceId/calls/reject',
    {
      schema: {
        tags,
        summary: 'Rejeita uma chamada recebida',
        description: 'Use `id` e `from` do evento `call.update`.',
        params: DeviceParams,
        body: Type.Object({ callId: Type.String(), from: Type.String() }),
        response: { 200: Any },
      },
    },
    async (req) => callDevice(req.device.id, 'rejectCall', req.body),
  )
}
