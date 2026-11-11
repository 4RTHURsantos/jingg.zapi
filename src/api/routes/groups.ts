import { Type } from '@sinclair/typebox'
import { Any, callDevice, DeviceParams } from '../context.js'
import type { App } from '../server.js'

const tags = ['Grupos']
const GroupParams = Type.Object({
  deviceId: Type.String({ format: 'uuid' }),
  groupJid: Type.String({ pattern: '@g\\.us$', description: 'JID do grupo, ex. 1203630…@g.us' }),
})

export async function groupRoutes(app: App) {
  app.get(
    '/devices/:deviceId/groups',
    { schema: { tags, summary: 'Grupos de que o dispositivo participa', params: DeviceParams, response: { 200: Any } } },
    async (req) => callDevice(req.device.id, 'groups.list'),
  )

  app.post(
    '/devices/:deviceId/groups',
    {
      schema: {
        tags,
        summary: 'Cria um grupo',
        params: DeviceParams,
        body: Type.Object({
          subject: Type.String({ minLength: 1, maxLength: 100 }),
          participants: Type.Array(Type.String(), { minItems: 1 }),
        }),
        response: { 201: Any },
      },
    },
    async (req, reply) => reply.status(201).send(await callDevice(req.device.id, 'groups.create', req.body)),
  )

  app.post(
    '/devices/:deviceId/groups/join',
    {
      schema: {
        tags,
        summary: 'Entra em um grupo por código/link de convite',
        params: DeviceParams,
        body: Type.Object({ code: Type.String({ description: 'Código ou link https://chat.whatsapp.com/…' }) }),
        response: { 200: Any },
      },
    },
    async (req) => callDevice(req.device.id, 'groups.join', { code: req.body.code.split('/').pop()!.split('?')[0] }),
  )

  app.get(
    '/devices/:deviceId/groups/invite/:code',
    {
      schema: {
        tags,
        summary: 'Dados de um grupo a partir do código de convite (sem entrar)',
        params: Type.Object({ deviceId: Type.String({ format: 'uuid' }), code: Type.String() }),
        response: { 200: Any },
      },
    },
    async (req) => callDevice(req.device.id, 'groups.inviteInfo', { code: req.params.code }),
  )

  app.get(
    '/devices/:deviceId/groups/:groupJid',
    { schema: { tags, summary: 'Metadados e participantes do grupo', params: GroupParams, response: { 200: Any } } },
    async (req) => callDevice(req.device.id, 'groups.get', { jid: req.params.groupJid }),
  )

  app.patch(
    '/devices/:deviceId/groups/:groupJid',
    {
      schema: {
        tags,
        summary: 'Altera nome, descrição e configurações do grupo',
        params: GroupParams,
        body: Type.Object({
          subject: Type.Optional(Type.String({ maxLength: 100 })),
          description: Type.Optional(Type.String({ description: 'String vazia remove a descrição' })),
          announce: Type.Optional(Type.Boolean({ description: 'true = só admins enviam mensagens' })),
          locked: Type.Optional(Type.Boolean({ description: 'true = só admins editam dados do grupo' })),
          ephemeral: Type.Optional(Type.Integer({ description: 'Mensagens temporárias em segundos (0 desliga; 86400, 604800, 7776000)' })),
          memberAddMode: Type.Optional(Type.String({ enum: ['admin_add', 'all_member_add'] })),
          joinApprovalMode: Type.Optional(Type.Boolean({ description: 'true = entrada exige aprovação de admin' })),
        }),
        response: { 200: Any },
      },
    },
    async (req) => callDevice(req.device.id, 'groups.update', { jid: req.params.groupJid, ...req.body }),
  )

  app.post(
    '/devices/:deviceId/groups/:groupJid/participants',
    {
      schema: {
        tags,
        summary: 'Adiciona, remove, promove ou rebaixa participantes',
        params: GroupParams,
        body: Type.Object({
          action: Type.String({ enum: ['add', 'remove', 'promote', 'demote'] }),
          participants: Type.Array(Type.String(), { minItems: 1 }),
        }),
        response: { 200: Any },
      },
    },
    async (req) => callDevice(req.device.id, 'groups.participants', { jid: req.params.groupJid, ...req.body }),
  )

  app.get(
    '/devices/:deviceId/groups/:groupJid/invite-code',
    { schema: { tags, summary: 'Link de convite do grupo', params: GroupParams, response: { 200: Any } } },
    async (req) => callDevice(req.device.id, 'groups.inviteCode', { jid: req.params.groupJid }),
  )

  app.post(
    '/devices/:deviceId/groups/:groupJid/invite-code/revoke',
    { schema: { tags, summary: 'Revoga o link de convite e gera um novo', params: GroupParams, response: { 200: Any } } },
    async (req) => callDevice(req.device.id, 'groups.revokeInvite', { jid: req.params.groupJid }),
  )

  app.post(
    '/devices/:deviceId/groups/:groupJid/leave',
    { schema: { tags, summary: 'Sai do grupo', params: GroupParams, response: { 200: Any } } },
    async (req) => callDevice(req.device.id, 'groups.leave', { jid: req.params.groupJid }),
  )
}
