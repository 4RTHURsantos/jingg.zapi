import { STATUS_CODES } from 'node:http'
import fastifySwagger from '@fastify/swagger'
import fastifySwaggerUi from '@fastify/swagger-ui'
import type { TypeBoxTypeProvider } from '@fastify/type-provider-typebox'
import Fastify, { type FastifyInstance } from 'fastify'
import { config } from '../config.js'
import { pool, query } from '../db.js'
import { EVENT_TYPES } from '../events.js'
import { logger } from '../logger.js'
import { metrics, registry } from '../metrics.js'
import { storageHealthy } from '../storage.js'
import { DEVICE_COLUMNS, hashKey, HttpError, isAdminKey, redis, type DeviceRow } from './context.js'
import { contactRoutes } from './routes/contacts.js'
import { deviceRoutes } from './routes/devices.js'
import { eventRoutes } from './routes/events.js'
import { groupRoutes } from './routes/groups.js'
import { messageRoutes } from './routes/messages.js'
import { webhookRoutes } from './routes/webhooks.js'

declare module 'fastify' {
  interface FastifyRequest {
    auth: { admin: boolean; deviceId?: string }
    device: DeviceRow
  }
}

export type App = FastifyInstance<any, any, any, any, TypeBoxTypeProvider>

const DESCRIPTION = `
API de WhatsApp multi-dispositivo sobre Baileys.

**Autenticação** — envie a chave no header \`x-api-key\` (ou \`Authorization: Bearer\`).
A chave de administrador (\`ADMIN_API_KEY\`) acessa tudo; a chave de dispositivo (devolvida uma única vez na criação)
acessa somente as rotas do próprio dispositivo.

**Entrega garantida** — todo evento é gravado no Postgres antes de ser entregue. Webhooks são entregues
*ao menos uma vez*, com retry exponencial; use o campo \`id\` do evento para deduplicar e \`seq\` para ordenar.
O mesmo fluxo pode ser lido por polling em \`GET /v1/devices/{deviceId}/events?after={seq}\`.

**Eventos**: ${EVENT_TYPES.map((t) => `\`${t}\``).join(', ')}.
`

export async function buildServer(): Promise<App> {
  const app = Fastify({
    loggerInstance: logger.child({ module: 'http' }),
    bodyLimit: config.api.bodyLimit,
    disableRequestLogging: true,
    ajv: { customOptions: { removeAdditional: false, coerceTypes: true, useDefaults: true } },
  }).withTypeProvider<TypeBoxTypeProvider>() as unknown as App

  await app.register(fastifySwagger, {
    openapi: {
      info: { title: 'Jingg Zapi', description: DESCRIPTION, version: '0.1.0' },
      components: {
        securitySchemes: { apiKey: { type: 'apiKey', in: 'header', name: 'x-api-key' } },
      },
      security: [{ apiKey: [] }],
      tags: [
        { name: 'Dispositivos' },
        { name: 'Mensagens' },
        { name: 'Webhooks' },
        { name: 'Eventos' },
        { name: 'Contatos' },
        { name: 'Grupos' },
        { name: 'Sistema' },
      ],
    },
  })
  await app.register(fastifySwaggerUi, { routePrefix: '/docs' })

  app.decorateRequest('auth', null as any)
  app.decorateRequest('device', null as any)

  app.addHook('onResponse', async (req, reply) => {
    const route = req.routeOptions?.url ?? 'unmatched'
    metrics.httpDuration.observe({ method: req.method, route, status: reply.statusCode }, reply.elapsedTime / 1000)
    if (route !== '/health' && route !== '/metrics') {
      req.log.info({ method: req.method, url: req.url, status: reply.statusCode, ms: Math.round(reply.elapsedTime) }, 'request')
    }
  })

  app.setErrorHandler((err: any, req, reply) => {
    const status = err instanceof HttpError ? err.statusCode : (err.statusCode ?? 500)
    if (status >= 500) req.log.error({ err }, 'request failed')
    reply.status(status).send({
      statusCode: status,
      error: STATUS_CODES[status] ?? 'Error',
      message: status >= 500 && !(err instanceof HttpError) ? 'internal error' : err.message,
    })
  })

  // ───────────── public system routes ─────────────

  app.get('/health', { schema: { tags: ['Sistema'], security: [], summary: 'Liveness' } }, async () => ({ status: 'ok' }))

  app.get('/ready', { schema: { tags: ['Sistema'], security: [], summary: 'Readiness (Postgres, Redis, storage)' } }, async (_req, reply) => {
    const checks = {
      postgres: await pool.query('SELECT 1').then(() => true, () => false),
      redis: await redis.ping().then(() => true, () => false),
      storage: await storageHealthy(),
    }
    const ok = Object.values(checks).every(Boolean)
    return reply.status(ok ? 200 : 503).send({ status: ok ? 'ok' : 'degraded', checks })
  })

  app.get('/metrics', { schema: { hide: true } }, async (_req, reply) => {
    return reply.header('content-type', registry.contentType).send(await registry.metrics())
  })

  // ───────────── authenticated API ─────────────

  await app.register(
    async (v1) => {
      v1.addHook('onRequest', async (req) => {
        const header = req.headers['x-api-key']
        const bearer = req.headers.authorization?.match(/^Bearer\s+(.+)$/i)?.[1]
        const key = (Array.isArray(header) ? header[0] : header) ?? bearer
        if (!key) throw new HttpError(401, 'missing API key (x-api-key header)')
        if (isAdminKey(key)) {
          req.auth = { admin: true }
          return
        }
        const res = await query('SELECT id FROM devices WHERE api_key_hash = $1', [hashKey(key)])
        if (!res.rowCount) throw new HttpError(401, 'invalid API key')
        req.auth = { admin: false, deviceId: res.rows[0].id }
      })

      // every /devices/:deviceId/... route gets the device loaded and access-checked
      v1.addHook('preHandler', async (req) => {
        const deviceId = (req.params as any)?.deviceId
        if (!deviceId) return
        if (!req.auth.admin && req.auth.deviceId !== deviceId) throw new HttpError(403, 'this key cannot access this device')
        const res = await query<DeviceRow>(`SELECT ${DEVICE_COLUMNS} FROM devices WHERE id = $1`, [deviceId])
        if (!res.rowCount) throw new HttpError(404, 'device not found')
        req.device = res.rows[0]
      })

      await v1.register(deviceRoutes)
      await v1.register(webhookRoutes)
      await v1.register(messageRoutes)
      await v1.register(eventRoutes)
      await v1.register(contactRoutes)
      await v1.register(groupRoutes)
    },
    { prefix: '/v1' },
  )

  return app
}
