import { config } from '../config.js'
import { pool } from '../db.js'
import { webhookQueue } from '../events.js'
import { logger } from '../logger.js'
import { redis, rpc } from './context.js'
import { buildServer } from './server.js'

if (config.api.adminKey.length < 16) {
  logger.fatal('ADMIN_API_KEY must be set to a secret of at least 16 characters')
  process.exit(1)
}

await rpc.start()
const app = await buildServer()
await app.listen({ port: config.api.port, host: config.api.host })

let closing = false
async function shutdown(signal: string) {
  if (closing) return
  closing = true
  logger.info({ signal }, 'api shutting down')
  await app.close()
  await webhookQueue().close().catch(() => {})
  await pool.end().catch(() => {})
  redis.disconnect()
  process.exit(0)
}
process.on('SIGTERM', () => void shutdown('SIGTERM'))
process.on('SIGINT', () => void shutdown('SIGINT'))
