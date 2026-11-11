import { config } from '../config.js'
import { pool } from '../db.js'
import { logger } from '../logger.js'
import { startMetricsServer } from '../metrics.js'
import { createRedis } from '../redis.js'
import { serveRpc } from '../rpc.js'
import { Manager } from './manager.js'

const redis = createRedis('worker')
const controlSub = createRedis('worker-control')
const rpcSub = createRedis('worker-rpc')

const manager = new Manager(redis, controlSub)
await serveRpc(redis, rpcSub, (deviceId, method, params) => manager.handleRpc(deviceId, method, params))
await manager.start()

const metricsServer = startMetricsServer(config.metricsPort, async () => {
  await pool.query('SELECT 1')
  return redis.status === 'ready'
})

let closing = false
async function shutdown(signal: string) {
  if (closing) return
  closing = true
  logger.info({ signal }, 'worker shutting down')
  await manager.shutdown()
  metricsServer.close()
  await pool.end().catch(() => {})
  redis.disconnect()
  controlSub.disconnect()
  rpcSub.disconnect()
  process.exit(0)
}
process.on('SIGTERM', () => void shutdown('SIGTERM'))
process.on('SIGINT', () => void shutdown('SIGINT'))

// Baileys occasionally rejects promises from inside its socket internals; one bad session must not take the others down.
process.on('unhandledRejection', (err) => logger.error({ err }, 'unhandled rejection'))
process.on('uncaughtException', (err) => logger.error({ err }, 'uncaught exception'))
