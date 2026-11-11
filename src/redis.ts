import { Redis } from 'ioredis'
import { config } from './config.js'
import { logger } from './logger.js'

export function createRedis(name: string): Redis {
  const client = new Redis(config.redisUrl, {
    // required by BullMQ for blocking connections, and harmless elsewhere:
    // commands wait for the reconnection instead of failing after N retries
    maxRetriesPerRequest: null,
    connectionName: `zapi:${name}`,
  })
  client.on('error', (err) => logger.warn({ err: err.message, conn: name }, 'redis error'))
  return client
}

const P = 'zapi'
export const keys = {
  lease: (deviceId: string) => `${P}:lease:${deviceId}`,
  worker: (workerId: string) => `${P}:worker:${workerId}`,
  workerPattern: `${P}:worker:*`,
  qr: (deviceId: string) => `${P}:qr:${deviceId}`,
}

export const channels = {
  /** any change in the set of devices that should be running */
  control: `${P}:control`,
  /** payload = deviceId that has new outbound messages */
  wake: `${P}:wake`,
  rpcRequest: `${P}:rpc:req`,
  rpcResponse: (instanceId: string) => `${P}:rpc:res:${instanceId}`,
}

/** Extends a lease only if we still own it. Returns 1 when renewed. */
export const RENEW_LEASE = `
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('pexpire', KEYS[1], ARGV[2])
end
return 0`

/** Deletes a lease only if we still own it. */
export const RELEASE_LEASE = `
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0`
