import { randomUUID } from 'node:crypto'
import type { Redis } from 'ioredis'
import { config } from './config.js'
import { logger } from './logger.js'
import { channels } from './redis.js'

/**
 * Request/response between the API and whichever worker currently owns a device.
 * Used only for synchronous queries (group metadata, number checks, pairing codes...).
 * Durable work — sending messages — never goes through here.
 */

interface RpcRequest {
  id: string
  replyTo: string
  deviceId: string
  method: string
  params: unknown
}

interface RpcResponse {
  id: string
  ok: boolean
  result?: unknown
  error?: { message: string; statusCode: number }
}

export class RpcError extends Error {
  constructor(
    message: string,
    public statusCode = 500,
  ) {
    super(message)
  }
}

export class RpcClient {
  private pending = new Map<string, { resolve: (v: any) => void; reject: (e: Error) => void; timer: NodeJS.Timeout }>()
  private replyTo = channels.rpcResponse(config.instanceId)

  constructor(
    private pub: Redis,
    private sub: Redis,
  ) {}

  async start(): Promise<void> {
    await this.sub.subscribe(this.replyTo)
    this.sub.on('message', (channel, raw) => {
      if (channel !== this.replyTo) return
      let res: RpcResponse
      try {
        res = JSON.parse(raw)
      } catch {
        return
      }
      const entry = this.pending.get(res.id)
      if (!entry) return
      this.pending.delete(res.id)
      clearTimeout(entry.timer)
      if (res.ok) entry.resolve(res.result)
      else entry.reject(new RpcError(res.error?.message ?? 'rpc failed', res.error?.statusCode ?? 500))
    })
  }

  call<T = unknown>(deviceId: string, method: string, params: unknown = {}, timeoutMs = config.api.rpcTimeoutMs): Promise<T> {
    const id = randomUUID()
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id)
        reject(new RpcError('the worker holding this device did not answer in time', 504))
      }, timeoutMs)
      this.pending.set(id, { resolve, reject, timer })
      const req: RpcRequest = { id, replyTo: this.replyTo, deviceId, method, params }
      this.pub.publish(channels.rpcRequest, JSON.stringify(req)).catch((err) => {
        this.pending.delete(id)
        clearTimeout(timer)
        reject(new RpcError(`could not reach workers: ${err.message}`, 503))
      })
    })
  }
}

export type RpcHandler = (deviceId: string, method: string, params: any) => Promise<unknown> | undefined

/**
 * Every worker hears every request; only the one that owns the device answers.
 * `handler` returns undefined (synchronously) when the device is not held locally.
 */
export async function serveRpc(pub: Redis, sub: Redis, handler: RpcHandler): Promise<void> {
  await sub.subscribe(channels.rpcRequest)
  sub.on('message', (channel, raw) => {
    if (channel !== channels.rpcRequest) return
    let req: RpcRequest
    try {
      req = JSON.parse(raw)
    } catch {
      return
    }
    let promise: Promise<unknown> | undefined
    try {
      promise = handler(req.deviceId, req.method, req.params)
    } catch (err) {
      promise = Promise.reject(err)
    }
    if (!promise) return
    promise
      .then((result): RpcResponse => ({ id: req.id, ok: true, result: result ?? null }))
      .catch((err): RpcResponse => {
        const statusCode = err instanceof RpcError ? err.statusCode : (err?.output?.statusCode ?? err?.data ?? 502)
        logger.warn({ err: err?.message, method: req.method, deviceId: req.deviceId }, 'rpc call failed')
        return {
          id: req.id,
          ok: false,
          error: {
            message: err?.message ?? 'rpc failed',
            statusCode: typeof statusCode === 'number' && statusCode >= 400 && statusCode < 600 ? statusCode : 502,
          },
        }
      })
      .then((res) => pub.publish(req.replyTo, JSON.stringify(res)))
      .catch((err) => logger.warn({ err }, 'could not publish rpc response'))
  })
}
