import type { Redis } from 'ioredis'
import { config } from '../config.js'
import { query } from '../db.js'
import { logger } from '../logger.js'
import { metrics } from '../metrics.js'
import { channels, keys, RELEASE_LEASE, RENEW_LEASE } from '../redis.js'
import { RpcError } from '../rpc.js'
import { commands } from './commands.js'
import { Session } from './session.js'

/**
 * Decides which devices this worker runs.
 *
 * A device belongs to whoever holds its Redis lease (`SET NX PX`). Leases are renewed at a third
 * of their TTL; a worker that cannot renew stops the socket immediately, so two workers never
 * hold the same WhatsApp session for longer than the overlap WhatsApp itself resolves (440).
 * If a worker dies, its leases expire and the others pick the devices up on their next reconcile.
 */
export class Manager {
  readonly workerId = config.instanceId
  private sessions = new Map<string, Session>()
  private lastRenewed = new Map<string, number>()
  private reconciling = false
  private reconcileAgain = false
  private shuttingDown = false
  private timers: NodeJS.Timeout[] = []

  constructor(
    private redis: Redis,
    private sub: Redis,
  ) {}

  async start(): Promise<void> {
    await this.heartbeat()
    await this.sub.subscribe(channels.control, channels.wake)
    this.sub.on('message', (channel, payload) => {
      if (channel === channels.control) void this.reconcile()
      else if (channel === channels.wake) void this.sessions.get(payload)?.drain()
    })

    const every = (ms: number, fn: () => Promise<void>) =>
      this.timers.push(setInterval(() => fn().catch((err) => logger.error({ err }, 'manager tick failed')), ms))

    every(5_000, () => this.heartbeat())
    every(Math.floor(config.worker.leaseTtlMs / 3), () => this.renewLeases())
    every(config.worker.reconcileIntervalMs, () => this.reconcile())
    // safety net for the wake pub/sub (which is fire-and-forget) and for retries that became due
    every(3_000, () => this.drainDue())

    await this.reconcile()
    logger.info({ workerId: this.workerId }, 'worker manager started')
  }

  get(deviceId: string): Session | undefined {
    return this.sessions.get(deviceId)
  }

  get size(): number {
    return this.sessions.size
  }

  /** RPC entry point. Returns undefined when the device is not ours, so another worker can answer. */
  handleRpc(deviceId: string, method: string, params: unknown): Promise<unknown> | undefined {
    const session = this.sessions.get(deviceId)
    if (!session) return undefined
    const command = commands[method]
    if (!command) return Promise.reject(new RpcError(`unknown method ${method}`, 400))
    return command(session, params ?? {})
  }

  private async heartbeat(): Promise<void> {
    await this.redis.set(keys.worker(this.workerId), JSON.stringify({ sessions: this.sessions.size, at: Date.now() }), 'EX', 15)
    const byState: Record<string, number> = { connecting: 0, qr: 0, open: 0, closed: 0 }
    for (const s of this.sessions.values()) byState[s.state]++
    for (const [state, count] of Object.entries(byState)) metrics.sessions.set({ state }, count)
  }

  private async aliveWorkers(): Promise<number> {
    let cursor = '0'
    let count = 0
    do {
      const [next, found] = await this.redis.scan(cursor, 'MATCH', keys.workerPattern, 'COUNT', 200)
      cursor = next
      count += found.length
    } while (cursor !== '0')
    return Math.max(count, 1)
  }

  async reconcile(): Promise<void> {
    if (this.shuttingDown) return
    if (this.reconciling) {
      this.reconcileAgain = true
      return
    }
    this.reconciling = true
    try {
      do {
        this.reconcileAgain = false
        await this.reconcileOnce()
      } while (this.reconcileAgain && !this.shuttingDown)
    } catch (err) {
      logger.error({ err }, 'reconcile failed')
    } finally {
      this.reconciling = false
    }
  }

  private async reconcileOnce(): Promise<void> {
    const rows = await query<{ id: string }>(`SELECT id FROM devices WHERE desired_state = 'started'`)
    const wanted = new Set(rows.rows.map((r) => r.id))

    // 1. stop what should no longer run (stopped or deleted through the API)
    for (const [id, session] of this.sessions) {
      if (wanted.has(id)) continue
      this.sessions.delete(id)
      await session.stop(true, 'stopped')
      await this.release(id)
      logger.info({ deviceId: id }, 'session stopped')
    }

    // 2. fair share, so a new worker actually receives load
    const workers = await this.aliveWorkers()
    const fairShare = Math.ceil(wanted.size / workers)
    const capacity = Math.min(config.worker.maxDevices, Math.max(fairShare, 1))

    if (config.worker.rebalance && workers > 1 && this.sessions.size > capacity) {
      const excess = [...this.sessions.keys()].slice(0, this.sessions.size - capacity)
      for (const id of excess) {
        const session = this.sessions.get(id)!
        this.sessions.delete(id)
        await session.stop(false)
        await this.release(id)
        logger.info({ deviceId: id }, 'session handed over for rebalancing')
      }
      if (excess.length) await this.redis.publish(channels.control, 'rebalance')
      return
    }

    // 3. claim what nobody runs
    const candidates = [...wanted].filter((id) => !this.sessions.has(id)).sort(() => Math.random() - 0.5)
    for (const id of candidates) {
      // when every worker is at its fair share, the hard limit is what matters: never leave a device unclaimed
      if (this.sessions.size >= config.worker.maxDevices) break
      if (this.sessions.size >= capacity && workers > 1) break
      const claimed = await this.redis.set(keys.lease(id), this.workerId, 'PX', config.worker.leaseTtlMs, 'NX')
      if (claimed !== 'OK') continue
      this.lastRenewed.set(id, Date.now())
      const session = new Session(id, this.redis, {
        onTerminal: (deviceId) => this.drop(deviceId),
        ownsLease: async (deviceId) => (await this.redis.get(keys.lease(deviceId))) === this.workerId,
      })
      this.sessions.set(id, session)
      logger.info({ deviceId: id }, 'device claimed')
      void session.start()
    }
  }

  private drop(deviceId: string): void {
    if (!this.sessions.delete(deviceId)) return
    void this.release(deviceId)
  }

  private async release(deviceId: string): Promise<void> {
    this.lastRenewed.delete(deviceId)
    await this.redis.eval(RELEASE_LEASE, 1, keys.lease(deviceId), this.workerId).catch(() => {})
  }

  private async renewLeases(): Promise<void> {
    for (const [id, session] of this.sessions) {
      let renewed: boolean | undefined
      try {
        renewed = (await this.redis.eval(RENEW_LEASE, 1, keys.lease(id), this.workerId, config.worker.leaseTtlMs)) === 1
      } catch {
        renewed = undefined // Redis unreachable: we do not know yet
      }
      if (renewed) {
        this.lastRenewed.set(id, Date.now())
        continue
      }
      // Lost for sure, or unknown for so long that the lease may have expired: another worker may own it now.
      const stale = Date.now() - (this.lastRenewed.get(id) ?? 0) > config.worker.leaseTtlMs * 0.8
      if (renewed === false || stale) {
        logger.warn({ deviceId: id, renewed }, 'lease lost; stopping session')
        this.sessions.delete(id)
        this.lastRenewed.delete(id)
        await session.stop(false)
      }
    }
  }

  private async drainDue(): Promise<void> {
    const open = [...this.sessions.values()].filter((s) => s.isOpen).map((s) => s.deviceId)
    if (!open.length) return
    const due = await query<{ device_id: string }>(
      `SELECT DISTINCT device_id FROM outbound_messages
        WHERE device_id = ANY ($1::uuid[]) AND status IN ('queued', 'sending') AND next_attempt_at <= now()`,
      [open],
    )
    for (const row of due.rows) void this.sessions.get(row.device_id)?.drain()
  }

  /** Graceful stop: close sockets, free the leases and tell the other workers to take over right away. */
  async shutdown(): Promise<void> {
    this.shuttingDown = true
    this.timers.forEach(clearInterval)
    const sessions = [...this.sessions.entries()]
    this.sessions.clear()
    await Promise.allSettled(
      sessions.map(async ([id, session]) => {
        await session.stop(false)
        await this.release(id)
      }),
    )
    await this.redis.del(keys.worker(this.workerId)).catch(() => {})
    await this.redis.publish(channels.control, 'worker-left').catch(() => {})
  }
}
