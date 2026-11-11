import type { Redis } from 'ioredis'
import {
  Browsers,
  BufferJSON,
  DisconnectReason,
  downloadMediaMessage,
  fetchLatestBaileysVersion,
  generateMessageIDV2,
  getAggregateVotesInPollMessage,
  isJidStatusBroadcast,
  jidNormalizedUser,
  makeCacheableSignalKeyStore,
  makeWASocket,
  proto,
  type BaileysEventMap,
  type ConnectionState,
  type GroupMetadata,
  type WAMessage,
  type WAMessageKey,
  type WASocket,
  type WAVersion,
} from 'baileys'
import { clearAuthState, usePostgresAuthState } from '../auth-state.js'
import { config } from '../config.js'
import { query, toJsonb, tx } from '../db.js'
import { emitEvent, enqueueDeliveries, type EmittedEvent, type EventType } from '../events.js'
import { logger as rootLogger } from '../logger.js'
import { metrics } from '../metrics.js'
import { keys } from '../redis.js'
import { presignedUrl, putObject, statObject } from '../storage.js'
import {
  extensionFor,
  unwrapLottie,
  normalizeMessage,
  STATUS_NAMES,
  STATUS_RANK,
  textOf,
  type MediaInfo,
  type NormalizedMessage,
} from './normalize.js'
import { buildContent, type OutboundRow } from './outbound.js'
import { StickerError } from './sticker.js'

const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))

function withTimeout<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: NodeJS.Timeout
  return Promise.race([
    promise,
    new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${what} timed out after ${ms}ms`)), ms)
    }),
  ]).finally(() => clearTimeout(timer))
}

// One version lookup shared by every session in the process; WhatsApp rejects clients that are too old.
let versionCache: { version: WAVersion; at: number } | undefined
async function waVersion(): Promise<WAVersion | undefined> {
  if (versionCache && Date.now() - versionCache.at < 6 * 3600_000) return versionCache.version
  try {
    const { version } = await fetchLatestBaileysVersion()
    versionCache = { version, at: Date.now() }
    return version
  } catch {
    return versionCache?.version // undefined → Baileys' bundled default
  }
}

export interface DeviceSettings {
  /** show as online and follow who is typing in every chat that writes to the device */
  presenceEvents?: boolean
}

/** A failure that retrying cannot fix (e.g. the number has no WhatsApp account). */
class PermanentError extends Error {}

export interface SessionHooks {
  /** the session ended for good (logged out, replaced, QR never scanned): drop it and free the lease */
  onTerminal(deviceId: string): void
  ownsLease(deviceId: string): Promise<boolean>
}

export type SessionState = 'connecting' | 'qr' | 'open' | 'closed'

export class Session {
  state: SessionState = 'connecting'
  private sock?: WASocket
  private stopped = false
  private generation = 0
  private backoffMs = 0
  private reconnectTimer?: NodeJS.Timeout
  private meJid = ''
  private meLid = ''
  private settings: DeviceSettings = {}
  /** chats already subscribed on the current socket */
  private presenceSubscribed = new Set<string>()
  private lastPresence = new Map<string, { state: string; at: number }>()
  private lastStatus = ''
  private draining = false
  /** connection events and message events run on separate serial chains, so a slow media download never delays a QR code */
  private connChain: Promise<void> = Promise.resolve()
  private msgChain: Promise<void> = Promise.resolve()
  private groupCache = new Map<string, { data: GroupMetadata; at: number }>()
  private resolvedJids = new Map<string, string>()
  private log

  constructor(
    readonly deviceId: string,
    private redis: Redis,
    private hooks: SessionHooks,
  ) {
    this.log = rootLogger.child({ deviceId })
  }

  get isOpen(): boolean {
    return this.state === 'open' && !this.stopped
  }

  get socket(): WASocket | undefined {
    return this.sock
  }

  get me(): string {
    return this.meJid
  }

  // ───────────────────────────── lifecycle ─────────────────────────────

  async start(): Promise<void> {
    try {
      await this.connect()
    } catch (err) {
      this.log.error({ err }, 'could not start session; retrying')
      this.scheduleReconnect()
    }
  }

  /**
   * Closes the socket without logging out. `announce` publishes the disconnection
   * (used when the user asked to stop; not when another worker is about to take over).
   */
  async stop(announce: boolean, reason?: string): Promise<void> {
    if (this.stopped) return
    this.stopped = true
    this.state = 'closed'
    clearTimeout(this.reconnectTimer)
    try {
      await this.sock?.end(undefined)
    } catch {}
    // let in-flight writes land before the device changes hands
    await withTimeout(Promise.allSettled([this.connChain, this.msgChain]), 15_000, 'session drain').catch(() => {})
    await this.redis.del(keys.qr(this.deviceId)).catch(() => {})
    if (announce) await this.setStatus('disconnected', reason ?? null, { clearWorker: true }).catch(() => {})
  }

  private async connect(): Promise<void> {
    if (this.stopped) return
    const gen = ++this.generation
    const { state, saveCreds } = await usePostgresAuthState(this.deviceId)
    await this.loadSettings()
    const baileysLogger = this.log.child({ module: 'baileys' }, { level: config.baileysLogLevel })
    const version = await waVersion()

    const sock = makeWASocket({
      auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, baileysLogger as any) },
      ...(version ? { version } : {}),
      // pairing-code login only works with a browser identity WhatsApp recognises
      browser: Browsers.macOS('Chrome'),
      logger: baileysLogger as any,
      // typing notifications are only sent to accounts that show as online
      markOnlineOnConnect: config.worker.markOnlineOnConnect || !!this.settings.presenceEvents,
      // recent-history sync stays on (Baileys needs it for LID mappings); history is not turned into events
      syncFullHistory: false,
      generateHighQualityLinkPreview: false,
      getMessage: async (key) => (await this.loadStoredMessage(key.id!, key.remoteJid ?? undefined))?.message ?? undefined,
      cachedGroupMetadata: async (jid) => this.cachedGroup(jid),
    })
    this.sock = sock
    this.state = 'connecting'
    if (state.creds.me?.id) this.meJid = jidNormalizedUser(state.creds.me.id)
    // while reconnecting, the status (and its reason) stays as it is until the socket opens
    if (this.lastStatus !== 'reconnecting') await this.setStatus('connecting', null)

    sock.ev.process((events) => {
      if (gen !== this.generation) return // a socket we already replaced
      // credentials are saved immediately: losing a key rotation breaks the session
      if (events['creds.update']) {
        saveCreds().catch((err) => this.log.error({ err }, 'FAILED TO SAVE CREDENTIALS'))
      }
      if (events['connection.update']) {
        const update = events['connection.update']
        this.connChain = this.connChain
          .then(() => this.onConnectionUpdate(update, sock))
          .catch((err) => this.log.error({ err }, 'connection handler failed'))
      }
      this.msgChain = this.msgChain
        .then(() => this.onEvents(events))
        .catch((err) => this.log.error({ err }, 'event handler failed'))
    })
  }

  private scheduleReconnect(immediate = false): void {
    if (this.stopped) return
    this.backoffMs = immediate ? 0 : Math.min(this.backoffMs ? this.backoffMs * 2 : 1000, 60_000)
    const wait = immediate ? 0 : this.backoffMs + Math.floor(Math.random() * 500)
    clearTimeout(this.reconnectTimer)
    this.reconnectTimer = setTimeout(() => {
      this.connect().catch((err) => {
        this.log.error({ err }, 'reconnect failed')
        this.scheduleReconnect()
      })
    }, wait)
  }

  private async onConnectionUpdate(update: Partial<ConnectionState>, sock: WASocket): Promise<void> {
    const { connection, lastDisconnect, qr } = update

    if (qr) {
      this.state = 'qr'
      await this.redis.set(keys.qr(this.deviceId), JSON.stringify({ qr, at: Date.now() }), 'EX', 90)
      await this.setStatus('qr', null)
      await this.emit('qr.updated', { qr })
    }

    if (connection === 'open') {
      this.state = 'open'
      this.backoffMs = 0
      this.meJid = jidNormalizedUser(sock.user?.id)
      this.meLid = sock.user?.lid ? jidNormalizedUser(sock.user.lid) : ''
      await this.redis.del(keys.qr(this.deviceId)).catch(() => {})
      await this.setStatus('connected', null, {
        jid: this.meJid,
        lid: sock.user?.lid ? jidNormalizedUser(sock.user.lid) : null,
        name: sock.user?.name ?? null,
      })
      this.log.info({ jid: this.meJid }, 'connected')
      // finish anything a previous run stored but never announced, then flush the send queue
      this.msgChain = this.msgChain.then(() => this.recoverPending()).catch((err) => this.log.error({ err }, 'recovery failed'))
      void this.drain()
      void this.restorePresence()
    }

    if (connection === 'close') {
      const wasOpen = this.state === 'open'
      this.state = 'closed'
      if (this.stopped) return
      const error: any = lastDisconnect?.error
      const code: number | undefined = error?.output?.statusCode
      const message: string = error?.message ?? 'connection closed'
      metrics.reconnects.inc({ code: String(code ?? 'unknown') })
      this.log.warn({ code, message, wasOpen }, 'connection closed')

      if (code === DisconnectReason.loggedOut) {
        await clearAuthState(this.deviceId)
        return this.terminate('logged_out', 'logged_out')
      }
      if (code === DisconnectReason.forbidden) return this.terminate('disconnected', 'forbidden')
      if (code === DisconnectReason.connectionReplaced) {
        // If we no longer own the lease another worker took the device over and this socket is simply stale.
        // If we do own it, someone else is using these credentials: fighting for the connection would loop forever.
        if (await this.hooks.ownsLease(this.deviceId).catch(() => true)) {
          return this.terminate('disconnected', 'connection_replaced')
        }
        this.stopped = true
        this.hooks.onTerminal(this.deviceId)
        return
      }
      if (!sock.authState.creds.registered && !sock.authState.creds.me && /QR refs attempts ended/i.test(message)) {
        return this.terminate('disconnected', 'qr_timeout')
      }

      const restart = code === DisconnectReason.restartRequired
      // restartRequired right after pairing is part of the normal login handshake, not an outage
      if (!restart) await this.setStatus('reconnecting', `${code ?? ''} ${message}`.trim())
      this.scheduleReconnect(restart)
    }
  }

  /** Ends the session and flips the device to "stopped" so no worker picks it up again until asked. */
  private async terminate(status: string, reason: string): Promise<void> {
    this.stopped = true
    this.state = 'closed'
    clearTimeout(this.reconnectTimer)
    try {
      await this.sock?.end(undefined)
    } catch {}
    await this.redis.del(keys.qr(this.deviceId)).catch(() => {})
    await this.setStatus(status, reason, { clearWorker: true, stopDevice: true })
    this.log.warn({ status, reason }, 'session terminated')
    this.hooks.onTerminal(this.deviceId)
  }

  private async setStatus(
    status: string,
    reason: string | null,
    opts: { jid?: string; lid?: string | null; name?: string | null; clearWorker?: boolean; stopDevice?: boolean } = {},
  ): Promise<void> {
    // one event per transition, not one per reconnect attempt
    const changed = status !== this.lastStatus
    this.lastStatus = status
    const emitted = await tx(async (client) => {
      await client.query(
        `UPDATE devices SET status = $2, status_reason = $3, updated_at = now(),
                worker_id = CASE WHEN $4 THEN NULL ELSE $5 END,
                desired_state = CASE WHEN $6 THEN 'stopped' ELSE desired_state END,
                wa_jid = COALESCE($7, wa_jid), wa_lid = COALESCE($8, wa_lid), wa_name = COALESCE($9, wa_name),
                connected_at = CASE WHEN $2 = 'connected' THEN now() ELSE connected_at END
          WHERE id = $1`,
        [
          this.deviceId,
          status,
          reason,
          !!opts.clearWorker,
          config.instanceId,
          !!opts.stopDevice,
          opts.jid ?? null,
          opts.lid ?? null,
          opts.name ?? null,
        ],
      )
      if (!changed) return null
      return emitEvent(client, this.deviceId, 'connection.update', {
        status,
        reason: reason ?? undefined,
        jid: opts.jid ?? (this.meJid || undefined),
        name: opts.name ?? undefined,
      })
    })
    if (emitted) await enqueueDeliveries(emitted.deliveryIds)
  }

  private async emit(type: EventType, data: unknown): Promise<EmittedEvent> {
    const emitted = await tx((client) => emitEvent(client, this.deviceId, type, data))
    await enqueueDeliveries(emitted.deliveryIds)
    return emitted
  }

  // ───────────────────────────── inbound ─────────────────────────────

  /**
   * WhatsApp considers a message delivered once Baileys has it, so a failure here cannot be
   * re-requested from the server. Each handler therefore retries hard before giving up.
   */
  private async guarded(what: string, fn: () => Promise<void>, context?: unknown): Promise<void> {
    for (let attempt = 1; ; attempt++) {
      try {
        return await fn()
      } catch (err) {
        if (attempt >= 6) {
          this.log.error({ err, what, context }, 'EVENT DROPPED after retries')
          return
        }
        this.log.warn({ err, what, attempt }, 'event handler failed; retrying')
        await delay(Math.min(1000 * 2 ** (attempt - 1), 15_000))
      }
    }
  }

  private async onEvents(events: Partial<BaileysEventMap>): Promise<void> {
    const upsert = events['messages.upsert']
    if (upsert) {
      // both "notify" (live) and "append" (delivered while we were offline) carry real messages
      for (const msg of upsert.messages) {
        await this.guarded('messages.upsert', () => this.handleMessage(msg), { key: msg.key })
      }
    }
    for (const item of events['messages.update'] ?? []) {
      await this.guarded('messages.update', () => this.handleMessageUpdate(item.key, item.update), { key: item.key })
    }
    for (const item of events['messages.reaction'] ?? []) {
      await this.guarded('messages.reaction', async () => {
        await this.applyReaction(item.key, item.reaction)
      })
    }
    for (const item of events['message-receipt.update'] ?? []) {
      await this.guarded('message-receipt.update', async () => {
        const r = item.receipt
        await this.emit('message.receipt', {
          messageId: item.key.id,
          chat: item.key.remoteJid,
          user: r.userJid,
          status: r.playedTimestamp ? 'played' : r.readTimestamp ? 'read' : 'delivered',
        })
      })
    }
    for (const update of events['groups.update'] ?? []) {
      if (update.id) this.groupCache.delete(update.id)
      await this.guarded('groups.update', async () => void (await this.emit('group.updated', update)))
    }
    const participants = events['group-participants.update']
    if (participants) {
      this.groupCache.delete(participants.id)
      await this.guarded('group-participants.update', async () => {
        await this.emit('group.participants', { ...participants, group: participants.id })
      })
    }
    const joinRequest = events['group.join-request']
    if (joinRequest) {
      await this.guarded('group.join-request', async () => {
        await this.emit('group.join_request', { ...joinRequest, group: joinRequest.id })
      })
    }
    for (const call of events.call ?? []) {
      await this.guarded('call', async () => void (await this.emit('call.update', call)))
    }
    const presence = events['presence.update']
    if (presence) {
      await this.guarded('presence.update', () => this.handlePresence(presence))
    }
  }

  /** Idempotent: the unique key on (device, chat, id, fromMe) makes redeliveries a no-op. */
  async handleMessage(msg: WAMessage): Promise<void> {
    const key = msg.key
    if (!key?.remoteJid || !key.id) return
    if (config.worker.ignoreStatusBroadcast && isJidStatusBroadcast(key.remoteJid)) return
    const norm = normalizeMessage(msg, this.meJid, this.meLid)
    if (!norm) return
    if (!norm.fromMe && this.settings.presenceEvents) void this.subscribePresence(norm.chat)

    const result = await tx(async (client) => {
      if (norm.fromMe) {
        const out = await client.query('SELECT id FROM outbound_messages WHERE device_id = $1 AND wa_message_id = $2', [
          this.deviceId,
          norm.id,
        ])
        if (out.rowCount) norm.outboundId = out.rows[0].id
      }
      // media we sent through the API is not downloaded back: the caller already has it
      const wantsMedia = !!norm.media && config.media.download && !norm.outboundId
      if (norm.media && !wantsMedia) norm.media.status = 'not_downloaded'

      const inserted = await client.query(
        `INSERT INTO messages (device_id, chat_jid, message_id, from_me, sender_jid, type, status, ts, payload, raw, pending_event)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11)
         ON CONFLICT (device_id, chat_jid, message_id, from_me) DO NOTHING
         RETURNING id`,
        [
          this.deviceId,
          norm.chat,
          norm.id,
          norm.fromMe,
          norm.sender,
          norm.type,
          norm.fromMe ? 'sent' : null,
          norm.timestamp,
          toJsonb(norm),
          toJsonb(msg, BufferJSON.replacer),
          wantsMedia,
        ],
      )
      if (!inserted.rowCount) return null
      const rowId: number = inserted.rows[0].id
      if (wantsMedia) return { rowId, emitted: null }
      return { rowId, emitted: await emitEvent(client, this.deviceId, this.eventTypeFor(norm), norm) }
    })

    if (!result) return // seen before
    metrics.messagesReceived.inc({ type: norm.type })
    if (result.emitted) await enqueueDeliveries(result.emitted.deliveryIds)
    else await this.finalizeMedia(result.rowId, msg, norm)
  }

  // ───────────────────────────── reactions ─────────────────────────────

  /**
   * Keeps the current reaction of each person per message. Only real changes become events, so
   * redeliveries and history replays stay silent, and a late-arriving older reaction cannot undo a newer one.
   */
  private async applyReaction(target: WAMessageKey, reaction: proto.IReaction): Promise<void> {
    const rk = reaction.key
    if (!target.id || !target.remoteJid) return
    const fromMe = !!rk?.fromMe
    const sender = fromMe ? this.meJid : jidNormalizedUser(rk?.participant ?? rk?.remoteJid ?? target.remoteJid)
    const senderAlt = fromMe ? undefined : ((rk as WAMessageKey | undefined)?.participantAlt ?? (rk as WAMessageKey | undefined)?.remoteJidAlt ?? undefined)
    const emoji = reaction.text || null
    const millis = Number(reaction.senderTimestampMs ?? 0)
    const at = new Date(millis > 0 ? millis : Date.now())

    const emitted = await tx(async (client) => {
      const current = await client.query(
        `SELECT emoji, ts FROM message_reactions
          WHERE device_id = $1 AND chat_jid = $2 AND message_id = $3 AND sender_jid = $4 FOR UPDATE`,
        [this.deviceId, target.remoteJid, target.id, sender],
      )
      const previous: string | null = current.rows[0]?.emoji ?? null
      if (previous === emoji) return null
      if (current.rowCount && current.rows[0].ts > at) return null
      if (emoji) {
        await client.query(
          `INSERT INTO message_reactions (device_id, chat_jid, message_id, sender_jid, sender_alt, from_me, emoji, ts)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
           ON CONFLICT (device_id, chat_jid, message_id, sender_jid)
           DO UPDATE SET emoji = EXCLUDED.emoji, ts = EXCLUDED.ts, sender_alt = COALESCE(EXCLUDED.sender_alt, message_reactions.sender_alt)`,
          [this.deviceId, target.remoteJid, target.id, sender, senderAlt ?? null, fromMe, emoji, at],
        )
      } else {
        await client.query(
          'DELETE FROM message_reactions WHERE device_id = $1 AND chat_jid = $2 AND message_id = $3 AND sender_jid = $4',
          [this.deviceId, target.remoteJid, target.id, sender],
        )
      }
      return emitEvent(client, this.deviceId, 'message.reaction', {
        messageId: target.id,
        chat: target.remoteJid,
        messageFromMe: !!target.fromMe,
        emoji,
        previousEmoji: previous ?? undefined,
        removed: !emoji,
        fromMe,
        sender,
        senderAlt,
        timestamp: at.toISOString(),
      })
    })
    if (emitted) await enqueueDeliveries(emitted.deliveryIds)
  }

  // ───────────────────────────── presence ─────────────────────────────

  private async loadSettings(): Promise<void> {
    const res = await query('SELECT settings FROM devices WHERE id = $1', [this.deviceId])
    this.settings = res.rows[0]?.settings ?? {}
  }

  /** Called when the settings change while the device is running. */
  async reloadSettings(): Promise<void> {
    const before = !!this.settings.presenceEvents
    await this.loadSettings()
    if (!this.isOpen || before === !!this.settings.presenceEvents) return
    if (this.settings.presenceEvents) await this.restorePresence()
    else if (!config.worker.markOnlineOnConnect) await this.sock?.sendPresenceUpdate('unavailable').catch(() => {})
  }

  /** WhatsApp forgets presence subscriptions when the socket closes, so they are replayed on every connection. */
  private async restorePresence(): Promise<void> {
    const sock = this.sock
    if (!sock) return
    this.presenceSubscribed.clear()
    this.lastPresence.clear()
    try {
      if (this.settings.presenceEvents) await sock.sendPresenceUpdate('available')
      const saved = await query('SELECT jid FROM presence_subscriptions WHERE device_id = $1 ORDER BY touched_at DESC LIMIT $2', [
        this.deviceId,
        config.presence.resubscribeLimit,
      ])
      for (const row of saved.rows) {
        if (sock !== this.sock || !this.isOpen) return
        await sock.presenceSubscribe(row.jid)
        this.presenceSubscribed.add(row.jid)
        await delay(100) // a burst of subscriptions looks like abuse
      }
    } catch (err: any) {
      this.log.warn({ err: err?.message }, 'could not restore presence subscriptions')
    }
  }

  /** Remembers the chat and asks WhatsApp for its typing/online updates. Failures are harmless: the next message tries again. */
  async subscribePresence(jid: string): Promise<void> {
    if (this.presenceSubscribed.has(jid)) return
    this.presenceSubscribed.add(jid)
    try {
      await query(
        `INSERT INTO presence_subscriptions (device_id, jid) VALUES ($1, $2)
         ON CONFLICT (device_id, jid) DO UPDATE SET touched_at = now()`,
        [this.deviceId, jid],
      )
      await this.sock?.presenceSubscribe(jid)
    } catch (err: any) {
      this.presenceSubscribed.delete(jid)
      this.log.debug({ err: err?.message, jid }, 'presence subscribe failed')
    }
  }

  private async handlePresence(update: BaileysEventMap['presence.update']): Promise<void> {
    for (const [participant, data] of Object.entries(update.presences)) {
      const state = data.lastKnownPresence
      const cacheKey = `${update.id}|${participant}`
      const last = this.lastPresence.get(cacheKey)
      // WhatsApp repeats "composing" while the person keeps typing; one event per burst is enough
      if (last && last.state === state && Date.now() - last.at < 10_000) continue
      if (this.lastPresence.size > 5000) this.lastPresence.clear()
      this.lastPresence.set(cacheKey, { state, at: Date.now() })
      await this.emit('presence.update', {
        chat: update.id,
        chatAlt: await this.altJid(update.id),
        participant,
        participantAlt: participant === update.id ? undefined : await this.altJid(participant),
        isGroup: update.id.endsWith('@g.us'),
        state,
        typing: state === 'composing',
        recording: state === 'recording',
        lastSeen: data.lastSeen ? new Date(data.lastSeen * 1000).toISOString() : undefined,
        timestamp: new Date().toISOString(),
      })
    }
  }

  /** The phone-number form of a LID (or the reverse), when Baileys knows the mapping. */
  private async altJid(jid: string): Promise<string | undefined> {
    try {
      const map = this.sock?.signalRepository?.lidMapping
      if (!map) return undefined
      if (jid.endsWith('@lid')) return (await map.getPNForLID(jid)) ?? undefined
      if (jid.endsWith('@s.whatsapp.net')) return (await map.getLIDForPN(jid)) ?? undefined
    } catch {}
    return undefined
  }

  private eventTypeFor(norm: NormalizedMessage): EventType {
    return norm.fromMe ? 'message.sent' : 'message.received'
  }

  /**
   * Second half of a media message: the row is already durable (pending_event = true),
   * so a crash during the download is picked up again by recoverPending().
   */
  private async finalizeMedia(rowId: number, msg: WAMessage, norm: NormalizedMessage): Promise<void> {
    norm.media = await this.downloadMedia(msg, norm)
    const emitted = await tx(async (client) => {
      const updated = await client.query(
        'UPDATE messages SET payload = $2::jsonb, pending_event = false WHERE id = $1 AND pending_event RETURNING id',
        [rowId, toJsonb(norm)],
      )
      if (!updated.rowCount) return null
      return emitEvent(client, this.deviceId, this.eventTypeFor(norm), norm)
    })
    if (emitted) await enqueueDeliveries(emitted.deliveryIds)
  }

  private async recoverPending(): Promise<void> {
    const rows = await query(
      'SELECT id, raw::text AS raw, payload FROM messages WHERE device_id = $1 AND pending_event ORDER BY id',
      [this.deviceId],
    )
    if (rows.rowCount) this.log.info({ count: rows.rowCount }, 'recovering messages left pending by a previous run')
    for (const row of rows.rows) {
      const msg: WAMessage = JSON.parse(row.raw, BufferJSON.reviver)
      await this.guarded('recover', () => this.finalizeMedia(row.id, msg, row.payload))
    }
  }

  async downloadMedia(msg: WAMessage, norm: NormalizedMessage): Promise<MediaInfo> {
    const info: MediaInfo = { ...(norm.media as MediaInfo) }
    if (info.size && info.size > config.media.maxBytes) return { ...info, status: 'too_large' }
    const sock = this.sock
    if (!sock) return { ...info, status: 'failed', error: 'device not connected' }

    const month = norm.timestamp.slice(0, 7)
    const objectKey = `${this.deviceId}/${month}/${norm.id}.${extensionFor(info.mimetype, info.fileName)}`
    let lastError = ''
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        await withTimeout(
          (async () => {
            const stream = await downloadMediaMessage(
              unwrapLottie(msg),
              'stream',
              {},
              { logger: this.log.child({ module: 'media' }, { level: 'warn' }) as any, reuploadRequest: sock.updateMediaMessage },
            )
            await putObject(objectKey, stream, info.mimetype?.split(';')[0])
          })(),
          config.media.timeoutMs,
          'media download',
        )
        const { size } = await statObject(objectKey)
        metrics.mediaDownloads.inc({ result: 'stored' })
        return { ...info, size, status: 'stored', key: objectKey, url: await presignedUrl(objectKey), error: undefined }
      } catch (err: any) {
        lastError = err?.message ?? String(err)
        this.log.warn({ err: lastError, attempt, messageId: norm.id }, 'media download failed')
      }
    }
    metrics.mediaDownloads.inc({ result: 'failed' })
    return { ...info, status: 'failed', error: lastError }
  }

  private async handleMessageUpdate(key: WAMessageKey, update: Partial<WAMessage>): Promise<void> {
    if (!key.id || !key.remoteJid) return
    const base = { id: key.id, chat: key.remoteJid, fromMe: !!key.fromMe }

    if (update.status !== null && update.status !== undefined) {
      const status = STATUS_NAMES[update.status]
      if (status && status !== 'pending') await this.applyStatus(key, status, update)
    }

    const revoked = update.messageStubType === proto.WebMessageInfo.StubType.REVOKE || update.message === null
    if (revoked) {
      await this.emit('message.deleted', base)
      return
    }

    const edited = update.message?.editedMessage?.message
    if (edited) {
      await this.emit('message.edited', { ...base, text: textOf(edited) })
    }

    if (update.pollUpdates?.length) {
      const creation = await this.loadStoredMessage(key.id, key.remoteJid)
      if (creation?.message) {
        const votes = getAggregateVotesInPollMessage({ message: creation.message, pollUpdates: update.pollUpdates }, this.meJid)
        await this.emit('message.poll_vote', { ...base, votes })
      }
    }
  }

  private async applyStatus(key: WAMessageKey, status: string, update: Partial<WAMessage>): Promise<void> {
    const emitted = await tx(async (client) => {
      const current = await client.query(
        'SELECT id, status FROM messages WHERE device_id = $1 AND message_id = $2 AND chat_jid = $3 AND from_me = $4 FOR UPDATE',
        [this.deviceId, key.id, key.remoteJid, !!key.fromMe],
      )
      const previous: string | null = current.rows[0]?.status ?? null
      // receipts can arrive out of order or twice; only ever move forward
      if (previous && (STATUS_RANK[status] ?? 0) <= (STATUS_RANK[previous] ?? 0)) return []
      if (current.rowCount) await client.query('UPDATE messages SET status = $2 WHERE id = $1', [current.rows[0].id, status])

      let outboundId: string | undefined
      const events: EmittedEvent[] = []
      if (key.fromMe) {
        const error = status === 'error' ? `rejected by WhatsApp (${(update.messageStubParameters ?? []).join(', ') || 'unknown'})` : null
        const out = await client.query(
          `UPDATE outbound_messages
              SET ack = $3,
                  status = CASE WHEN $3 = 'error' THEN 'failed' ELSE status END,
                  last_error = COALESCE($4, last_error)
            WHERE device_id = $1 AND wa_message_id = $2
            RETURNING id, to_jid`,
          [this.deviceId, key.id, status, error],
        )
        outboundId = out.rows[0]?.id
        if (outboundId && status === 'error') {
          metrics.messagesFailed.inc()
          events.push(await emitEvent(client, this.deviceId, 'message.failed', { outboundId, to: out.rows[0].to_jid, error }))
        }
      }
      events.push(
        await emitEvent(client, this.deviceId, 'message.status', {
          id: key.id,
          chat: key.remoteJid,
          fromMe: !!key.fromMe,
          status,
          outboundId,
        }),
      )
      return events
    })
    await enqueueDeliveries(emitted.flatMap((e) => e.deliveryIds))
  }

  async loadStoredMessage(messageId: string, chatJid?: string): Promise<WAMessage | undefined> {
    const res = await query(
      `SELECT raw::text AS raw FROM messages
        WHERE device_id = $1 AND message_id = $2
        ORDER BY (chat_jid = $3) DESC LIMIT 1`,
      [this.deviceId, messageId, chatJid ?? ''],
    )
    return res.rowCount ? JSON.parse(res.rows[0].raw, BufferJSON.reviver) : undefined
  }

  // ───────────────────────────── groups cache ─────────────────────────────

  private cachedGroup(jid: string): GroupMetadata | undefined {
    const hit = this.groupCache.get(jid)
    if (!hit) return undefined
    if (Date.now() - hit.at > 5 * 60_000) {
      this.groupCache.delete(jid)
      return undefined
    }
    return hit.data
  }

  rememberGroup(data: GroupMetadata): void {
    this.groupCache.set(data.id, { data, at: Date.now() })
  }

  // ───────────────────────────── outbound ─────────────────────────────

  /**
   * Sends everything that is due for this device, oldest first. The queue lives in Postgres,
   * so it survives restarts of this worker and of Redis; the row is only marked "sent" after
   * Baileys accepted it, and a retry reuses the same WhatsApp message id so the recipient
   * never sees a duplicate.
   */
  async drain(): Promise<void> {
    if (this.draining || !this.isOpen) return
    this.draining = true
    try {
      while (this.isOpen) {
        const res = await query<OutboundRow>(
          `SELECT id, device_id, to_jid, type, content, wa_message_id, attempts
             FROM outbound_messages
            WHERE device_id = $1 AND status IN ('queued', 'sending') AND next_attempt_at <= now()
            ORDER BY seq LIMIT 1`,
          [this.deviceId],
        )
        const row = res.rows[0]
        if (!row) break
        await this.sendOne(row)
        if (config.send.intervalMs > 0) await delay(config.send.intervalMs)
      }
    } catch (err) {
      this.log.error({ err }, 'send queue drain failed')
    } finally {
      this.draining = false
    }
  }

  /**
   * Phone numbers are resolved to the JID WhatsApp actually has on file. This matters in Brazil,
   * where an account may be registered with or without the ninth digit: sending to the other
   * form is accepted by the server and silently never arrives.
   */
  private async resolveRecipient(jid: string): Promise<string> {
    if (!config.send.checkNumber || !jid.endsWith('@s.whatsapp.net')) return jid
    const cached = this.resolvedJids.get(jid)
    if (cached) return cached
    let found: { jid: string; exists: boolean }[] | undefined
    try {
      found = await withTimeout(this.sock!.onWhatsApp(jid), 20_000, 'number lookup')
    } catch (err: any) {
      this.log.warn({ err: err?.message, jid }, 'number lookup failed; sending to the number as given')
      return jid
    }
    if (!found) return jid
    const hit = found.find((f) => f.exists)
    if (!hit) throw new PermanentError(`${jid.split('@')[0]} is not on WhatsApp`)
    if (this.resolvedJids.size > 5000) this.resolvedJids.clear()
    this.resolvedJids.set(jid, hit.jid)
    return hit.jid
  }

  private async sendOne(row: OutboundRow): Promise<void> {
    const sock = this.sock!
    const waId = row.wa_message_id ?? generateMessageIDV2(sock.user?.id)
    // the WhatsApp id is persisted BEFORE sending: if we crash mid-send, the retry is recognisably the same message
    await query(
      `UPDATE outbound_messages SET status = 'sending', wa_message_id = $2, attempts = attempts + 1 WHERE id = $1`,
      [row.id, waId],
    )
    try {
      const { content, options } = await buildContent(row, (id, chat) => this.loadStoredMessage(id, chat))
      const recipient = await this.resolveRecipient(row.to_jid)
      const sent = await withTimeout(
        sock.sendMessage(recipient, content, { ...options, messageId: waId }),
        config.send.timeoutMs,
        'send',
      )
      await query(`UPDATE outbound_messages SET status = 'sent', sent_at = now(), last_error = NULL WHERE id = $1`, [row.id])
      metrics.messagesSent.inc({ type: row.type })
      if (sent) await this.guarded('store sent message', () => this.handleMessage(sent), { key: sent.key })
    } catch (err: any) {
      const message: string = err?.message ?? String(err)
      if (!this.isOpen) {
        // the connection dropped under us: not the message's fault, so the attempt is not counted
        await query(`UPDATE outbound_messages SET status = 'queued', attempts = GREATEST(attempts - 1, 0), last_error = $2 WHERE id = $1`, [
          row.id,
          message,
        ])
        return
      }
      metrics.sendAttemptsFailed.inc()
      const attempts = row.attempts + 1
      this.log.warn({ err: message, outboundId: row.id, attempts }, 'send failed')
      if (err instanceof PermanentError || err instanceof StickerError || attempts >= config.send.maxAttempts) {
        const emitted = await tx(async (client) => {
          await client.query(`UPDATE outbound_messages SET status = 'failed', last_error = $2 WHERE id = $1`, [row.id, message])
          return emitEvent(client, this.deviceId, 'message.failed', {
            outboundId: row.id,
            to: row.to_jid,
            type: row.type,
            error: message,
            attempts,
          })
        })
        metrics.messagesFailed.inc()
        await enqueueDeliveries(emitted.deliveryIds)
      } else {
        const backoffSeconds = Math.min(5 * 2 ** (attempts - 1), 300)
        await query(
          `UPDATE outbound_messages
              SET status = 'queued', last_error = $2, next_attempt_at = now() + make_interval(secs => $3)
            WHERE id = $1`,
          [row.id, message, backoffSeconds],
        )
      }
    }
  }
}
