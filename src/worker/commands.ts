import { BufferJSON, type WAMessage } from 'baileys'
import { query, toJsonb } from '../db.js'
import { toJid } from '../jid.js'
import { RpcError } from '../rpc.js'
import type { NormalizedMessage } from './normalize.js'
import type { Session } from './session.js'

type Command = (session: Session, params: any) => Promise<unknown>

const open = (session: Session) => {
  if (!session.isOpen || !session.socket) throw new RpcError('device is not connected to WhatsApp', 409)
  return session.socket
}

/** Synchronous operations the API forwards to the worker that owns the device. */
export const commands: Record<string, Command> = {
  async pairingCode(session, { phone }) {
    const sock = session.socket
    if (!sock) throw new RpcError('device is not started', 409)
    if (sock.authState.creds.registered) throw new RpcError('device is already paired', 409)
    const code = await sock.requestPairingCode(String(phone).replace(/\D/g, ''))
    return { code }
  },

  async logout(session) {
    await open(session).logout()
    return { ok: true }
  },

  async checkNumbers(session, { numbers }) {
    const sock = open(session)
    const jids = (numbers as string[]).map((n) => toJid(n))
    const found = (await sock.onWhatsApp(...jids)) ?? []
    return (numbers as string[]).map((input, i) => {
      const hit = found.find((f) => f.jid === jids[i] || f.jid.split('@')[0] === jids[i].split('@')[0])
      return { input, exists: !!hit?.exists, jid: hit?.jid ?? null, lid: (hit as any)?.lid ?? null }
    })
  },

  async profilePicture(session, { jid }) {
    const url = await open(session)
      .profilePictureUrl(toJid(jid), 'image')
      .catch(() => undefined)
    return { url: url ?? null }
  },

  async contactStatus(session, { jid }) {
    return (await open(session).fetchStatus(toJid(jid))) ?? []
  },

  async businessProfile(session, { jid }) {
    return (await open(session).getBusinessProfile(toJid(jid))) ?? null
  },

  async block(session, { jid, action }) {
    await open(session).updateBlockStatus(toJid(jid), action)
    return { ok: true }
  },

  async presence(session, { to, state }) {
    const sock = open(session)
    await sock.sendPresenceUpdate(state, to ? toJid(to) : undefined)
    return { ok: true }
  },

  async presenceSubscribe(session, { jid }) {
    await open(session).presenceSubscribe(toJid(jid))
    return { ok: true }
  },

  async markRead(session, { messages }) {
    const sock = open(session)
    await sock.readMessages(
      (messages as { chat: string; id: string; participant?: string }[]).map((m) => ({
        remoteJid: toJid(m.chat),
        id: m.id,
        fromMe: false,
        participant: m.participant ? toJid(m.participant) : undefined,
      })),
    )
    return { ok: true }
  },

  async rejectCall(session, { callId, from }) {
    await open(session).rejectCall(callId, toJid(from))
    return { ok: true }
  },

  async updateProfile(session, { name, status }) {
    const sock = open(session)
    if (name !== undefined) await sock.updateProfileName(name)
    if (status !== undefined) await sock.updateProfileStatus(status)
    return { ok: true }
  },

  /** Retries the download of a media message whose first attempt failed (or was skipped). */
  async downloadMedia(session, { messageId }) {
    open(session)
    const res = await query(
      'SELECT id, raw::text AS raw, payload FROM messages WHERE device_id = $1 AND message_id = $2 ORDER BY id LIMIT 1',
      [session.deviceId, messageId],
    )
    if (!res.rowCount) throw new RpcError('message not found', 404)
    const payload: NormalizedMessage = res.rows[0].payload
    if (!payload.media) throw new RpcError('message has no media', 400)
    const msg: WAMessage = JSON.parse(res.rows[0].raw, BufferJSON.reviver)
    payload.media = await session.downloadMedia(msg, payload)
    await query('UPDATE messages SET payload = $2::jsonb WHERE id = $1', [res.rows[0].id, toJsonb(payload)])
    if (payload.media.status !== 'stored') throw new RpcError(`media download failed: ${payload.media.error ?? payload.media.status}`, 502)
    return payload.media
  },

  // ───────────── groups ─────────────

  async 'groups.list'(session) {
    const all = await open(session).groupFetchAllParticipating()
    return Object.values(all).map((g) => {
      session.rememberGroup(g)
      return { id: g.id, subject: g.subject, owner: g.owner, size: g.size ?? g.participants?.length, creation: g.creation, announce: g.announce, restrict: g.restrict, isCommunity: g.isCommunity }
    })
  },

  async 'groups.get'(session, { jid }) {
    const meta = await open(session).groupMetadata(jid)
    session.rememberGroup(meta)
    return meta
  },

  async 'groups.create'(session, { subject, participants }) {
    const meta = await open(session).groupCreate(subject, (participants as string[]).map(toJid))
    session.rememberGroup(meta)
    return meta
  },

  async 'groups.update'(session, { jid, subject, description, announce, locked, ephemeral, memberAddMode, joinApprovalMode }) {
    const sock = open(session)
    if (subject !== undefined) await sock.groupUpdateSubject(jid, subject)
    if (description !== undefined) await sock.groupUpdateDescription(jid, description || undefined)
    if (announce !== undefined) await sock.groupSettingUpdate(jid, announce ? 'announcement' : 'not_announcement')
    if (locked !== undefined) await sock.groupSettingUpdate(jid, locked ? 'locked' : 'unlocked')
    if (ephemeral !== undefined) await sock.groupToggleEphemeral(jid, ephemeral)
    if (memberAddMode !== undefined) await sock.groupMemberAddMode(jid, memberAddMode)
    if (joinApprovalMode !== undefined) await sock.groupJoinApprovalMode(jid, joinApprovalMode ? 'on' : 'off')
    const meta = await sock.groupMetadata(jid)
    session.rememberGroup(meta)
    return meta
  },

  async 'groups.participants'(session, { jid, action, participants }) {
    return open(session).groupParticipantsUpdate(jid, (participants as string[]).map(toJid), action)
  },

  async 'groups.inviteCode'(session, { jid }) {
    const code = await open(session).groupInviteCode(jid)
    return { code, url: code ? `https://chat.whatsapp.com/${code}` : null }
  },

  async 'groups.revokeInvite'(session, { jid }) {
    const code = await open(session).groupRevokeInvite(jid)
    return { code, url: code ? `https://chat.whatsapp.com/${code}` : null }
  },

  async 'groups.inviteInfo'(session, { code }) {
    return open(session).groupGetInviteInfo(code)
  },

  async 'groups.join'(session, { code }) {
    return { jid: (await open(session).groupAcceptInvite(code)) ?? null }
  },

  async 'groups.leave'(session, { jid }) {
    await open(session).groupLeave(jid)
    return { ok: true }
  },
}
