import type { AnyMessageContent, MiscMessageGenerationOptions, WAMessage, WAMessageKey } from 'baileys'
import { toJid } from '../jid.js'
import { getObjectBuffer } from '../storage.js'

/** Shape stored in outbound_messages.content; validated by the API before it is queued. */
export interface OutboundContent {
  text?: string
  media?: { url?: string; key?: string }
  caption?: string
  mimetype?: string
  fileName?: string
  ptt?: boolean
  gif?: boolean
  viewOnce?: boolean
  latitude?: number
  longitude?: number
  name?: string
  address?: string
  contacts?: { name: string; phone: string; organization?: string }[]
  /** target of a reaction / edit / delete */
  messageId?: string
  emoji?: string
  targetFromMe?: boolean
  participant?: string
  poll?: { name: string; options: string[]; selectableCount?: number }
  quotedMessageId?: string
  mentions?: string[]
  mentionAll?: boolean
}

export interface OutboundRow {
  id: string
  device_id: string
  to_jid: string
  type: string
  content: OutboundContent
  wa_message_id: string | null
  attempts: number
}

export type LoadStoredMessage = (messageId: string, chatJid: string) => Promise<WAMessage | undefined>

function vcard(c: { name: string; phone: string; organization?: string }): string {
  const digits = c.phone.replace(/\D/g, '')
  return [
    'BEGIN:VCARD',
    'VERSION:3.0',
    `FN:${c.name}`,
    ...(c.organization ? [`ORG:${c.organization};`] : []),
    `TEL;type=CELL;type=VOICE;waid=${digits}:+${digits}`,
    'END:VCARD',
  ].join('\n')
}

async function mediaSource(media: OutboundContent['media']): Promise<Buffer | { url: string }> {
  if (media?.key) return getObjectBuffer(media.key)
  if (media?.url) return { url: media.url }
  throw new Error('media source missing')
}

async function targetKey(row: OutboundRow, load: LoadStoredMessage): Promise<WAMessageKey> {
  const c = row.content
  if (!c.messageId) throw new Error('messageId is required')
  const stored = await load(c.messageId, row.to_jid)
  if (stored?.key) return stored.key
  return {
    remoteJid: row.to_jid,
    id: c.messageId,
    fromMe: c.targetFromMe ?? row.type !== 'reaction',
    participant: c.participant ? toJid(c.participant) : undefined,
  }
}

export async function buildContent(
  row: OutboundRow,
  load: LoadStoredMessage,
): Promise<{ content: AnyMessageContent; options: MiscMessageGenerationOptions }> {
  const c = row.content
  const options: MiscMessageGenerationOptions = {}
  const mentions = c.mentions?.length ? c.mentions.map(toJid) : undefined
  const extra = { ...(mentions ? { mentions } : {}), ...(c.mentionAll ? { mentionAll: true } : {}) }
  const viewOnce = c.viewOnce ? { viewOnce: true } : {}

  if (c.quotedMessageId) {
    const quoted = await load(c.quotedMessageId, row.to_jid)
    // quoting is cosmetic: if the original is gone the message still goes out, just without the quote
    if (quoted) options.quoted = quoted
  }

  let content: AnyMessageContent
  switch (row.type) {
    case 'text':
      content = { text: c.text ?? '', ...extra }
      break
    case 'image':
      content = { image: await mediaSource(c.media), caption: c.caption, mimetype: c.mimetype, ...extra, ...viewOnce }
      break
    case 'video':
      content = {
        video: await mediaSource(c.media),
        caption: c.caption,
        mimetype: c.mimetype,
        gifPlayback: c.gif || undefined,
        ...extra,
        ...viewOnce,
      }
      break
    case 'audio':
      content = {
        audio: await mediaSource(c.media),
        ptt: c.ptt || undefined,
        mimetype: c.mimetype ?? (c.ptt ? 'audio/ogg; codecs=opus' : 'audio/mpeg'),
        ...viewOnce,
      }
      break
    case 'document':
      content = {
        document: await mediaSource(c.media),
        mimetype: c.mimetype ?? 'application/octet-stream',
        fileName: c.fileName,
        caption: c.caption,
      }
      break
    case 'sticker':
      content = { sticker: await mediaSource(c.media), mimetype: c.mimetype }
      break
    case 'location':
      content = {
        location: { degreesLatitude: c.latitude, degreesLongitude: c.longitude, name: c.name, address: c.address },
      }
      break
    case 'contact': {
      const list = c.contacts ?? []
      content = {
        contacts: {
          displayName: list.length === 1 ? list[0].name : `${list.length} contatos`,
          contacts: list.map((ct) => ({ displayName: ct.name, vcard: vcard(ct) })),
        },
      }
      break
    }
    case 'poll':
      content = {
        poll: {
          name: c.poll!.name,
          values: c.poll!.options,
          selectableCount: c.poll!.selectableCount ?? 1,
        },
        ...extra,
      }
      break
    case 'reaction':
      content = { react: { text: c.emoji ?? '', key: await targetKey(row, load) } }
      break
    case 'edit':
      content = { text: c.text ?? '', edit: await targetKey(row, load), ...extra }
      break
    case 'delete':
      content = { delete: await targetKey(row, load) }
      break
    default:
      throw new Error(`unsupported message type "${row.type}"`)
  }
  return { content, options }
}
