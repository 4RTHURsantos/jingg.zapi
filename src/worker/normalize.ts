import { getContentType, normalizeMessageContent, proto, toNumber, type WAMessage } from 'baileys'
import { isGroupJid } from '../jid.js'

export interface MediaInfo {
  mimetype?: string
  size?: number
  fileName?: string
  seconds?: number
  width?: number
  height?: number
  ptt?: boolean
  animated?: boolean
  /** vector (Lottie) sticker: the stored file is a .was archive, not a WebP */
  lottie?: boolean
  /** pending → stored | failed | too_large | not_downloaded */
  status: string
  key?: string
  url?: string
  error?: string
}

export interface NormalizedMessage {
  id: string
  chat: string
  /** the other addressing form of the chat (phone JID when `chat` is a LID, and vice versa), when known */
  chatAlt?: string
  sender: string
  senderAlt?: string
  fromMe: boolean
  isGroup: boolean
  pushName?: string
  timestamp: string
  type: string
  /** raw Baileys content key, useful when type is "unknown" */
  contentType: string
  text?: string
  media?: MediaInfo
  location?: { latitude?: number; longitude?: number; name?: string; address?: string; live?: boolean }
  contacts?: { name?: string; vcard?: string }[]
  poll?: { name?: string; options: string[]; selectableCount?: number }
  selection?: { id?: string; text?: string }
  stickerPack?: { id?: string; name?: string; publisher?: string; count?: number }
  quoted?: { id: string; sender?: string }
  mentions?: string[]
  viewOnce?: boolean
  forwarded?: boolean
  /** set when the message was sent through this API */
  outboundId?: string
}

const MEDIA_TYPES: Record<string, string> = {
  imageMessage: 'image',
  videoMessage: 'video',
  ptvMessage: 'video',
  audioMessage: 'audio',
  documentMessage: 'document',
  stickerMessage: 'sticker',
}

/** content that is plumbing rather than a message a human would see; handled through other events */
const SKIPPED = new Set([
  'protocolMessage',
  'reactionMessage',
  'pollUpdateMessage',
  'senderKeyDistributionMessage',
  'messageContextInfo',
  'keepInChatMessage',
  'encReactionMessage',
  'encEventResponseMessage',
  'associatedChildMessage',
])

const num = (v: unknown): number | undefined => {
  if (v === null || v === undefined) return undefined
  const n = toNumber(v as any)
  return Number.isFinite(n) ? n : undefined
}

export function textOf(content: proto.IMessage | undefined | null): string | undefined {
  if (!content) return undefined
  return (
    content.conversation ??
    content.extendedTextMessage?.text ??
    content.imageMessage?.caption ??
    content.videoMessage?.caption ??
    content.documentMessage?.caption ??
    undefined
  ) || undefined
}

/**
 * Lottie stickers arrive wrapped in a container Baileys does not open, which would make them
 * surface as "unknown" and fail to download.
 */
export function unwrapLottie(msg: WAMessage): WAMessage {
  const inner = normalizeMessageContent(msg.message)?.lottieStickerMessage?.message
  return inner ? { ...msg, message: inner } : msg
}

/** Returns null for anything that should not surface as a message event. */
export function normalizeMessage(msg: WAMessage, meJid: string): NormalizedMessage | null {
  const key = msg.key
  if (!key?.remoteJid || !key.id || !msg.message) return null

  const content = normalizeMessageContent(unwrapLottie(msg).message)
  const contentType = getContentType(content)
  if (!content || !contentType || SKIPPED.has(contentType)) return null

  const chat = key.remoteJid
  const isGroup = isGroupJid(chat)
  const fromMe = !!key.fromMe
  const sender = fromMe ? meJid : isGroup ? (key.participant ?? chat) : chat
  const senderAlt = fromMe ? undefined : isGroup ? key.participantAlt : key.remoteJidAlt
  const seconds = num(msg.messageTimestamp)

  const out: NormalizedMessage = {
    id: key.id,
    chat,
    chatAlt: key.remoteJidAlt || undefined,
    sender,
    senderAlt: senderAlt || undefined,
    fromMe,
    isGroup,
    pushName: msg.pushName || undefined,
    timestamp: new Date(seconds ? seconds * 1000 : Date.now()).toISOString(),
    type: 'unknown',
    contentType,
  }

  const m = msg.message
  if (key.isViewOnce || m.viewOnceMessage || m.viewOnceMessageV2 || m.viewOnceMessageV2Extension) out.viewOnce = true

  const inner: any = (content as any)[contentType]
  const ctx: proto.IContextInfo | undefined | null = typeof inner === 'object' ? inner?.contextInfo : undefined
  if (ctx?.stanzaId) out.quoted = { id: ctx.stanzaId, sender: ctx.participant || undefined }
  if (ctx?.mentionedJid?.length) out.mentions = ctx.mentionedJid
  if (ctx?.isForwarded) out.forwarded = true

  const mediaType = MEDIA_TYPES[contentType]
  if (mediaType) {
    out.type = mediaType
    out.text = inner.caption || undefined
    out.media = {
      mimetype: inner.mimetype || undefined,
      size: num(inner.fileLength),
      fileName: inner.fileName || undefined,
      seconds: inner.seconds || undefined,
      width: inner.width || undefined,
      height: inner.height || undefined,
      ptt: inner.ptt || undefined,
      animated: inner.isAnimated || inner.isLottie || undefined,
      lottie: inner.isLottie || undefined,
      status: 'pending',
    }
    return out
  }

  switch (contentType) {
    case 'conversation':
    case 'extendedTextMessage':
      out.type = 'text'
      out.text = textOf(content)
      break
    case 'locationMessage':
    case 'liveLocationMessage':
      out.type = 'location'
      out.location = {
        latitude: inner.degreesLatitude ?? undefined,
        longitude: inner.degreesLongitude ?? undefined,
        name: inner.name || undefined,
        address: inner.address || undefined,
        live: contentType === 'liveLocationMessage' || undefined,
      }
      break
    case 'contactMessage':
      out.type = 'contact'
      out.contacts = [{ name: inner.displayName || undefined, vcard: inner.vcard || undefined }]
      break
    case 'contactsArrayMessage':
      out.type = 'contact'
      out.contacts = (inner.contacts ?? []).map((c: proto.Message.IContactMessage) => ({
        name: c.displayName || undefined,
        vcard: c.vcard || undefined,
      }))
      break
    case 'pollCreationMessage':
    case 'pollCreationMessageV2':
    case 'pollCreationMessageV3':
      out.type = 'poll'
      out.poll = {
        name: inner.name || undefined,
        options: (inner.options ?? []).map((o: proto.Message.PollCreationMessage.IOption) => o.optionName ?? ''),
        selectableCount: inner.selectableOptionsCount ?? undefined,
      }
      break
    case 'buttonsResponseMessage':
      out.type = 'button_reply'
      out.selection = { id: inner.selectedButtonId || undefined, text: inner.selectedDisplayText || undefined }
      out.text = inner.selectedDisplayText || undefined
      break
    case 'templateButtonReplyMessage':
      out.type = 'button_reply'
      out.selection = { id: inner.selectedId || undefined, text: inner.selectedDisplayText || undefined }
      out.text = inner.selectedDisplayText || undefined
      break
    case 'listResponseMessage':
      out.type = 'list_reply'
      out.selection = { id: inner.singleSelectReply?.selectedRowId || undefined, text: inner.title || undefined }
      out.text = inner.title || undefined
      break
    case 'groupInviteMessage':
      out.type = 'group_invite'
      out.text = inner.caption || inner.groupName || undefined
      break
    case 'stickerPackMessage':
      out.type = 'sticker_pack'
      out.text = inner.name || undefined
      out.stickerPack = {
        id: inner.stickerPackId || undefined,
        name: inner.name || undefined,
        publisher: inner.publisher || undefined,
        count: inner.stickers?.length || undefined,
      }
      break
    case 'eventMessage':
      out.type = 'event'
      out.text = inner.name || undefined
      break
    default:
      out.type = 'unknown'
  }
  return out
}

const STATUS = proto.WebMessageInfo.Status
export const STATUS_NAMES: Record<number, string> = {
  [STATUS.ERROR]: 'error',
  [STATUS.PENDING]: 'pending',
  [STATUS.SERVER_ACK]: 'sent',
  [STATUS.DELIVERY_ACK]: 'delivered',
  [STATUS.READ]: 'read',
  [STATUS.PLAYED]: 'played',
}
export const STATUS_RANK: Record<string, number> = { pending: 0, sent: 1, delivered: 2, read: 3, played: 4, error: 9 }

const EXTENSIONS: Record<string, string> = {
  'image/jpeg': 'jpg',
  'image/png': 'png',
  'image/webp': 'webp',
  'image/gif': 'gif',
  'application/was': 'was',
  'video/mp4': 'mp4',
  'video/3gpp': '3gp',
  'audio/ogg': 'ogg',
  'audio/mpeg': 'mp3',
  'audio/mp4': 'm4a',
  'audio/aac': 'aac',
  'audio/wav': 'wav',
  'application/pdf': 'pdf',
  'application/zip': 'zip',
  'text/plain': 'txt',
  'text/csv': 'csv',
  'application/msword': 'doc',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
  'application/vnd.ms-excel': 'xls',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
  'application/vnd.ms-powerpoint': 'ppt',
  'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'pptx',
}

export function extensionFor(mimetype?: string, fileName?: string): string {
  const fromName = fileName?.match(/\.([A-Za-z0-9]{1,8})$/)?.[1]
  if (fromName) return fromName.toLowerCase()
  const base = mimetype?.split(';')[0]?.trim().toLowerCase()
  return (base && EXTENSIONS[base]) || 'bin'
}
