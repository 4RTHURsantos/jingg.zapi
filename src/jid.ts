/**
 * Accepts a phone number ("+55 11 99999-0000", "5511999990000") or a full JID
 * ("...@s.whatsapp.net", "...@g.us", "...@lid") and returns a JID.
 */
export function toJid(input: string): string {
  const value = input.trim()
  if (value.includes('@')) return value
  const digits = value.replace(/\D/g, '')
  if (!digits) throw new Error(`invalid recipient: "${input}"`)
  return `${digits}@s.whatsapp.net`
}

export const isGroupJid = (jid: string) => jid.endsWith('@g.us')
