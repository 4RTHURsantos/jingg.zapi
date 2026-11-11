import assert from 'node:assert/strict'
import { test } from 'node:test'
import { toJid } from '../src/jid.js'
import { sign, verify } from '../src/signature.js'
import { extensionFor, normalizeMessage } from '../src/worker/normalize.js'

const ME = '5511900000000@s.whatsapp.net'
const base = (message: any, key: any = {}) =>
  ({
    key: { remoteJid: '5511988887777@s.whatsapp.net', id: 'ABC123', fromMe: false, ...key },
    messageTimestamp: 1_760_000_000,
    pushName: 'Maria',
    message,
  }) as any

test('toJid accepts numbers and JIDs', () => {
  assert.equal(toJid('+55 (11) 98888-7777'), '5511988887777@s.whatsapp.net')
  assert.equal(toJid('120363000000000000@g.us'), '120363000000000000@g.us')
  assert.throws(() => toJid('abc'))
})

test('plain text message', () => {
  const n = normalizeMessage(base({ conversation: 'oi' }), ME)!
  assert.equal(n.type, 'text')
  assert.equal(n.text, 'oi')
  assert.equal(n.sender, '5511988887777@s.whatsapp.net')
  assert.equal(n.isGroup, false)
  assert.equal(n.timestamp, new Date(1_760_000_000_000).toISOString())
})

test('group message keeps participant, LID alt and quote', () => {
  const n = normalizeMessage(
    base(
      { extendedTextMessage: { text: 'resposta', contextInfo: { stanzaId: 'Q1', participant: '111@lid', mentionedJid: ['222@lid'] } } },
      { remoteJid: '1203@g.us', participant: '999@lid', participantAlt: '5511977776666@s.whatsapp.net' },
    ),
    ME,
  )!
  assert.equal(n.isGroup, true)
  assert.equal(n.sender, '999@lid')
  assert.equal(n.senderAlt, '5511977776666@s.whatsapp.net')
  assert.deepEqual(n.quoted, { id: 'Q1', sender: '111@lid', fromMe: false })
  assert.deepEqual(n.mentions, ['222@lid'])
})

test('media message exposes pending media info', () => {
  const n = normalizeMessage(base({ imageMessage: { caption: 'foto', mimetype: 'image/jpeg', fileLength: 1234, width: 10, height: 20 } }), ME)!
  assert.equal(n.type, 'image')
  assert.equal(n.text, 'foto')
  assert.equal(n.media?.status, 'pending')
  assert.equal(n.media?.size, 1234)
})

test('view-once and ephemeral wrappers are unwrapped', () => {
  const n = normalizeMessage(base({ viewOnceMessageV2: { message: { imageMessage: { mimetype: 'image/jpeg' } } } }), ME)!
  assert.equal(n.type, 'image')
  assert.equal(n.viewOnce, true)
  const e = normalizeMessage(base({ ephemeralMessage: { message: { conversation: 'some' } } }), ME)!
  assert.equal(e.text, 'some')
})

test('own messages are attributed to the device', () => {
  const n = normalizeMessage(base({ conversation: 'eu' }, { fromMe: true }), ME)!
  assert.equal(n.sender, ME)
  assert.equal(n.fromMe, true)
})

test('protocol, reaction and empty messages are skipped', () => {
  assert.equal(normalizeMessage(base({ protocolMessage: { type: 0 } }), ME), null)
  assert.equal(normalizeMessage(base({ reactionMessage: { text: '👍' } }), ME), null)
  assert.equal(normalizeMessage(base(undefined), ME), null)
})

test('poll, location and contacts', () => {
  const p = normalizeMessage(base({ pollCreationMessageV3: { name: 'Qual?', options: [{ optionName: 'A' }, { optionName: 'B' }], selectableOptionsCount: 1 } }), ME)!
  assert.deepEqual(p.poll, { name: 'Qual?', options: ['A', 'B'], selectableCount: 1 })
  const l = normalizeMessage(base({ locationMessage: { degreesLatitude: -23.5, degreesLongitude: -46.6, name: 'SP' } }), ME)!
  assert.equal(l.location?.latitude, -23.5)
  const c = normalizeMessage(base({ contactsArrayMessage: { contacts: [{ displayName: 'A', vcard: 'v' }, { displayName: 'B', vcard: 'w' }] } }), ME)!
  assert.equal(c.contacts?.length, 2)
})

test('file extension from name, then mimetype', () => {
  assert.equal(extensionFor('application/pdf', 'contrato.PDF'), 'pdf')
  assert.equal(extensionFor('audio/ogg; codecs=opus'), 'ogg')
  assert.equal(extensionFor(undefined), 'bin')
})

test('webhook signature round-trips and rejects tampering', () => {
  const sig = sign('segredo', '{"a":1}')
  assert.match(sig, /^sha256=[0-9a-f]{64}$/)
  assert.ok(verify('segredo', '{"a":1}', sig))
  assert.ok(!verify('segredo', '{"a":2}', sig))
  assert.ok(!verify('outro', '{"a":1}', sig))
})

test('a reply carries the author, type and text of the quoted message', () => {
  const reply = (contextInfo: any) => normalizeMessage(base({ extendedTextMessage: { text: 'sim', contextInfo } }), ME, '777@lid')!

  const toMine = reply({ stanzaId: 'Q1', participant: '5511900000000:12@s.whatsapp.net', quotedMessage: { conversation: 'Vamos?' } })
  assert.deepEqual(toMine.quoted, { id: 'Q1', sender: ME, fromMe: true, type: 'text', text: 'Vamos?' })

  const toMineByLid = reply({ stanzaId: 'Q2', participant: '777@lid', quotedMessage: { imageMessage: { caption: 'foto' } } })
  assert.deepEqual(toMineByLid.quoted, { id: 'Q2', sender: '777@lid', fromMe: true, type: 'image', text: 'foto' })

  const toTheirs = reply({ stanzaId: 'Q3', participant: '5511988887777@s.whatsapp.net', quotedMessage: { audioMessage: { ptt: true } } })
  assert.deepEqual(toTheirs.quoted, { id: 'Q3', sender: '5511988887777@s.whatsapp.net', fromMe: false, type: 'audio', text: undefined })

  const wrapped = reply({ stanzaId: 'Q4', participant: '1@lid', quotedMessage: { ephemeralMessage: { message: { stickerMessage: {} } } } })
  assert.equal(wrapped.quoted?.type, 'sticker')
})
