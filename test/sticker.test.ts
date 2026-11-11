import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import sharp from 'sharp'
import { normalizeMessage } from '../src/worker/normalize.js'
import { sniff, StickerError, toSticker, webpChunks } from '../src/worker/sticker.js'

const ME = '5511900000000@s.whatsapp.net'

const solid = (width: number, height: number, color: string) =>
  sharp({ create: { width, height, channels: 4, background: color } })

async function animatedGif(frames = 6): Promise<Buffer> {
  const colors = ['#e11', '#1a1', '#11e', '#ee1', '#1ee', '#e1e', '#888', '#fff']
  const pages = await Promise.all(
    Array.from({ length: frames }, (_, i) => solid(200, 120, colors[i % colors.length]).png().toBuffer()),
  )
  return sharp(pages, { join: { animated: true } }).gif({ delay: 100 }).toBuffer()
}

const hasFfmpeg = (() => {
  try {
    execFileSync('ffmpeg', ['-version'], { stdio: 'ignore' })
    return true
  } catch {
    return false
  }
})()

test('static image becomes a 512×512 WebP that keeps its proportions', async () => {
  const png = await solid(800, 300, '#c03').png().toBuffer()
  const s = await toSticker(png)
  const info = await sharp(s.buffer).metadata()
  assert.equal(s.animated, false)
  assert.equal(info.format, 'webp')
  assert.deepEqual([info.width, info.height], [512, 512])
  assert.ok(info.hasAlpha, 'letterbox must be transparent')
  assert.ok(s.buffer.length <= 100 * 1024)
  // the bars above and below the wide image are transparent, the middle is not
  const { data } = await sharp(s.buffer).ensureAlpha().raw().toBuffer({ resolveWithObject: true })
  assert.equal(data[3], 0)
  assert.equal(data[(256 * 512 + 256) * 4 + 3], 255)
})

test('JPEG without alpha is accepted', async () => {
  const jpg = await solid(640, 640, '#249').removeAlpha().jpeg().toBuffer()
  const s = await toSticker(jpg)
  assert.equal((await sharp(s.buffer).metadata()).format, 'webp')
})

test('animated GIF becomes an animated WebP with every frame', async () => {
  const s = await toSticker(await animatedGif(6))
  const info = await sharp(s.buffer, { animated: true }).metadata()
  assert.equal(s.animated, true)
  assert.equal(info.format, 'webp')
  assert.equal(info.pages, 6)
  assert.deepEqual([info.width, info.pageHeight], [512, 512])
  assert.ok(s.buffer.length <= 500 * 1024)
})

test('a compliant sticker passes through byte for byte', async () => {
  const webp = await solid(512, 512, '#0a5').webp().toBuffer()
  const s = await toSticker(webp)
  assert.ok(s.buffer.equals(webp))
})

test('pack metadata is written as EXIF and the file stays readable', async () => {
  for (const source of [await solid(512, 512, '#0a5').webp().toBuffer(), await animatedGif(3)]) {
    const s = await toSticker(source, { pack: 'Meu Pacote', author: 'Jingg', emojis: ['😀'] })
    const chunks = webpChunks(s.buffer)
    assert.equal(chunks[0].id, 'VP8X')
    assert.ok(chunks[0].data[0] & 0x08, 'EXIF flag')
    const exif = chunks.find((c) => c.id === 'EXIF')!
    const json = JSON.parse(exif.data.subarray(22).toString('utf8'))
    assert.equal(exif.data.readUInt32LE(14), exif.data.length - 22)
    assert.equal(json['sticker-pack-name'], 'Meu Pacote')
    assert.equal(json['sticker-pack-publisher'], 'Jingg')
    assert.deepEqual(json.emojis, ['😀'])
    assert.equal(s.buffer.readUInt32LE(4), s.buffer.length - 8)
    const info = await sharp(s.buffer, { animated: true }).metadata()
    assert.deepEqual([info.width, info.pageHeight ?? info.height], [512, 512])
    assert.equal((info.pages ?? 1) > 1, s.animated)
  }
})

test('unsupported sources fail permanently', async () => {
  await assert.rejects(toSticker(Buffer.from('this is definitely not an image')), StickerError)
  await assert.rejects(toSticker(Buffer.concat([Buffer.from('PK\x03\x04'), Buffer.alloc(64)])), /Lottie/)
  assert.equal(sniff(Buffer.concat([Buffer.from([0, 0, 0, 0x20]), Buffer.from('ftypisom'), Buffer.alloc(16)])), 'video')
})

test('video becomes an animated sticker', { skip: !hasFfmpeg && 'ffmpeg not installed' }, async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zapi-test-'))
  try {
    const mp4 = join(dir, 'clip.mp4')
    execFileSync('ffmpeg', ['-loglevel', 'error', '-f', 'lavfi', '-i', 'testsrc=duration=2:size=320x180:rate=24', '-pix_fmt', 'yuv420p', mp4])
    const s = await toSticker(readFileSync(mp4))
    const info = await sharp(s.buffer, { animated: true }).metadata()
    assert.equal(s.animated, true)
    assert.deepEqual([info.width, info.pageHeight], [512, 512])
    assert.ok((info.pages ?? 0) >= 20, `expected ~24 frames, got ${info.pages}`)
    assert.ok(s.buffer.length <= 1024 * 1024)
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

const incoming = (message: any) =>
  ({ key: { remoteJid: '5511988887777@s.whatsapp.net', id: 'S1', fromMe: false }, messageTimestamp: 1_760_000_000, message }) as any

test('received stickers: static, animated, Lottie and pack', () => {
  const sticker = { mimetype: 'image/webp', fileLength: 9000, width: 512, height: 512 }
  const plain = normalizeMessage(incoming({ stickerMessage: sticker }), ME)!
  assert.equal(plain.type, 'sticker')
  assert.equal(plain.media?.animated, undefined)

  const animated = normalizeMessage(incoming({ stickerMessage: { ...sticker, isAnimated: true } }), ME)!
  assert.equal(animated.media?.animated, true)
  assert.equal(animated.media?.lottie, undefined)

  const lottie = normalizeMessage(
    incoming({ lottieStickerMessage: { message: { stickerMessage: { ...sticker, mimetype: 'application/was', isLottie: true } } } }),
    ME,
  )!
  assert.equal(lottie.type, 'sticker')
  assert.equal(lottie.media?.lottie, true)
  assert.equal(lottie.media?.animated, true)

  const pack = normalizeMessage(incoming({ stickerPackMessage: { stickerPackId: 'p1', name: 'Gatos', publisher: 'Eu', stickers: [{}, {}] } }), ME)!
  assert.equal(pack.type, 'sticker_pack')
  assert.deepEqual(pack.stickerPack, { id: 'p1', name: 'Gatos', publisher: 'Eu', count: 2 })
})
