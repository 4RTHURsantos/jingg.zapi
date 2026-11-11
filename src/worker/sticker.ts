import { execFile } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import sharp, { type Metadata } from 'sharp'
import { config } from '../config.js'

/** The input can never become a sticker; retrying the send would not help. */
export class StickerError extends Error {}

export interface StickerMeta {
  /** pack name shown when the recipient taps the sticker */
  pack?: string
  author?: string
  emojis?: string[]
}

export interface Sticker {
  buffer: Buffer
  animated: boolean
  width: number
  height: number
}

const SIDE = 512
/** what WhatsApp's own sticker makers aim for */
const TARGET_STATIC = 100 * 1024
const TARGET_ANIMATED = 500 * 1024
/** above this WhatsApp shows the sticker as broken */
const HARD_LIMIT = 1024 * 1024
const QUALITIES = [80, 60, 40, 25, 12]
const TRANSPARENT = { r: 0, g: 0, b: 0, alpha: 0 }

type Kind = 'image' | 'video' | 'lottie' | 'unknown'

export function sniff(buf: Buffer): Kind {
  if (buf.length < 12) return 'unknown'
  const ascii = (from: number, to: number) => buf.toString('latin1', from, to)
  if (ascii(0, 4) === 'RIFF' && ascii(8, 12) === 'WEBP') return 'image'
  if (ascii(0, 4) === 'GIF8') return 'image'
  if (buf[0] === 0x89 && ascii(1, 4) === 'PNG') return 'image' // also APNG
  if (buf[0] === 0xff && buf[1] === 0xd8) return 'image'
  if (ascii(4, 8) === 'ftyp') {
    const brand = ascii(8, 12)
    return /^(avif|avis|heic|heix|mif1|msf1)/.test(brand) ? 'image' : 'video'
  }
  if (buf.readUInt32BE(0) === 0x1a45dfa3) return 'video' // webm / mkv
  if (ascii(0, 4) === 'RIFF' && ascii(8, 12) === 'AVI ') return 'video'
  if (ascii(0, 2) === 'PK') return 'lottie' // .was is a zip
  const head = ascii(0, 256).trimStart()
  if (head.startsWith('<svg') || head.startsWith('<?xml')) return 'image'
  return 'unknown'
}

/**
 * Turns an image, GIF or short video into what WhatsApp expects of a sticker: a 512×512 WebP
 * (animated when the source is), small enough to render. Files that already comply pass through untouched.
 */
export async function toSticker(input: Buffer, meta?: StickerMeta): Promise<Sticker> {
  const kind = sniff(input)
  if (kind === 'lottie') throw new StickerError('Lottie stickers (.was) cannot be sent; use an image, GIF or video')
  if (kind === 'unknown') throw new StickerError('unsupported sticker source; use PNG, JPEG, WebP, GIF or a short video')

  let source = input
  if (kind === 'video') source = await videoToWebp(input)

  let info: Metadata
  try {
    info = await sharp(source, { animated: true }).metadata()
  } catch (err: any) {
    throw new StickerError(`could not read sticker source: ${err?.message ?? err}`)
  }
  const animated = (info.pages ?? 1) > 1
  const target = animated ? TARGET_ANIMATED : TARGET_STATIC
  const compliant =
    info.format === 'webp' && info.width === SIDE && (info.pageHeight ?? info.height) === SIDE && source.length <= target

  let buffer = source
  if (!compliant) {
    let best: Buffer | undefined
    for (const quality of QUALITIES) {
      const out = await sharp(source, { animated: true })
        .rotate() // honour EXIF orientation of photos
        .resize(SIDE, SIDE, { fit: 'contain', background: TRANSPARENT })
        .webp({ quality, alphaQuality: Math.max(quality, 50), effort: 4, loop: 0 })
        .toBuffer()
        .catch((err) => {
          throw new StickerError(`could not convert to sticker: ${err?.message ?? err}`)
        })
      if (!best || out.length < best.length) best = out
      if (out.length <= target) break
    }
    buffer = best!
    if (buffer.length > HARD_LIMIT) {
      throw new StickerError(
        `sticker is still ${Math.round(buffer.length / 1024)} KB after compression (limit 1 MB); use a shorter or simpler animation`,
      )
    }
  }

  if (meta && (meta.pack || meta.author || meta.emojis?.length)) {
    buffer = setWebpExif(buffer, stickerExif(meta), { width: SIDE, height: SIDE, hasAlpha: true })
  }
  return { buffer, animated, width: SIDE, height: SIDE }
}

/** First seconds of a video as an animated WebP already on the sticker canvas. Needs ffmpeg. */
async function videoToWebp(input: Buffer): Promise<Buffer> {
  const { ffmpegPath, maxSeconds, fps } = config.sticker
  const dir = await mkdtemp(join(tmpdir(), 'zapi-sticker-'))
  const src = join(dir, 'in')
  const dst = join(dir, 'out.webp')
  try {
    // a file rather than stdin: MP4s with the index at the end cannot be read from a pipe
    await writeFile(src, input)
    const filter =
      `fps=${fps},scale=${SIDE}:${SIDE}:force_original_aspect_ratio=decrease:flags=lanczos,format=rgba,` +
      `pad=${SIDE}:${SIDE}:(ow-iw)/2:(oh-ih)/2:color=0x00000000`
    const args = ['-hide_banner', '-loglevel', 'error', '-y', '-t', String(maxSeconds), '-i', src]
    args.push('-vf', filter, '-an', '-c:v', 'libwebp', '-q:v', '70', '-loop', '0', '-f', 'webp', dst)
    await new Promise<void>((resolve, reject) => {
      execFile(ffmpegPath, args, { timeout: 60_000, windowsHide: true }, (err, _stdout, stderr) => {
        if (!err) return resolve()
        if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
          return reject(new StickerError('video stickers need ffmpeg, which is not installed on this worker'))
        }
        reject(new StickerError(`ffmpeg could not convert the video: ${String(stderr || err.message).trim().slice(-300)}`))
      })
    })
    return await readFile(dst)
  } finally {
    await rm(dir, { recursive: true, force: true }).catch(() => undefined)
  }
}

/** WhatsApp reads pack name, author and emojis from a private EXIF tag (0x5741) holding JSON. */
export function stickerExif(meta: StickerMeta): Buffer {
  const json = Buffer.from(
    JSON.stringify({
      'sticker-pack-id': randomUUID(),
      'sticker-pack-name': meta.pack ?? '',
      'sticker-pack-publisher': meta.author ?? '',
      emojis: meta.emojis ?? [],
    }),
    'utf8',
  )
  // little-endian TIFF header + one IFD entry: tag 0x5741, type UNDEFINED, count = json length, data at offset 22
  const header = Buffer.from([
    0x49, 0x49, 0x2a, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x41, 0x57, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x16, 0x00,
    0x00, 0x00,
  ])
  header.writeUInt32LE(json.length, 14)
  return Buffer.concat([header, json])
}

interface Chunk {
  id: string
  data: Buffer
}

export function webpChunks(webp: Buffer): Chunk[] {
  if (webp.toString('latin1', 0, 4) !== 'RIFF' || webp.toString('latin1', 8, 12) !== 'WEBP') throw new StickerError('not a WebP file')
  const chunks: Chunk[] = []
  let pos = 12
  while (pos + 8 <= webp.length) {
    const size = webp.readUInt32LE(pos + 4)
    chunks.push({ id: webp.toString('latin1', pos, pos + 4), data: webp.subarray(pos + 8, pos + 8 + size) })
    pos += 8 + size + (size & 1)
  }
  return chunks
}

/** Replaces the EXIF chunk of a WebP, upgrading a simple file to the extended (VP8X) layout when needed. */
export function setWebpExif(webp: Buffer, exif: Buffer, canvas: { width: number; height: number; hasAlpha: boolean }): Buffer {
  const chunks = webpChunks(webp).filter((c) => c.id !== 'EXIF')
  const EXIF_FLAG = 0x08
  if (chunks[0]?.id === 'VP8X') {
    const data = Buffer.from(chunks[0].data)
    data[0] |= EXIF_FLAG
    chunks[0] = { id: 'VP8X', data }
  } else {
    const data = Buffer.alloc(10)
    data[0] = EXIF_FLAG | (canvas.hasAlpha ? 0x10 : 0)
    data.writeUIntLE(canvas.width - 1, 4, 3)
    data.writeUIntLE(canvas.height - 1, 7, 3)
    chunks.unshift({ id: 'VP8X', data })
  }
  // the spec orders metadata after the image data: EXIF first, then XMP
  const xmp = chunks.findIndex((c) => c.id === 'XMP ')
  chunks.splice(xmp === -1 ? chunks.length : xmp, 0, { id: 'EXIF', data: exif })

  const parts: Buffer[] = []
  for (const c of chunks) {
    const head = Buffer.alloc(8)
    head.write(c.id, 0, 'latin1')
    head.writeUInt32LE(c.data.length, 4)
    parts.push(head, c.data)
    if (c.data.length & 1) parts.push(Buffer.alloc(1))
  }
  const body = Buffer.concat(parts)
  const riff = Buffer.alloc(12)
  riff.write('RIFF', 0, 'latin1')
  riff.writeUInt32LE(body.length + 4, 4)
  riff.write('WEBP', 8, 'latin1')
  return Buffer.concat([riff, body])
}
