import type { Readable } from 'node:stream'
import { Client } from 'minio'
import { config } from './config.js'

function makeClient(endpoint: string): Client {
  const url = new URL(endpoint)
  return new Client({
    endPoint: url.hostname,
    port: url.port ? Number(url.port) : url.protocol === 'https:' ? 443 : 80,
    useSSL: url.protocol === 'https:',
    accessKey: config.s3.accessKey,
    secretKey: config.s3.secretKey,
    // an explicit region keeps presigning fully offline (no bucket-location lookup)
    region: config.s3.region,
    pathStyle: true,
  })
}

const internal = makeClient(config.s3.endpoint)
/** only used to sign URLs, never to make requests: the public host may not resolve from inside the container */
const publicSigner = makeClient(config.s3.publicEndpoint)

export async function ensureBucket(): Promise<void> {
  if (!(await internal.bucketExists(config.s3.bucket))) {
    await internal.makeBucket(config.s3.bucket, config.s3.region)
  }
}

export async function putObject(
  key: string,
  body: Buffer | Readable,
  contentType?: string,
  size?: number,
): Promise<void> {
  const meta = contentType ? { 'Content-Type': contentType } : undefined
  await internal.putObject(config.s3.bucket, key, body, size, meta)
}

export async function statObject(key: string): Promise<{ size: number }> {
  const stat = await internal.statObject(config.s3.bucket, key)
  return { size: stat.size }
}

export async function getObjectBuffer(key: string): Promise<Buffer> {
  const stream = await internal.getObject(config.s3.bucket, key)
  const chunks: Buffer[] = []
  for await (const chunk of stream) chunks.push(chunk as Buffer)
  return Buffer.concat(chunks)
}

export async function removeObject(key: string): Promise<void> {
  await internal.removeObject(config.s3.bucket, key)
}

export function presignedUrl(key: string, ttlSeconds = config.s3.urlTtlSeconds): Promise<string> {
  // SigV4 caps presigned URLs at 7 days
  const ttl = Math.min(ttlSeconds, 7 * 24 * 3600)
  return publicSigner.presignedGetObject(config.s3.bucket, key, ttl)
}

export async function storageHealthy(): Promise<boolean> {
  try {
    await internal.bucketExists(config.s3.bucket)
    return true
  } catch {
    return false
  }
}
