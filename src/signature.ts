import { createHmac, timingSafeEqual } from 'node:crypto'

/** Value of the X-Zapi-Signature header: HMAC-SHA256 of the raw request body, keyed by the webhook secret. */
export function sign(secret: string, body: string): string {
  return 'sha256=' + createHmac('sha256', secret).update(body).digest('hex')
}

export function verify(secret: string, body: string, signature: string): boolean {
  const expected = Buffer.from(sign(secret, body))
  const given = Buffer.from(signature)
  return expected.length === given.length && timingSafeEqual(expected, given)
}
