import { readdir, readFile } from 'node:fs/promises'
import { join } from 'node:path'
import pg from 'pg'
import { config } from './config.js'
import { logger } from './logger.js'

// bigint (int8) columns are sequence numbers that fit comfortably in a JS number
pg.types.setTypeParser(20, (v) => Number(v))

export const pool = new pg.Pool({
  connectionString: config.databaseUrl,
  max: config.dbPoolSize,
})

pool.on('error', (err) => logger.error({ err }, 'postgres pool error'))

export type Queryable = Pick<pg.PoolClient, 'query'>

export const query = <T extends pg.QueryResultRow = any>(text: string, params?: unknown[]) =>
  pool.query<T>(text, params as any[])

/** Runs fn inside a transaction; commits on success, rolls back on any throw. */
export async function tx<T>(fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect()
  try {
    await client.query('BEGIN')
    const result = await fn(client)
    await client.query('COMMIT')
    return result
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {})
    throw err
  } finally {
    client.release()
  }
}

/**
 * JSON for a jsonb column. Postgres rejects the NUL escape inside jsonb strings,
 * and WhatsApp text occasionally carries it, so it is stripped here.
 */
export function toJsonb(value: unknown, replacer?: (key: string, value: any) => any): string {
  return JSON.stringify(value ?? null, replacer).replace(/\\u0000/g, '')
}

export async function migrate(dir = join(process.cwd(), 'migrations')): Promise<void> {
  const client = await pool.connect()
  try {
    // serialize concurrent migrators
    await client.query('SELECT pg_advisory_lock(727274)')
    await client.query(
      `CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`,
    )
    const applied = new Set((await client.query('SELECT name FROM schema_migrations')).rows.map((r) => r.name))
    const files = (await readdir(dir)).filter((f) => f.endsWith('.sql')).sort()
    for (const file of files) {
      if (applied.has(file)) continue
      const sql = await readFile(join(dir, file), 'utf8')
      await client.query('BEGIN')
      try {
        await client.query(sql)
        await client.query('INSERT INTO schema_migrations (name) VALUES ($1)', [file])
        await client.query('COMMIT')
        logger.info({ file }, 'migration applied')
      } catch (err) {
        await client.query('ROLLBACK')
        throw err
      }
    }
  } finally {
    await client.query('SELECT pg_advisory_unlock(727274)').catch(() => {})
    client.release()
  }
}
