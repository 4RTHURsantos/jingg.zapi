import {
  BufferJSON,
  initAuthCreds,
  proto,
  type AuthenticationCreds,
  type AuthenticationState,
  type SignalDataSet,
  type SignalDataTypeMap,
} from 'baileys'
import { query, tx } from './db.js'

/**
 * Baileys auth state persisted in Postgres, so any worker can take a device over.
 * Signal keys are written in a single transaction per `set` call: a half-written
 * key batch would corrupt the session.
 */
export async function usePostgresAuthState(deviceId: string): Promise<{
  state: AuthenticationState
  saveCreds: () => Promise<void>
  hasSession: boolean
}> {
  const res = await query('SELECT creds::text AS creds FROM auth_creds WHERE device_id = $1', [deviceId])
  const hasSession = !!res.rowCount
  const creds: AuthenticationCreds = hasSession ? JSON.parse(res.rows[0].creds, BufferJSON.reviver) : initAuthCreds()

  // creds are saved in call order; a slow older write must never land after a newer one
  let saveChain: Promise<void> = Promise.resolve()
  const saveCreds = () => {
    const snapshot = JSON.stringify(creds, BufferJSON.replacer)
    saveChain = saveChain
      .catch(() => {})
      .then(async () => {
        await query(
          `INSERT INTO auth_creds (device_id, creds) VALUES ($1, $2::jsonb)
           ON CONFLICT (device_id) DO UPDATE SET creds = EXCLUDED.creds, updated_at = now()`,
          [deviceId, snapshot],
        )
      })
    return saveChain
  }

  const state: AuthenticationState = {
    creds,
    keys: {
      async get<T extends keyof SignalDataTypeMap>(type: T, ids: string[]) {
        const out: { [id: string]: SignalDataTypeMap[T] } = {}
        if (!ids.length) return out
        const rows = await query(
          'SELECT id, value::text AS value FROM auth_keys WHERE device_id = $1 AND type = $2 AND id = ANY ($3::text[])',
          [deviceId, type, ids],
        )
        for (const row of rows.rows) {
          let value = JSON.parse(row.value, BufferJSON.reviver)
          if (type === 'app-state-sync-key' && value) {
            value = proto.Message.AppStateSyncKeyData.fromObject(value)
          }
          out[row.id] = value
        }
        return out
      },

      async set(data: SignalDataSet) {
        const upTypes: string[] = []
        const upIds: string[] = []
        const upValues: string[] = []
        const delTypes: string[] = []
        const delIds: string[] = []
        for (const type of Object.keys(data) as (keyof SignalDataSet)[]) {
          const entries = data[type] ?? {}
          for (const id of Object.keys(entries)) {
            const value = (entries as Record<string, unknown>)[id]
            if (value === null || value === undefined) {
              delTypes.push(type)
              delIds.push(id)
            } else {
              upTypes.push(type)
              upIds.push(id)
              upValues.push(JSON.stringify(value, BufferJSON.replacer))
            }
          }
        }
        if (!upTypes.length && !delTypes.length) return
        await tx(async (client) => {
          if (upTypes.length) {
            await client.query(
              `INSERT INTO auth_keys (device_id, type, id, value)
               SELECT $1, t, i, v::jsonb FROM unnest($2::text[], $3::text[], $4::text[]) AS x(t, i, v)
               ON CONFLICT (device_id, type, id) DO UPDATE SET value = EXCLUDED.value`,
              [deviceId, upTypes, upIds, upValues],
            )
          }
          if (delTypes.length) {
            await client.query(
              `DELETE FROM auth_keys k USING unnest($2::text[], $3::text[]) AS x(t, i)
                WHERE k.device_id = $1 AND k.type = x.t AND k.id = x.i`,
              [deviceId, delTypes, delIds],
            )
          }
        })
      },

      async clear() {
        await query('DELETE FROM auth_keys WHERE device_id = $1', [deviceId])
      },
    },
  }

  return { state, saveCreds, hasSession }
}

/** Forgets the WhatsApp session entirely; the next connect starts a fresh pairing. */
export async function clearAuthState(deviceId: string): Promise<void> {
  await tx(async (client) => {
    await client.query('DELETE FROM auth_keys WHERE device_id = $1', [deviceId])
    await client.query('DELETE FROM auth_creds WHERE device_id = $1', [deviceId])
  })
}
