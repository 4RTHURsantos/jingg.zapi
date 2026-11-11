import { migrate, pool } from './db.js'
import { logger } from './logger.js'
import { ensureBucket } from './storage.js'

try {
  await migrate()
  await ensureBucket()
  logger.info('migrations complete')
  await pool.end()
} catch (err) {
  logger.error({ err }, 'migration failed')
  process.exit(1)
}
