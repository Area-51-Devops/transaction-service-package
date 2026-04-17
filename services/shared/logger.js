'use strict';
const pino = require('pino');

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  formatters: {
    level(label) { return { level: label }; }
  },
  timestamp: pino.stdTimeFunctions.isoTime,
  base: { service: process.env.SERVICE_NAME || 'banking-service' }
});

/**
 * Returns a child logger with request context attached.
 * @param {object} ctx - { requestId, userId, transactionId }
 */
function childLogger(ctx = {}) {
  return logger.child(ctx);
}

module.exports = { logger, childLogger };
