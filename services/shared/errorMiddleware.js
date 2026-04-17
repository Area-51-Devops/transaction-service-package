'use strict';
const { logger } = require('./logger');

/**
 * Global Express error handler.
 * Returns: { success: false, error: { code, message } }
 */
// eslint-disable-next-line no-unused-vars
function errorMiddleware(err, req, res, next) {
  const statusCode = err.statusCode || err.status || 500;
  const code       = err.code    || 'INTERNAL_ERROR';
  const message    = err.message || 'An unexpected error occurred';

  logger.error({
    requestId: req.requestId,
    code,
    message,
    stack: err.stack
  }, 'Unhandled error');

  res.status(statusCode).json({
    success: false,
    error: { code, message }
  });
}

/**
 * Helper to create a structured API error.
 */
function createError(statusCode, code, message) {
  const err = new Error(message);
  err.statusCode = statusCode;
  err.code = code;
  return err;
}

module.exports = { errorMiddleware, createError };
