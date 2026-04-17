'use strict';
const { v4: uuidv4 } = require('uuid');

/**
 * Middleware: reads X-Request-ID header or generates a new UUID.
 * Attaches it to req.requestId and echoes it in the response header.
 */
function requestIdMiddleware(req, res, next) {
  const id = req.headers['x-request-id'] || uuidv4();
  req.requestId = id;
  res.setHeader('x-request-id', id);
  next();
}

module.exports = { requestIdMiddleware };
