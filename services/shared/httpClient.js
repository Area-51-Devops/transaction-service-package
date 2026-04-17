'use strict';
const axios        = require('axios');
const axiosRetry   = require('axios-retry').default;

/**
 * Create a base Axios instance that:
 *  - Propagates X-Request-ID
 *  - Retries ONLY on GET requests or requests that carry an Idempotency-Key header
 *  - Uses exponential back-off (3 retries)
 */
function createHttpClient(baseURL, options = {}) {
  const instance = axios.create({
    baseURL,
    timeout: options.timeout || 10000,
    headers: { 'Content-Type': 'application/json' }
  });

  // Retry only safe / idempotent calls
  axiosRetry(instance, {
    retries: 3,
    retryDelay: axiosRetry.exponentialDelay,
    retryCondition: (error) => {
      const method = error.config?.method?.toLowerCase();
      const hasIdemKey = !!error.config?.headers?.['idempotency-key'];
      const isIdempotent = method === 'get' || hasIdemKey;
      const isNetworkOrServerError =
        axiosRetry.isNetworkOrIdempotentRequestError(error) ||
        (error.response && error.response.status >= 500);
      return isIdempotent && isNetworkOrServerError;
    }
  });

  // X-Request-ID propagation
  instance.interceptors.request.use((config) => {
    if (options.getRequestId) {
      config.headers['x-request-id'] = options.getRequestId();
    }
    return config;
  });

  return instance;
}

module.exports = { createHttpClient };
