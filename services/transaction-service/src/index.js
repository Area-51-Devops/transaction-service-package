'use strict';
require('dotenv').config();

const express = require('express');
const cors    = require('cors');
const mysql   = require('mysql2/promise');
const amqp    = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const { logger }              = require('../shared/logger');
const { requestIdMiddleware }  = require('../shared/requestId');
const { errorMiddleware, createError } = require('../shared/errorMiddleware');
const { createHttpClient }     = require('../shared/httpClient');
const { authMiddleware, adminMiddleware } = require('../shared/authMiddleware');

const PORT         = process.env.PORT            || 3003;
const ACCOUNT_SVC  = process.env.ACCOUNT_SVC_URL || 'http://account-service:3002';
const CONFIG_SVC   = process.env.CONFIG_SVC_URL  || 'http://config-service:3008';
const MQ_URL       = process.env.MQ_URL          || 'amqp://rabbitmq';
const EXCHANGE     = 'banking_events';

// Default fraud threshold (INR) used when config-service is unavailable
const DEFAULT_FRAUD_THRESHOLD = 500000; // ₹5,00,000

let pool;
let mqChannel;
let fraudThreshold = DEFAULT_FRAUD_THRESHOLD;  // in-memory cache
let isStarted      = false;

// ──────────────────────────────────────────────
// HTTP clients (safe retry only)
// ──────────────────────────────────────────────
const accountClient = createHttpClient(ACCOUNT_SVC);
const configClient  = createHttpClient(CONFIG_SVC);

// ──────────────────────────────────────────────
// Exponential backoff connector
// ──────────────────────────────────────────────
async function connectWithRetry(connectFn, name, maxRetries = 10) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const result = await connectFn();
      logger.info({ service: 'transaction-service' }, `${name} connected`);
      return result;
    } catch (err) {
      const delay = Math.min(1000 * Math.pow(2, attempt - 1), 30000);
      logger.warn({ service: 'transaction-service', attempt, delay }, `${name} not ready, retrying...`);
      if (attempt === maxRetries) throw err;
      await new Promise(r => setTimeout(r, delay));
    }
  }
}

// ──────────────────────────────────────────────
// Fetch fraud threshold from config-service
// ──────────────────────────────────────────────
async function refreshFraudThreshold() {
  try {
    const { data } = await configClient.get('/config/fraudThresholdInr');
    if (data && data.value !== undefined) {
      fraudThreshold = Number(data.value);
      logger.info({ fraudThreshold }, 'Fraud threshold refreshed from config-service');
    }
  } catch (err) {
    logger.warn({ err: err.message, fallback: fraudThreshold }, 'Config-service unreachable; using cached fraud threshold');
  }
}

// ──────────────────────────────────────────────
// Outbox Poller  (SKIP LOCKED for multi-instance safety)
// ──────────────────────────────────────────────
async function startOutboxPoller() {
  setInterval(async () => {
    if (!mqChannel) return;
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      // Grab up to 10 UNPUBLISHED or FAILED-but-retryable events
      const [events] = await conn.execute(
        `SELECT * FROM outbox_events
           WHERE status IN ('UNPUBLISHED','FAILED') AND retry_count < 5
           ORDER BY id ASC LIMIT 10
           FOR UPDATE SKIP LOCKED`
      );
      if (events.length === 0) { await conn.rollback(); conn.release(); return; }

      // Mark as PROCESSING atomically
      const ids = events.map(e => e.id);
      await conn.execute(
        `UPDATE outbox_events SET status = 'PROCESSING' WHERE id IN (${ids.map(() => '?').join(',')})`,
        ids
      );
      await conn.commit();
      conn.release();

      // Publish each event
      for (const event of events) {
        const conn2 = await pool.getConnection();
        try {
          // mysql2 auto-parses JSON columns into JS objects — must re-stringify before publishing
          const payloadStr = typeof event.payload === 'string' ? event.payload : JSON.stringify(event.payload);
          mqChannel.publish(EXCHANGE, event.event_type, Buffer.from(payloadStr), { persistent: true });
          await conn2.execute(
            "UPDATE outbox_events SET status='PUBLISHED', updated_at=NOW() WHERE id=?",
            [event.id]
          );
          logger.info({ eventId: event.id, eventType: event.event_type }, 'Outbox event published');
        } catch (pubErr) {
          logger.error({ eventId: event.id, err: pubErr.message }, 'Failed to publish outbox event');
          await conn2.execute(
            "UPDATE outbox_events SET status='FAILED', retry_count=retry_count+1, updated_at=NOW() WHERE id=?",
            [event.id]
          );
        } finally {
          conn2.release();
        }
      }
    } catch (err) {
      logger.error({ err: err.message }, 'Outbox poller error');
      try { await conn.rollback(); } catch (_) {}
      conn.release();
    }
  }, 5000); // every 5 seconds
}

// ──────────────────────────────────────────────
// Saga Recovery Poller  (crash-safe timeout handling)
// Finds transactions stuck in DEBITED where timeout_at < NOW()
// Triggers compensation (credit back the from-account)
// ──────────────────────────────────────────────
async function startSagaRecoveryPoller() {
  setInterval(async () => {
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      const [stuckTxs] = await conn.execute(
        `SELECT * FROM transactions
           WHERE saga_state = 'DEBITED' AND timeout_at < NOW()
           LIMIT 10 FOR UPDATE SKIP LOCKED`
      );
      if (stuckTxs.length === 0) { await conn.rollback(); conn.release(); return; }

      // Mark as FAILED before releasing the lock
      const ids = stuckTxs.map(t => t.id);
      await conn.execute(
        `UPDATE transactions SET saga_state='FAILED', status='FAILED', updated_at=NOW()
           WHERE id IN (${ids.map(() => '?').join(',')})`,
        ids
      );
      await conn.commit();
      conn.release();

      // Issue compensation credits
      for (const tx of stuckTxs) {
        logger.warn({ txId: tx.id }, 'Saga recovery: compensating stuck DEBITED transaction');
        try {
          const compensationKey = `comp:${tx.id}`;
          await accountClient.post(
            `/accounts/${tx.from_account_id}/credit`,
            { amount: tx.amount },
            { headers: { 'idempotency-key': compensationKey } }
          );
          logger.info({ txId: tx.id }, 'Compensation credit issued successfully');
        } catch (creditErr) {
          logger.error({ txId: tx.id, err: creditErr.message }, 'Compensation credit failed, will retry on next poll');
        }
      }
    } catch (err) {
      logger.error({ err: err.message }, 'Saga recovery poller error');
      try { await conn.rollback(); } catch (_) {}
      conn.release();
    }
  }, 30000); // every 30 seconds
}

// ──────────────────────────────────────────────
// MQ consumer — handle FraudApproved / FraudRejected
// ──────────────────────────────────────────────
async function startFraudEventConsumer(channel) {
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  const q = await channel.assertQueue('fraud_decisions', { durable: true });
  await channel.bindQueue(q.queue, EXCHANGE, 'FraudApproved');
  await channel.bindQueue(q.queue, EXCHANGE, 'FraudRejected');

  channel.consume(q.queue, async (msg) => {
    if (!msg) return;
    try {
      const event = JSON.parse(msg.content.toString());
      const { transactionId, decision } = event;
      const conn = await pool.getConnection();
      try {
        if (decision === 'APPROVED') {
          // Credit the to-account since it was skipped during FLAGGED state
          await accountClient.post(
            `/accounts/${event.toAccountId}/credit`,
            { amount: event.amount },
            { headers: { 'idempotency-key': `credit:fraud:${transactionId}:${event.toAccountId}` } }
          );
          
          await conn.execute(
            "UPDATE transactions SET saga_state='SUCCESS', status='SUCCESS', updated_at=NOW() WHERE id=?",
            [transactionId]
          );
          logger.info({ transactionId }, 'Fraud approved — transaction marked SUCCESS and credited');
        } else {
          // Rejected — compensation credit
          const [rows] = await conn.execute('SELECT * FROM transactions WHERE id=?', [transactionId]);
          if (rows.length > 0) {
            await conn.execute(
              "UPDATE transactions SET saga_state='FAILED', status='FAILED', updated_at=NOW() WHERE id=?",
              [transactionId]
            );
            await accountClient.post(
              `/accounts/${rows[0].from_account_id}/credit`,
              { amount: rows[0].amount },
              { headers: { 'idempotency-key': `comp:fraud:${transactionId}` } }
            );
          }
          logger.warn({ transactionId }, 'Fraud rejected — compensation issued');
        }
      } finally {
        conn.release();
      }
      channel.ack(msg);
    } catch (err) {
      logger.error({ err: err.message }, 'Error processing fraud decision event');
      channel.nack(msg, false, true); // requeue
    }
  });
}

async function init() {
  pool = await connectWithRetry(async () => {
    const p = mysql.createPool({
      host:              process.env.DB_HOST || 'mysql',
      user:              process.env.DB_USER || 'root',
      password:          process.env.DB_PASS || 'rootpassword',
      database:          process.env.DB_NAME || 'banking_db',
      waitForConnections: true,
      connectionLimit:   15,
      queueLimit:        0
    });
    await p.execute('SELECT 1');
    return p;
  }, 'MySQL');

  const mqConn = await connectWithRetry(async () => {
    const conn = await amqp.connect(MQ_URL);
    mqChannel = await conn.createChannel();
    await mqChannel.assertExchange(EXCHANGE, 'topic', { durable: true });
    return conn;
  }, 'RabbitMQ');

  await refreshFraudThreshold();
  setInterval(refreshFraudThreshold, 60000); // refresh every minute

  await startFraudEventConsumer(mqChannel);
  startOutboxPoller();
  startSagaRecoveryPoller();

  isStarted = true;
}

// ──────────────────────────────────────────────
// Express App
// ──────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());
app.use(requestIdMiddleware);

// ── Health Probes ──────────────────────────────
app.get('/health/startup', (req, res) => {
  res.json({ status: isStarted ? 'UP' : 'STARTING', service: 'transaction-service' });
});
app.get('/health/liveness', async (req, res, next) => {
  try {
    await pool.execute('SELECT 1');
    res.json({ status: 'UP', service: 'transaction-service' });
  } catch (err) { next(createError(503, 'HEALTH_CHECK_FAILED', 'DB ping failed')); }
});
app.get('/health/readiness', async (req, res, next) => {
  try {
    if (!isStarted) throw new Error('Not ready');
    await pool.execute('SELECT 1');
    res.json({ status: 'READY', service: 'transaction-service' });
  } catch (err) { next(createError(503, 'NOT_READY', 'Not ready')); }
});
app.get('/health', (req, res) => res.json({ status: 'UP', service: 'transaction-service' }));

// ── Transfer (Saga Orchestrator) ───────────────
app.post('/transfer', async (req, res, next) => {
  const { fromAccountId, toAccountId, amount } = req.body;
  const idemKey   = req.headers['idempotency-key'];
  const requestId = req.requestId;
  const log = logger.child({ requestId, endpoint: 'transfer' });

  if (!fromAccountId || !toAccountId || !amount) {
    return next(createError(400, 'VALIDATION_ERROR', 'fromAccountId, toAccountId, and amount are required'));
  }
  if (fromAccountId == toAccountId) {
    return next(createError(400, 'VALIDATION_ERROR', 'Cannot transfer to the same account'));
  }
  if (isNaN(amount) || Number(amount) <= 0) {
    return next(createError(400, 'VALIDATION_ERROR', 'Amount must be a positive number'));
  }

  // ── Idempotency check (user_id, endpoint, idem_key) ────
  if (idemKey) {
    const userId = req.body.userId || fromAccountId; // use account as proxy if userId not provided
    const [existing] = await pool.execute(
      'SELECT response FROM idempotency_keys WHERE idem_key=? AND user_id=? AND endpoint=?',
      [idemKey, userId, '/transfer']
    );
    if (existing.length > 0 && existing[0].response) {
      log.info({ idemKey }, 'Idempotent response returned');
      return res.json(JSON.parse(existing[0].response));
    }
  }

  let txId = uuidv4();
  let conn;
  try {
    conn = await pool.getConnection();
    // ── STEP 1: Create transaction record (INITIATED) ──
    await conn.beginTransaction();
    const timeoutAt = new Date(Date.now() + 30000); // 30-second saga timeout
    await conn.execute(
      `INSERT INTO transactions (id, from_account_id, to_account_id, amount, status, saga_state, idempotency_key, request_id, timeout_at)
         VALUES (?,?,?,?,'INITIATED','INITIATED',?,?,?)`,
      [txId, fromAccountId, toAccountId, amount, idemKey || null, requestId, timeoutAt]
    );
    await conn.commit();
    conn.release();
    conn = null; // nullify to prevent double release in catch block

    log.info({ txId, fromAccountId, toAccountId, amount }, 'Transaction INITIATED');

    // ── STEP 2: Debit from-account ──────────────────────
    try {
      await accountClient.post(
        `/accounts/${fromAccountId}/debit`,
        { amount },
        { headers: { 'idempotency-key': `debit:${txId}:${fromAccountId}` } }
      );
    } catch (debitErr) {
      await pool.execute(
        "UPDATE transactions SET saga_state='FAILED', status='FAILED', updated_at=NOW() WHERE id=?", [txId]
      );
      log.info({ txId }, 'Debit failed — transaction FAILED');
      return next(createError(400, 'DEBIT_FAILED', debitErr.response?.data?.error?.message || 'Debit failed'));
    }

    // Update saga_state to DEBITED
    await pool.execute(
      "UPDATE transactions SET saga_state='DEBITED', status='DEBITED', updated_at=NOW() WHERE id=?", [txId]
    );
    log.info({ txId }, 'Transaction DEBITED');

    // ── STEP 3: Fraud check ─────────────────────────────
    if (Number(amount) >= fraudThreshold) {
      // Flag the transaction — async fraud decision via MQ
      const conn2 = await pool.getConnection();
      try {
        await conn2.beginTransaction();
        await conn2.execute(
          "UPDATE transactions SET saga_state='FLAGGED', status='FLAGGED', updated_at=NOW() WHERE id=?", [txId]
        );
        // Write to Outbox inside same transaction
        const payload = JSON.stringify({ transactionId: txId, fromAccountId, toAccountId, amount, requestId });
        await conn2.execute(
          "INSERT INTO outbox_events (event_type, aggregate_id, payload, status) VALUES ('TransactionFlagged', ?, ?, 'UNPUBLISHED')",
          [String(txId), payload]
        );
        await conn2.commit();
      } finally {
        conn2.release();
      }

      log.warn({ txId, amount, threshold: fraudThreshold }, 'Transaction FLAGGED for fraud review');
      const response = { success: true, txId, status: 'FLAGGED', message: 'Transaction flagged for fraud review' };
      if (idemKey) {
        const userId = req.body.userId || fromAccountId;
        await pool.execute(
          'INSERT IGNORE INTO idempotency_keys (idem_key, user_id, endpoint, response) VALUES (?,?,?,?)',
          [idemKey, userId, '/transfer', JSON.stringify(response)]
        );
      }
      return res.status(202).json(response);
    }

    // ── STEP 4: Credit to-account ───────────────────────
    try {
      await accountClient.post(
        `/accounts/${toAccountId}/credit`,
        { amount },
        { headers: { 'idempotency-key': `credit:${txId}:${toAccountId}` } }
      );
    } catch (creditErr) {
      // Compensation: reverse the debit
      log.error({ txId }, 'Credit failed — issuing compensation debit reversal');
      try {
        await accountClient.post(
          `/accounts/${fromAccountId}/credit`,
          { amount },
          { headers: { 'idempotency-key': `comp:${txId}:${fromAccountId}` } }
        );
      } catch (compErr) {
        log.error({ txId, err: compErr.message }, 'CRITICAL: Compensation also failed — requires manual review');
      }
      await pool.execute(
        "UPDATE transactions SET saga_state='FAILED', status='FAILED', updated_at=NOW() WHERE id=?", [txId]
      );
      return next(createError(500, 'CREDIT_FAILED', 'Transfer failed; debit has been reversed'));
    }

    // ── STEP 5: Finalize — SUCCESS + Outbox ────────────
    const conn3 = await pool.getConnection();
    try {
      await conn3.beginTransaction();
      await conn3.execute(
        "UPDATE transactions SET saga_state='SUCCESS', status='SUCCESS', updated_at=NOW() WHERE id=?", [txId]
      );
      const payload = JSON.stringify({ transactionId: txId, fromAccountId, toAccountId, amount, requestId, userId: req.body.userId });
      await conn3.execute(
        "INSERT INTO outbox_events (event_type, aggregate_id, payload, status) VALUES ('TransactionCompleted', ?, ?, 'UNPUBLISHED')",
        [String(txId), payload]
      );
      await conn3.commit();
    } finally {
      conn3.release();
    }

    log.info({ txId }, 'Transaction SUCCESS');
    const response = { success: true, txId, status: 'SUCCESS' };

    if (idemKey) {
      const userId = req.body.userId || fromAccountId;
      await pool.execute(
        'INSERT IGNORE INTO idempotency_keys (idem_key, user_id, endpoint, response) VALUES (?,?,?,?)',
        [idemKey, userId, '/transfer', JSON.stringify(response)]
      );
    }

    res.json(response);
  } catch (err) {
    if (conn) {
      try { await conn.rollback(); } catch (_) {}
      conn.release();
    }
    if (txId) {
      await pool.execute(
        "UPDATE transactions SET saga_state='FAILED', status='FAILED', updated_at=NOW() WHERE id=?", [txId]
      );
    }
    next(err);
  }
});

// ── Get Transactions ───────────────────────────
app.get('/transactions', async (req, res, next) => {
  try {
    const { accountId } = req.query;
    let rows;
    if (accountId) {
      [rows] = await pool.execute(
        `SELECT t.*, 
                a1.account_number AS from_account_number, 
                a2.account_number AS to_account_number 
         FROM transactions t
         LEFT JOIN accounts a1 ON t.from_account_id = a1.id
         LEFT JOIN accounts a2 ON t.to_account_id = a2.id
         WHERE t.from_account_id=? OR (t.to_account_id=? AND t.status='SUCCESS') 
         ORDER BY t.created_at DESC LIMIT 50`,
        [accountId, accountId]
      );
    } else {
      [rows] = await pool.execute('SELECT * FROM transactions ORDER BY created_at DESC LIMIT 50');
    }
    res.json({ success: true, transactions: rows });
  } catch (err) { next(err); }
});

// ── Get Flagged Transactions (Admin only) ──────
app.get('/transactions/flagged', authMiddleware, adminMiddleware, async (req, res, next) => {
  try {
    const page = Math.max(1, parseInt(req.query.page) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit) || 15));
    const offset = (page - 1) * limit;

    const [transactions] = await pool.execute(
      "SELECT * FROM transactions WHERE status='FLAGGED' ORDER BY created_at DESC LIMIT ? OFFSET ?",
      [String(limit), String(offset)]
    );
    const [countRes] = await pool.execute("SELECT COUNT(*) as total FROM transactions WHERE status='FLAGGED'");
    
    res.json({
      success: true,
      transactions,
      pagination: {
        total: countRes[0].total,
        page,
        limit,
        pages: Math.ceil(countRes[0].total / limit)
      }
    });
  } catch (err) { next(err); }
});

// ── Admin Review Fraud Flagged Transaction ─────
app.patch('/transactions/:id/fraud-status', authMiddleware, adminMiddleware, async (req, res, next) => {
  const { status } = req.body;
  if (!['APPROVED', 'REJECTED'].includes(status)) {
    return next(createError(400, 'VALIDATION_ERROR', "status must be 'APPROVED' or 'REJECTED'"));
  }

  const txId = req.params.id;
  const adminId = req.user?.userId;
  const log = logger.child({ requestId: req.requestId, txId });

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    
    const [rows] = await conn.execute(
      "SELECT * FROM transactions WHERE id=? FOR UPDATE SKIP LOCKED", [txId]
    );
    if (rows.length === 0) { 
      await conn.rollback(); 
      return next(createError(404, 'TX_NOT_FOUND', 'Transaction not found or locked')); 
    }

    const tx = rows[0];
    if (tx.status !== 'FLAGGED') {
      await conn.rollback();
      return next(createError(400, 'INVALID_STATE', `Transaction is ${tx.status}, not FLAGGED`));
    }
    
    const eventType = status === 'APPROVED' ? 'FraudApproved' : 'FraudRejected';
    
    const [accRows] = await conn.execute('SELECT user_id FROM accounts WHERE id=?', [tx.from_account_id]);
    const senderUserId = accRows.length > 0 ? accRows[0].user_id : tx.from_account_id;

    const payload = JSON.stringify({ 
      transactionId: txId, 
      decision: status, 
      amount: tx.amount, 
      processedAt: new Date().toISOString(),
      userId: senderUserId,
      fromAccountId: tx.from_account_id,
      toAccountId: tx.to_account_id
    });

    await conn.execute(
      "INSERT INTO outbox_events (event_type, aggregate_id, payload, status) VALUES (?, ?, ?, 'UNPUBLISHED')",
      [eventType, String(txId), payload]
    );

    // Set to PROCESSING so it drops off the flagged queue immediately while Consumer finalizes it
    await conn.execute("UPDATE transactions SET status='PROCESSING' WHERE id=?", [txId]);

    await conn.commit();
    log.info({ status, adminId }, 'Manual fraud review submitted');
    res.json({ success: true, txId, status });
  } catch (err) {
    await conn.rollback(); next(err);
  } finally {
    conn.release();
  }
});

// ── Get Single Transaction ─────────────────────
app.get('/transactions/:id', async (req, res, next) => {
  try {
    const [rows] = await pool.execute('SELECT * FROM transactions WHERE id=?', [req.params.id]);
    if (rows.length === 0) return next(createError(404, 'TX_NOT_FOUND', 'Transaction not found'));
    res.json({ success: true, transaction: rows[0] });
  } catch (err) { next(err); }
});

// ── Global Error Handler ───────────────────────
app.use(errorMiddleware);

// ── Boot ───────────────────────────────────────
app.listen(PORT, () => logger.info({ port: PORT }, 'transaction-service listening'));

init().catch(err => {
  logger.fatal({ err }, 'transaction-service failed to initialise');
  process.exit(1);
});
