const jwt = require('jsonwebtoken');
const { createError } = require('./errorMiddleware');

const JWT_SECRET = process.env.JWT_SECRET || 'nexus_banking_secret';

const authMiddleware = (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return next(createError(401, 'UNAUTHORIZED', 'Missing or invalid authorization header'));
    }
    const token = authHeader.split(' ')[1];
    req.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch (err) {
    next(createError(401, 'INVALID_TOKEN', 'Token is invalid or expired'));
  }
};

const adminMiddleware = (req, res, next) => {
  if (req.user && req.user.role === 'ADMIN') {
    next();
  } else {
    next(createError(403, 'FORBIDDEN', 'Requires admin privileges'));
  }
};

module.exports = { authMiddleware, adminMiddleware };
