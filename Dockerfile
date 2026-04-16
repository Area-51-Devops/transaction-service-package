FROM node:20-alpine
WORKDIR /usr/src/app

# Upgrade all OS packages to latest patched versions
RUN apk update && apk upgrade --no-cache

# Build-time arg for GitHub Packages authentication.
# Passed in by the CI pipeline via --build-arg NODE_AUTH_TOKEN=<token>.
# Written to .npmrc for the install step, then deleted so it never
# survives into the final image layer.
# Docker context is the repo root ('.'), not services/ — paths are bare.
COPY package*.json ./

# We mount a docker secret file representing the token during the run
RUN --mount=type=secret,id=npm_token \
  echo "//npm.pkg.github.com/:_authToken=$(cat /run/secrets/npm_token)" > .npmrc && \
  (npm ci --omit=dev || npm install --omit=dev) && \
  rm -f .npmrc && \
  npm cache clean --force && \
  rm -rf /usr/local/lib/node_modules/npm /usr/local/bin/npm /usr/local/bin/npx

COPY src ./src
USER node

EXPOSE 3003
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD wget -qO- http://localhost:3003/health/startup || exit 1
CMD ["node", "src/index.js"]
