FROM node:22-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN if [ -f package-lock.json ]; then npm ci --omit=dev; else npm install --omit=dev; fi

COPY src ./src

ENV NODE_ENV=production
EXPOSE 3037

CMD ["node", "src/index.js"]
