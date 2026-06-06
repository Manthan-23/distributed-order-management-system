FROM node:18-alpine

# Enable pnpm
RUN corepack enable && corepack prepare pnpm@9.15.4 --activate

WORKDIR /app

COPY pnpm-lock.yaml package.json ./

RUN pnpm install --frozen-lockfile

COPY . .

EXPOSE 7070

CMD ["node", "index.js"]