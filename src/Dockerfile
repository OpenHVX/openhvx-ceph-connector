FROM node:20-slim

WORKDIR /app

COPY package.json tsconfig.json ./
COPY src ./src

RUN npm install --production=false
RUN npm run build

CMD ["node", "dist/app.js"]
