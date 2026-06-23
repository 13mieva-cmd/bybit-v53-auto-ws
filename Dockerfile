FROM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --omit=dev
COPY bybit_v53_auto_ws.js ./
CMD ["npm", "start"]
