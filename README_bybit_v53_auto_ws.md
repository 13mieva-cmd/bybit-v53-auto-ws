# Bybit v5.3 Auto WS Scanner

Adds:
- auto WS subscription for tickers + kline 15m/1h
- news refresh
- cached state and alerts

## Run
```bash
TELEGRAM_BOT_TOKEN=xxx TELEGRAM_CHAT_ID=yyy node bybit_v53_auto_ws.js
```

## Params
- `ALERT_SCORE=72`
- `REST_REFRESH_MS=30000`
- `ANN_REFRESH_MS=60000`
- `WATCHLIST=SOLUSDT,ARXUSDT`
