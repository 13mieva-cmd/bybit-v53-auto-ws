#!/usr/bin/env node
const https = require('https');
const http = require('http');
const fs = require('fs');
const WebSocket = require('ws');

fs.mkdirSync('output', { recursive: true });

const REST = 'https://api.bybit.com';
const WS = 'wss://stream.bybit.com/v5/public/spot';
const ANN_URL = 'https://announcements.bybit.com/en/?category=new_crypto';
const REST_REFRESH_MS = Number(process.env.REST_REFRESH_MS || 30000);
const ANN_REFRESH_MS = Number(process.env.ANN_REFRESH_MS || 60000);
const ALERT_SCORE = Number(process.env.ALERT_SCORE || 72);
const MIN_TURNOVER = Number(process.env.MIN_TURNOVER || 100000);
const MIN_CHANGE = Number(process.env.MIN_CHANGE || 5);
const ONLY_NEW_DAYS = Number(process.env.ONLY_NEW_DAYS || 90);
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';
const WATCHLIST = (process.env.WATCHLIST || '').split(',').map(s => s.trim().toUpperCase()).filter(Boolean);
const STATE_FILE = 'output/bybit_v53_state.json';
const SNAPSHOT_FILE = 'output/bybit_v53_snapshot.json';
const LOG_FILE = 'output/bybit_v53_signals.log';
const PORT = process.env.PORT || 10000;

function now(){ return new Date().toISOString(); }
function log(msg){ const line = `[${now()}] ${msg}`; console.log(line); fs.appendFileSync(LOG_FILE, line + '\n'); }
function f(v,d=0){ const n=Number(v); return Number.isFinite(n) ? n : d; }
function clamp(n,a,b){ return Math.max(a, Math.min(b,n)); }
function mean(a){ return a.length ? a.reduce((s,x)=>s+x,0)/a.length : 0; }
function std(a){ const m=mean(a); return a.length ? Math.sqrt(mean(a.map(x => (x-m)*(x-m)))) : 0; }
function ema(vals, period){ if(!vals.length) return []; const k=2/(period+1); let prev=vals[0]; const out=[prev]; for(let i=1;i<vals.length;i++){ prev = vals[i]*k + prev*(1-k); out.push(prev); } return out; }
function atr(candles, period=14){ const trs=[]; for(let i=1;i<candles.length;i++){ const c=candles[i], p=candles[i-1]; trs.push(Math.max(c.high-c.low, Math.abs(c.high-p.close), Math.abs(c.low-p.close))); } return ema(trs, period); }
function pct(a,b){ return b ? (a/b)*100 : 0; }

function httpGet(url, headers={}){
  return new Promise((resolve,reject)=>{
    https.get(url, {headers:{'User-Agent':'bybit-v5.3/1.0', ...headers}}, res => {
      let data='';
      res.on('data', c=>data += c);
      res.on('end', ()=>resolve({statusCode:res.statusCode, data}));
    }).on('error', reject);
  });
}

function get(path){
  return httpGet(REST + path, {'Accept':'application/json'}).then(r => {
    if(r.statusCode && r.statusCode >= 400) {
      const e = new Error(`HTTP ${r.statusCode}: ${r.data.slice(0,180)}`);
      e.statusCode = r.statusCode;
      throw e;
    }
    const j = JSON.parse(r.data);
    if(String(j.retCode) !== '0') {
      const e = new Error(JSON.stringify(j));
      e.retCode = j.retCode;
      throw e;
    }
    return j.result;
  });
}

function postTelegram(text){
  return new Promise((resolve,reject)=>{
    if(!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return resolve(false);
    const body = JSON.stringify({chat_id:TELEGRAM_CHAT_ID, text, parse_mode:'HTML', disable_web_page_preview:true});
    const req = https.request({
      method:'POST',
      hostname:'api.telegram.org',
      path:`/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
      headers:{'Content-Type':'application/json','Content-Length':Buffer.byteLength(body)}
    }, res => {
      res.resume();
      res.on('end', ()=>resolve(res.statusCode >= 200 && res.statusCode < 300));
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function loadState(){
  try { return JSON.parse(fs.readFileSync(STATE_FILE,'utf8')); }
  catch { return {seenNews:[], lastAlert:'', cache:{}, symbols:[], wsSubscribed:{}, newsSet:[]}; }
}
function saveState(s){ fs.writeFileSync(STATE_FILE, JSON.stringify(s, null, 2)); }

async function fetchAnnouncements(){
  try {
    const r = await httpGet(ANN_URL, {'Accept':'text/html'});
    const html = r.data || '';
    const re = /\b([A-Z0-9]{2,15})USDT\b/g;
    const out = new Set();
    let m;
    while((m = re.exec(html)) !== null) out.add(m[1] + 'USDT');
    return [...out];
  } catch (e) {
    log(`ANN ERR ${e.message}`);
    return [];
  }
}

async function fetchUniverse(){
  const [inst, tick] = await Promise.all([
    get('/v5/market/instruments-info?category=spot'),
    get('/v5/market/tickers?category=spot')
  ]);
  const tickMap = new Map((tick.list || []).map(x => [x.symbol, x]));
  const nowMs = Date.now();
  const rows = [];
  for(const x of (inst.list || [])){
    if(x.status !== 'Trading' || !x.symbol.endsWith('USDT')) continue;
    if(WATCHLIST.length && !WATCHLIST.includes(x.symbol)) continue;
    const t = tickMap.get(x.symbol);
    if(!t) continue;
    const launch = /^\d+$/.test(String(x.launchTime || '')) ? Number(x.launchTime) : null;
    const age_days = launch ? (nowMs - launch)/86400000 : 9999;
    const change24h_pct = f(t.price24hPcnt) * 100;
    const turnover24h_usdt = f(t.turnover24h);
    rows.push({
      symbol:x.symbol,
      baseCoin:x.baseCoin,
      age_days:Math.round(age_days*100)/100,
      last_price:f(t.lastPrice),
      change24h_pct:Math.round(change24h_pct*100)/100,
      turnover24h_usdt:Math.round(turnover24h_usdt*100)/100,
      volume24h:f(t.volume24h),
      bid:f(t.bid1Price),
      ask:f(t.ask1Price)
    });
  }
  return rows.filter(r => r.age_days <= ONLY_NEW_DAYS && r.turnover24h_usdt >= MIN_TURNOVER && r.change24h_pct >= MIN_CHANGE);
}

async function fetchKlines(symbol, interval, limit=120){
  const res = await get(`/v5/market/kline?category=spot&symbol=${encodeURIComponent(symbol)}&interval=${interval}&limit=${limit}`);
  return (res.list || []).map(x => ({
    start:Number(x[0]),
    open:Number(x[1]),
    high:Number(x[2]),
    low:Number(x[3]),
    close:Number(x[4]),
    volume:Number(x[5])
  })).reverse();
}

function scoreSignal(row, k15, k60, news, wsTick){
  const close15 = k15.map(c=>c.close), close60 = k60.map(c=>c.close), vol15 = k15.map(c=>c.volume);
  const e15_20 = ema(close15,20), e15_50 = ema(close15,50), e60_50 = ema(close60,50), e60_200 = ema(close60,200);
  const atr15 = atr(k15,14), atrNow = atr15[atr15.length-1] || 0, atrAvg30 = mean(atr15.slice(-30)) || 1;
  const volAvg20 = mean(vol15.slice(-20)) || 1, volStd20 = std(vol15.slice(-20)) || 1;
  const volSpike = (vol15[vol15.length-1] - volAvg20) / volStd20;
  const atrSpike = atrNow / atrAvg30;
  const last = k15[k15.length-1], prev = k15[k15.length-2];
  const bullish1h = close60[close60.length-1] > e60_200[e60_200.length-1] && e60_50[e60_50.length-1] > e60_200[e60_200.length-1];
  const bullish15 = e15_20[e15_20.length-1] > e15_50[e15_50.length-1] || e15_20[e15_20.length-2] <= e15_50[e15_50.length-2];
  const breakout = last.close > Math.max(...k15.slice(-10,-1).map(c=>c.high));
  const retest = prev.close <= Math.max(...k15.slice(-10,-1).map(c=>c.high)) && last.close > e15_20[e15_20.length-1];
  const spreadPct = pct(row.ask - row.bid, (row.ask + row.bid)/2);
  const wsMom = wsTick && wsTick.last ? clamp(((wsTick.last - row.last_price) / row.last_price) * 1000, -10, 10) : 0;
  const newsBoost = news ? 20 : 0;
  const ageScore = row.age_days <= 3 ? 30 : row.age_days <= 7 ? 24 : row.age_days <= 14 ? 18 : row.age_days <= 30 ? 12 : 6;
  const volumeScore = clamp(row.turnover24h_usdt / 100000, 0, 20);
  const momentumScore = clamp(row.change24h_pct * 1.2, 0, 20);
  const inplayScore = clamp((volSpike * 4) + ((atrSpike - 1) * 8) - (spreadPct * 40) + wsMom, 0, 20);
  const trendScore = bullish1h ? 20 : 0;
  const triggerScore = bullish15 && (breakout || retest) ? 20 : 0;
  const score = Math.round(ageScore + volumeScore + momentumScore + inplayScore + trendScore + triggerScore + newsBoost);
  const status = score >= ALERT_SCORE && bullish1h && bullish15 && (breakout || retest) && volSpike >= 1.5 ? 'LONG' : (bullish1h || bullish15 || news) ? 'WAIT' : 'NO LONG';
  const entry = last.close;
  const stop = Math.min(...k15.slice(-10).map(c=>c.low), entry - atrNow);
  const risk = Math.max(entry - stop, 1e-8);
  const tp1 = entry + risk * 1.5;
  const tp2 = entry + risk * 2.5;
  return {
    symbol: row.symbol,
    baseCoin:row.baseCoin,
    status,
    score,
    news,
    bullish1h,
    bullish15,
    breakout,
    retest,
    volSpike:+volSpike.toFixed(2),
    atrSpike:+atrSpike.toFixed(2),
    spreadPct:+spreadPct.toFixed(2),
    wsMom:+wsMom.toFixed(2),
    entry:+entry.toFixed(8),
    stop:+stop.toFixed(8),
    tp1:+tp1.toFixed(8),
    tp2:+tp2.toFixed(8),
    last_price:row.last_price,
    change24h_pct:row.change24h_pct,
    turnover24h_usdt:row.turnover24h_usdt,
    age_days:row.age_days
  };
}

function tg(r){
  return [
    `${r.status} — ${r.symbol}`,
    `score: ${r.score}/100`,
    `1h: ${r.bullish1h ? 'bullish' : 'bearish'}`,
    `15m: ${r.bullish15 ? 'confirm' : 'weak'}`,
    `news: ${r.news ? 'yes' : 'no'}`,
    `in-play: vol x${r.volSpike}, atr x${r.atrSpike}, spread ${r.spreadPct}%, ws ${r.wsMom}%`,
    `entry: ${r.entry}`,
    `sl: ${r.stop}`,
    `tp1: ${r.tp1}`,
    `tp2: ${r.tp2}`,
    `24h: ${r.change24h_pct}% | vol: ${Math.round(r.turnover24h_usdt)} | age: ${r.age_days}d`,
    `ann: ${ANN_URL}`
  ].join('\n');
}

async function updateNews(state){
  const news = await fetchAnnouncements();
  state.newsSet = news;
  for(const s of news){
    if(!state.seenNews.includes(s)){
      state.seenNews.push(s);
      log(`NEWS ${s}`);
      await postTelegram(`NEWS WATCH\n${s}\nAdded to watch context.\n${ANN_URL}`);
    }
  }
  state.seenNews = state.seenNews.slice(-200);
  saveState(state);
}

async function evaluate(state){
  let universe = [];
  try { universe = await fetchUniverse(); } catch (e) { log(`UNIVERSE ERR ${e.message}`); }
  const results = [];
  for(const row of universe.slice(0, 25)){
    try {
      const k15 = await fetchKlines(row.symbol, '15', 120);
      const k60 = await fetchKlines(row.symbol, '60', 120);
      if(k15.length < 60 || k60.length < 60) continue;
      const wsTick = state.cache[row.symbol] || null;
      const sig = scoreSignal(row, k15, k60, (state.newsSet || []).includes(row.symbol), wsTick);
      results.push(sig);
      if(sig.status === 'LONG'){
        const fp = `${sig.symbol}:${sig.score}:${sig.entry}`;
        if(state.lastAlert !== fp){
          state.lastAlert = fp;
          await postTelegram(tg(sig));
        }
      }
    } catch (e) {
      log(`SYMBOL ERR ${row.symbol} ${e.message}`);
    }
  }
  results.sort((a,b) => b.score - a.score);
  fs.writeFileSync(SNAPSHOT_FILE, JSON.stringify({ts: now(), results}, null, 2));
  if(results[0]) log(`${results[0].status} ${results[0].symbol} score=${results[0].score} news=${results[0].news} ws=${results[0].wsMom}`);
  else log('No candidates');
  saveState(state);
}

function startWS(state){
  const ws = new WebSocket(WS);
  ws.on('open', () => {
    log('WS connected');
    const syms = state.symbols && state.symbols.length ? state.symbols : WATCHLIST;
    for(const sym of syms.slice(0, 150)) {
      ws.send(JSON.stringify({op:'subscribe', args:[`tickers.${sym}`]}));
      ws.send(JSON.stringify({op:'subscribe', args:[`kline.15.${sym}`]}));
      ws.send(JSON.stringify({op:'subscribe', args:[`kline.60.${sym}`]}));
    }
  });
  ws.on('message', raw => {
    try {
      const msg = JSON.parse(raw.toString());
      if(msg.topic && msg.topic.startsWith('tickers.')){
        const sym = msg.topic.split('.')[1];
        const d = Array.isArray(msg.data) ? msg.data[0] : msg.data;
        if(d) state.cache[sym] = {last: f(d.lastPrice), bid: f(d.bid1Price), ask: f(d.ask1Price), ts: Date.now()};
      }
      if(msg.topic && msg.topic.startsWith('kline.')){
        const parts = msg.topic.split('.');
        const interval = parts[1];
        const sym = parts[2];
        const d = Array.isArray(msg.data) ? msg.data[0] : msg.data;
        if(d && d.confirm) state.wsSubscribed[sym] = {interval, close: f(d.close), ts: Date.now()};
      }
    } catch {}
  });
  ws.on('close', () => { log('WS closed, reconnecting'); setTimeout(() => startWS(state), 3000); });
  ws.on('error', e => log(`WS error ${e.message}`));
}

function startHealthServer(){
  http.createServer((req, res) => {
    res.writeHead(200, {'Content-Type':'text/plain'});
    res.end('OK');
  }).listen(PORT, '0.0.0.0', () => {
    log(`Health server on port ${PORT}`);
  });
}

async function main(){
  const state = loadState();
  log(`START v5.3 ALERT_SCORE=${ALERT_SCORE}`);
  if(TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID) log('Telegram enabled');
  if(WATCHLIST.length) log(`Watchlist: ${WATCHLIST.join(', ')}`);
  startHealthServer();
  startWS(state);
  await updateNews(state);
  await evaluate(state);
  setInterval(() => updateNews(state).catch(e => log(`NEWS ${e.message}`)), ANN_REFRESH_MS);
  setInterval(() => evaluate(state).catch(e => log(`EVAL ${e.message}`)), REST_REFRESH_MS);
  setInterval(() => saveState(state), 10000);
}

main().catch(e => { log(`FATAL ${e.message}`); process.exit(1); });
