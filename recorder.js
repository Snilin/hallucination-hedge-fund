#!/usr/bin/env node
'use strict';
/*
 * EDGE RECORDER  v2
 * Records the market data that does NOT exist in free historical archives:
 *   1. Forced liquidations (every one, market-wide)  -> raw, they are rare and precious
 *   2. Signed trade flow (who was the aggressor)     -> aggregated to 1-second bars
 *   3. Top of order book (bid/ask + sizes)           -> sampled once per second
 * from BOTH Binance futures (deep, has the liquidation feed) and Hyperliquid (where we execute).
 *
 * No dependencies. No API keys. Read-only market data. Nothing is ever traded.
 * Run:  node recorder.js
 *
 * v3 changes: Binance's @aggTrade stream accepts the subscription but never sends
 * data (verified against the live venue), so trades now come from @trade. Trades and
 * book run on separate connections so a silent feed is obvious, every connection
 * reports its own raw message count, and the first message from each feed is saved
 * to data/debug_first_messages.json.
 */

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

// ----------------------------------------------------------------- CONFIG ---
const CONFIG = {
  outDir: path.join(__dirname, 'data'),
  binanceCoins: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT',
                 'LINKUSDT', 'AVAXUSDT', 'SUIUSDT', 'WLDUSDT', 'ENAUSDT'],
  hlCoins:      ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE',
                 'LINK', 'AVAX', 'SUI', 'WLD', 'ENA'],
  flushMs: 1000,
  statusMs: 10000,
};

if (typeof WebSocket === 'undefined') {
  console.error('Needs Node 22+ (built-in WebSocket). You have ' + process.version);
  process.exit(1);
}

// ------------------------------------------------------------ FILE WRITER ---
fs.mkdirSync(CONFIG.outDir, { recursive: true });
const writers = {};
const dayStamp = (ms) => new Date(ms).toISOString().slice(0, 10);

function gzipOld(p) {
  fs.access(p, fs.constants.F_OK, (err) => {
    if (err) return;
    const out = fs.createWriteStream(p + '.gz');
    fs.createReadStream(p).pipe(zlib.createGzip()).pipe(out);
    out.on('finish', () => fs.unlink(p, () => {}));
  });
}

function write(name, obj) {
  const day = dayStamp(Date.now());
  let w = writers[name];
  if (!w || w.day !== day) {
    if (w) { w.stream.end(); gzipOld(w.path); }
    const dir = path.join(CONFIG.outDir, day);
    fs.mkdirSync(dir, { recursive: true });
    const p = path.join(dir, name + '.jsonl');
    w = writers[name] = { day, path: p, stream: fs.createWriteStream(p, { flags: 'a' }) };
    console.log('[' + new Date().toISOString() + '] opened ' + p);
  }
  w.stream.write(JSON.stringify(obj) + '\n');
}

// --------------------------------------------------------------- COUNTERS ---
const stats = {
  started: new Date().toISOString(),
  rawBnTrade: 0, rawBnBook: 0, rawBnLiq: 0, rawHl: 0,
  bnTrades: 0, bnBook: 0, liquidations: 0, liqUsd: 0, hlTrades: 0, hlBook: 0,
  reconnects: 0, lastError: null,
};
const firstMsg = {};
function captureFirst(tag, text) {
  if (firstMsg[tag]) return;
  firstMsg[tag] = String(text).slice(0, 600);
  try {
    fs.writeFileSync(path.join(CONFIG.outDir, 'debug_first_messages.json'),
                     JSON.stringify(firstMsg, null, 1));
  } catch (e) {}
}

// ----------------------------------------------------------- 1s BUCKETS ----
const bn = {}, hl = {};
function touch(map, sym) {
  if (!map[sym]) map[sym] = { o: null, h: -Infinity, l: Infinity, c: null,
                              bv: 0, sv: 0, n: 0, bid: null, ask: null, bq: null, aq: null };
  return map[sym];
}
function onTrade(map, sym, px, qty, buyAggressor) {
  const b = touch(map, sym);
  if (b.o === null) b.o = px;
  if (px > b.h) b.h = px;
  if (px < b.l) b.l = px;
  b.c = px;
  if (buyAggressor) b.bv += px * qty; else b.sv += px * qty;
  b.n++;
}
function onBook(map, sym, bid, bq, ask, aq) {
  const b = touch(map, sym);
  b.bid = bid; b.bq = bq; b.ask = ask; b.aq = aq;
}
function flush() {
  const t = Math.floor(Date.now() / 1000) * 1000;
  for (const [map, name] of [[bn, 'binance_1s'], [hl, 'hl_1s']]) {
    for (const sym of Object.keys(map)) {
      const b = map[sym];
      if (b.n === 0 && b.bid === null) { delete map[sym]; continue; }
      write(name, { t, s: sym,
        o: b.o, h: b.h === -Infinity ? null : b.h, l: b.l === Infinity ? null : b.l, c: b.c,
        bv: +b.bv.toFixed(2), sv: +b.sv.toFixed(2), n: b.n,
        bid: b.bid, bq: b.bq, ask: b.ask, aq: b.aq });
      delete map[sym];
    }
  }
}
setInterval(flush, CONFIG.flushMs);

// -------------------------------------------------------- MESSAGE HELPER ---
// Node's WebSocket may hand us a string, an ArrayBuffer, or a Blob. Normalise all three.
function readPayload(ev, cb) {
  const d = ev.data;
  if (typeof d === 'string') return cb(d);
  if (d instanceof ArrayBuffer) return cb(Buffer.from(d).toString('utf8'));
  if (ArrayBuffer.isView(d)) return cb(Buffer.from(d.buffer, d.byteOffset, d.byteLength).toString('utf8'));
  if (d && typeof d.text === 'function') return void d.text().then(cb).catch(() => {});
}

// ------------------------------------------------------------ CONNECTIONS ---
const backoff = {};
function retry(name, fn) {
  stats.reconnects++;
  backoff[name] = Math.min((backoff[name] || 1000) * 2, 60000);
  const wait = backoff[name];
  console.log('[' + name + '] disconnected, reconnecting in ' + (wait / 1000) + 's');
  setTimeout(() => { backoff[name] = 1000; fn(); }, wait);
}

function binanceFeed(tag, streams, handler) {
  const url = 'wss://fstream.binance.com/stream?streams=' + streams.join('/');
  let ws;
  try { ws = new WebSocket(url); }
  catch (e) { stats.lastError = tag + ':' + e.message; retry(tag, () => binanceFeed(tag, streams, handler)); return; }

  ws.onopen = () => { backoff[tag] = 1000; console.log('[' + tag + '] connected (' + streams.length + ' streams)'); };
  ws.onmessage = (ev) => readPayload(ev, (text) => {
    captureFirst(tag, text);
    try {
      const m = JSON.parse(text);
      if (m.error) { stats.lastError = tag + ':' + JSON.stringify(m.error); return; }
      const d = m.data || m;
      handler(d);
    } catch (e) { stats.lastError = tag + '-parse:' + e.message; }
  });
  ws.onclose = () => retry(tag, () => binanceFeed(tag, streams, handler));
  ws.onerror = () => { stats.lastError = tag + '-error'; try { ws.close(); } catch (e) {} };
}

function startBinance() {
  // --- trades, on their own connection so silence is obvious ---
  binanceFeed('bn-trades',
    CONFIG.binanceCoins.map(c => c.toLowerCase() + '@trade'),
    (d) => {
      stats.rawBnTrade++;
      // match by shape as well as by event name, in case the field is ever absent
      const isTrade = d.e === 'trade' || d.e === 'aggTrade' ||
                      (d.p !== undefined && d.q !== undefined && typeof d.m === 'boolean');
      if (!isTrade || !d.s) return;
      // d.m === true => the BUYER was the maker => the aggressor was a SELLER
      onTrade(bn, d.s, parseFloat(d.p), parseFloat(d.q), d.m === false);
      stats.bnTrades++;
    });

  // --- top of book, separate connection ---
  binanceFeed('bn-book',
    CONFIG.binanceCoins.map(c => c.toLowerCase() + '@bookTicker'),
    (d) => {
      stats.rawBnBook++;
      if (d.b === undefined || d.a === undefined || !d.s) return;
      onBook(bn, d.s, parseFloat(d.b), parseFloat(d.B), parseFloat(d.a), parseFloat(d.A));
      stats.bnBook++;
    });
}

function startLiquidations() {
  const tag = 'bn-liq';
  let ws;
  try { ws = new WebSocket('wss://fstream.binance.com/ws/!forceOrder@arr'); }
  catch (e) { retry(tag, startLiquidations); return; }

  ws.onopen = () => { backoff[tag] = 1000; console.log('[' + tag + '] liquidation feed connected (market-wide)'); };
  ws.onmessage = (ev) => readPayload(ev, (text) => {
    captureFirst(tag, text);
    stats.rawBnLiq++;
    try {
      const m = JSON.parse(text);
      const o = m.o || (m.data && m.data.o);
      if (!o) return;
      const px = parseFloat(o.ap || o.p), qty = parseFloat(o.q), usd = px * qty;
      write('liquidations', {
        t: o.T, s: o.s, side: o.S,
        victim: o.S === 'SELL' ? 'LONG' : 'SHORT',
        px, qty, usd: +usd.toFixed(2),
      });
      stats.liquidations++; stats.liqUsd += usd;
    } catch (e) { stats.lastError = 'liq-parse:' + e.message; }
  });
  ws.onclose = () => retry(tag, startLiquidations);
  ws.onerror = () => { stats.lastError = 'liq-error'; try { ws.close(); } catch (e) {} };
}

function startHyperliquid() {
  const tag = 'hyperliquid';
  let ws;
  try { ws = new WebSocket('wss://api.hyperliquid.xyz/ws'); }
  catch (e) { retry(tag, startHyperliquid); return; }
  let ping = null;

  ws.onopen = () => {
    backoff[tag] = 1000;
    console.log('[' + tag + '] connected');
    for (const coin of CONFIG.hlCoins) {
      ws.send(JSON.stringify({ method: 'subscribe', subscription: { type: 'trades', coin } }));
      ws.send(JSON.stringify({ method: 'subscribe', subscription: { type: 'l2Book', coin } }));
    }
    ping = setInterval(() => { try { ws.send(JSON.stringify({ method: 'ping' })); } catch (e) {} }, 30000);
  };
  ws.onmessage = (ev) => readPayload(ev, (text) => {
    captureFirst(tag, text);
    stats.rawHl++;
    try {
      const m = JSON.parse(text);
      if (m.channel === 'trades' && Array.isArray(m.data)) {
        for (const tr of m.data) {
          onTrade(hl, tr.coin, parseFloat(tr.px), parseFloat(tr.sz), tr.side === 'B');
          stats.hlTrades++;
        }
      } else if (m.channel === 'l2Book' && m.data && m.data.levels) {
        const [bids, asks] = m.data.levels;
        if (bids && bids.length && asks && asks.length) {
          onBook(hl, m.data.coin,
                 parseFloat(bids[0].px), parseFloat(bids[0].sz),
                 parseFloat(asks[0].px), parseFloat(asks[0].sz));
          stats.hlBook++;
        }
      }
    } catch (e) { stats.lastError = 'hl-parse:' + e.message; }
  });
  ws.onclose = () => { if (ping) clearInterval(ping); retry(tag, startHyperliquid); };
  ws.onerror = () => { stats.lastError = 'hl-error'; try { ws.close(); } catch (e) {} };
}

// --------------------------------------------------------------- STATUS ----
function diskUsage(dir) {
  let total = 0;
  try {
    for (const day of fs.readdirSync(dir)) {
      const dp = path.join(dir, day);
      if (!fs.statSync(dp).isDirectory()) continue;
      for (const f of fs.readdirSync(dp)) total += fs.statSync(path.join(dp, f)).size;
    }
  } catch (e) {}
  return total;
}

setInterval(() => {
  const bytes = diskUsage(CONFIG.outDir);
  const up = (Date.now() - Date.parse(stats.started)) / 3600000;
  const s = Object.assign({}, stats, {
    now: new Date().toISOString(),
    uptimeHours: +up.toFixed(2),
    liqUsdTotal: Math.round(stats.liqUsd),
    diskMB: +(bytes / 1048576).toFixed(1),
    projectedMBPerDay: up > 0.02 ? +((bytes / 1048576) / (up / 24)).toFixed(1) : null,
    healthy: stats.bnTrades > 0 && stats.bnBook > 0 && stats.hlTrades > 0,
  });
  fs.writeFileSync(path.join(CONFIG.outDir, 'status.json'), JSON.stringify(s, null, 1));
  console.log('[status] ' + s.now.slice(11, 19) +
    ' | BN trades ' + s.bnTrades + '/' + s.rawBnTrade +
    ' book ' + s.bnBook + '/' + s.rawBnBook +
    ' | liq ' + s.liquidations + ' ($' + (s.liqUsd / 1e6).toFixed(1) + 'M)' +
    ' | HL trades ' + s.hlTrades + ' book ' + s.hlBook +
    ' | ' + s.diskMB + 'MB' + (s.projectedMBPerDay ? ' (~' + s.projectedMBPerDay + 'MB/day)' : '') +
    (s.healthy ? '' : '  <-- A FEED IS SILENT') +
    (s.lastError ? '  err:' + s.lastError : ''));
}, CONFIG.statusMs);

// ------------------------------------------------------------------ BOOT ---
console.log('EDGE RECORDER v3 — writing to ' + CONFIG.outDir);
console.log('Counters read  matched/raw  so you can see if a feed goes quiet.\n');
startBinance();
startLiquidations();
startHyperliquid();

function shutdown() {
  console.log('\nstopping — flushing buffers...');
  flush();
  for (const k of Object.keys(writers)) writers[k].stream.end();
  setTimeout(() => process.exit(0), 500);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
