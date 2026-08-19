#!/usr/bin/env node
'use strict';
/*
 * EDGE RECORDER  v1
 * Records the market data that does NOT exist in free historical archives:
 *   1. Forced liquidations (every one, market-wide)  -> raw, they are rare and precious
 *   2. Signed trade flow (who was the aggressor)     -> aggregated to 1-second bars
 *   3. Top of order book (bid/ask + sizes)           -> sampled once per second
 * from BOTH Binance futures (deep, has liquidation feed) and Hyperliquid (where we execute).
 *
 * No dependencies. No API keys. Read-only market data. Nothing is ever traded.
 * Run:  node recorder.js
 */

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

// ----------------------------------------------------------------- CONFIG ---
const CONFIG = {
  outDir: path.join(__dirname, 'data'),
  // coins to record in detail (bid/ask + signed flow). Liquidations are market-wide regardless.
  binanceCoins: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT',
                 'LINKUSDT', 'AVAXUSDT', 'SUIUSDT', 'WLDUSDT', 'ENAUSDT'],
  hlCoins:      ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE',
                 'LINK', 'AVAX', 'SUI', 'WLD', 'ENA'],
  flushMs: 1000,          // write one row per coin per second
  statusMs: 10000,        // refresh status.json
};

if (typeof WebSocket === 'undefined') {
  console.error('This needs Node 22+ (built-in WebSocket). You have ' + process.version);
  process.exit(1);
}

// ------------------------------------------------------------ FILE WRITER ---
fs.mkdirSync(CONFIG.outDir, { recursive: true });

const writers = {};          // name -> {day, stream, path, lines}
function dayStamp(ms) { return new Date(ms).toISOString().slice(0, 10); }

function gzipOldFile(p) {
  // compress yesterday's file in the background; delete the original on success
  fs.access(p, fs.constants.F_OK, (err) => {
    if (err) return;
    const inp = fs.createReadStream(p);
    const out = fs.createWriteStream(p + '.gz');
    inp.pipe(zlib.createGzip()).pipe(out);
    out.on('finish', () => fs.unlink(p, () => {}));
  });
}

function write(name, obj) {
  const now = Date.now();
  const day = dayStamp(now);
  let w = writers[name];
  if (!w || w.day !== day) {
    if (w) { w.stream.end(); gzipOldFile(w.path); }
    const dir = path.join(CONFIG.outDir, day);
    fs.mkdirSync(dir, { recursive: true });
    const p = path.join(dir, name + '.jsonl');
    w = writers[name] = { day, path: p, lines: (w ? 0 : 0),
                          stream: fs.createWriteStream(p, { flags: 'a' }) };
    console.log('[' + new Date().toISOString() + '] opened ' + p);
  }
  w.stream.write(JSON.stringify(obj) + '\n');
  w.lines++;
}

// --------------------------------------------------------------- COUNTERS ---
const stats = {
  started: new Date().toISOString(),
  binanceTrades: 0, binanceBook: 0, liquidations: 0, liqUsd: 0,
  hlTrades: 0, hlBook: 0,
  reconnects: 0, lastError: null,
};

// ------------------------------------------------------- 1s AGGREGATORS ----
// bucket[sym] = { o,h,l,c, bv, sv, n, bid, ask, bq, aq }
const bnBucket = {};
const hlBucket = {};

function touch(map, sym) {
  if (!map[sym]) map[sym] = { o: null, h: -Infinity, l: Infinity, c: null,
                              bv: 0, sv: 0, n: 0, bid: null, ask: null, bq: null, aq: null };
  return map[sym];
}

function onTrade(map, sym, px, qty, isBuyAggressor) {
  const b = touch(map, sym);
  if (b.o === null) b.o = px;
  if (px > b.h) b.h = px;
  if (px < b.l) b.l = px;
  b.c = px;
  if (isBuyAggressor) b.bv += px * qty; else b.sv += px * qty;
  b.n++;
}

function onBook(map, sym, bid, bq, ask, aq) {
  const b = touch(map, sym);
  b.bid = bid; b.bq = bq; b.ask = ask; b.aq = aq;
}

function flush() {
  const t = Math.floor(Date.now() / 1000) * 1000;
  for (const [map, name] of [[bnBucket, 'binance_1s'], [hlBucket, 'hl_1s']]) {
    for (const sym of Object.keys(map)) {
      const b = map[sym];
      if (b.n === 0 && b.bid === null) { delete map[sym]; continue; }
      write(name, {
        t, s: sym,
        o: b.o, h: b.h === -Infinity ? null : b.h, l: b.l === Infinity ? null : b.l, c: b.c,
        bv: +b.bv.toFixed(2),        // $ bought by aggressive buyers this second
        sv: +b.sv.toFixed(2),        // $ sold by aggressive sellers this second
        n: b.n,                      // trade count
        bid: b.bid, bq: b.bq, ask: b.ask, aq: b.aq,
      });
      delete map[sym];
    }
  }
}
setInterval(flush, CONFIG.flushMs);

// ------------------------------------------------------------- BINANCE WS ---
function connectBinanceStreams() {
  const streams = [];
  for (const c of CONFIG.binanceCoins) {
    streams.push(c.toLowerCase() + '@aggTrade');
    streams.push(c.toLowerCase() + '@bookTicker');
  }
  const url = 'wss://fstream.binance.com/stream?streams=' + streams.join('/');
  let ws;
  try { ws = new WebSocket(url); } catch (e) { retry('binance-streams', connectBinanceStreams); return; }

  ws.onopen = () => console.log('[binance] market streams connected (' + streams.length + ' streams)');
  ws.onmessage = (ev) => {
    try {
      const m = JSON.parse(ev.data);
      const d = m.data; if (!d) return;
      if (d.e === 'aggTrade') {
        // d.m === true  => the BUYER was the maker => the aggressor was a SELLER
        onTrade(bnBucket, d.s, parseFloat(d.p), parseFloat(d.q), d.m === false);
        stats.binanceTrades++;
      } else if (d.b !== undefined && d.a !== undefined) {
        onBook(bnBucket, d.s, parseFloat(d.b), parseFloat(d.B), parseFloat(d.a), parseFloat(d.A));
        stats.binanceBook++;
      }
    } catch (e) { stats.lastError = 'bn-msg:' + e.message; }
  };
  ws.onclose = () => retry('binance-streams', connectBinanceStreams);
  ws.onerror = () => { stats.lastError = 'bn-stream-error'; try { ws.close(); } catch (e) {} };
}

function connectBinanceLiquidations() {
  const url = 'wss://fstream.binance.com/ws/!forceOrder@arr';
  let ws;
  try { ws = new WebSocket(url); } catch (e) { retry('binance-liq', connectBinanceLiquidations); return; }

  ws.onopen = () => console.log('[binance] liquidation feed connected (market-wide)');
  ws.onmessage = (ev) => {
    try {
      const m = JSON.parse(ev.data);
      const o = m.o; if (!o) return;
      const px = parseFloat(o.ap || o.p), qty = parseFloat(o.q);
      const usd = px * qty;
      // o.S is the side of the liquidation ORDER. A forced SELL means longs were liquidated.
      write('liquidations', {
        t: o.T, s: o.s, side: o.S, victim: o.S === 'SELL' ? 'LONG' : 'SHORT',
        px, qty, usd: +usd.toFixed(2),
      });
      stats.liquidations++; stats.liqUsd += usd;
    } catch (e) { stats.lastError = 'liq-msg:' + e.message; }
  };
  ws.onclose = () => retry('binance-liq', connectBinanceLiquidations);
  ws.onerror = () => { stats.lastError = 'bn-liq-error'; try { ws.close(); } catch (e) {} };
}

// -------------------------------------------------------- HYPERLIQUID WS ---
function connectHyperliquid() {
  let ws;
  try { ws = new WebSocket('wss://api.hyperliquid.xyz/ws'); }
  catch (e) { retry('hyperliquid', connectHyperliquid); return; }

  let ping = null;
  ws.onopen = () => {
    console.log('[hyperliquid] connected');
    for (const coin of CONFIG.hlCoins) {
      ws.send(JSON.stringify({ method: 'subscribe', subscription: { type: 'trades', coin } }));
      ws.send(JSON.stringify({ method: 'subscribe', subscription: { type: 'l2Book', coin } }));
    }
    ping = setInterval(() => { try { ws.send(JSON.stringify({ method: 'ping' })); } catch (e) {} }, 30000);
  };
  ws.onmessage = (ev) => {
    try {
      const m = JSON.parse(ev.data);
      if (m.channel === 'trades' && Array.isArray(m.data)) {
        for (const tr of m.data) {
          // HL: side 'B' = aggressive buy, 'A' = aggressive sell
          onTrade(hlBucket, tr.coin, parseFloat(tr.px), parseFloat(tr.sz), tr.side === 'B');
          stats.hlTrades++;
        }
      } else if (m.channel === 'l2Book' && m.data && m.data.levels) {
        const [bids, asks] = m.data.levels;
        if (bids && bids.length && asks && asks.length) {
          onBook(hlBucket, m.data.coin,
                 parseFloat(bids[0].px), parseFloat(bids[0].sz),
                 parseFloat(asks[0].px), parseFloat(asks[0].sz));
          stats.hlBook++;
        }
      }
    } catch (e) { stats.lastError = 'hl-msg:' + e.message; }
  };
  ws.onclose = () => { if (ping) clearInterval(ping); retry('hyperliquid', connectHyperliquid); };
  ws.onerror = () => { stats.lastError = 'hl-error'; try { ws.close(); } catch (e) {} };
}

// ------------------------------------------------------------ RECONNECTS ---
const backoff = {};
function retry(name, fn) {
  stats.reconnects++;
  backoff[name] = Math.min((backoff[name] || 1000) * 2, 60000);
  const wait = backoff[name];
  console.log('[' + name + '] disconnected, reconnecting in ' + (wait / 1000) + 's');
  setTimeout(() => { backoff[name] = 1000; fn(); }, wait);
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
    projectedMBPerDay: up > 0.05 ? +((bytes / 1048576) / (up / 24)).toFixed(1) : null,
  });
  fs.writeFileSync(path.join(CONFIG.outDir, 'status.json'), JSON.stringify(s, null, 1));
  console.log('[status] ' + s.now +
    ' | binance trades ' + s.binanceTrades +
    ' | liquidations ' + s.liquidations + ' ($' + Math.round(s.liqUsd / 1e6) + 'M)' +
    ' | HL trades ' + s.hlTrades +
    ' | disk ' + s.diskMB + 'MB' +
    (s.projectedMBPerDay ? ' (~' + s.projectedMBPerDay + 'MB/day)' : ''));
}, CONFIG.statusMs);

// ------------------------------------------------------------------ BOOT ---
console.log('EDGE RECORDER starting — writing to ' + CONFIG.outDir);
console.log('Recording ' + CONFIG.binanceCoins.length + ' coins on Binance + Hyperliquid, plus market-wide liquidations.');
console.log('Leave this running. Ctrl-C to stop. Safe to stop and restart any time.\n');
connectBinanceStreams();
connectBinanceLiquidations();
connectHyperliquid();

process.on('SIGINT', () => {
  console.log('\nstopping — flushing buffers...');
  flush();
  for (const k of Object.keys(writers)) writers[k].stream.end();
  setTimeout(() => process.exit(0), 500);
});
