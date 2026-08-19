#!/usr/bin/env node
'use strict';
/*
 * LIQUIDATION RECORDER  v1  — the featherweight, always-on half of the Edge Recorder.
 *
 * Records forced liquidations from THREE venues at once:
 *   Binance futures  (market-wide)
 *   Bybit linear     (per-symbol, majors + liquid alts)
 *   OKX swaps        (market-wide)
 *
 * Why three: liquidation history cannot be bought or backfilled anywhere, a single feed
 * going quiet is invisible until it's too late (we already got burned by one), and
 * cascades often start on one venue and spread — so cross-venue timing is itself signal.
 *
 * Footprint: a few MB per day. Negligible CPU. No dependencies, no API keys,
 * read-only market data. Nothing is ever traded.
 *
 * Run:  node liq-recorder.js
 */

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

const CONFIG = {
  outDir: path.join(__dirname, 'liq-data'),
  // Bybit needs explicit symbols (no market-wide channel). Binance and OKX are market-wide.
  bybitSymbols: ['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','DOGEUSDT','LINKUSDT','AVAXUSDT',
                 'SUIUSDT','WLDUSDT','ENAUSDT','ADAUSDT','LTCUSDT','BCHUSDT','DOTUSDT',
                 'NEARUSDT','APTUSDT','ARBUSDT','OPUSDT','INJUSDT','TIAUSDT','SEIUSDT',
                 'PEPEUSDT','WIFUSDT','ORDIUSDT','FILUSDT'],
  statusMs: 60000,
};

if (typeof WebSocket === 'undefined') {
  console.error('Needs Node 22+ (built-in WebSocket). You have ' + process.version);
  process.exit(1);
}

fs.mkdirSync(CONFIG.outDir, { recursive: true });

// ------------------------------------------------------------- FILE WRITER --
let writer = null;
function write(obj) {
  const day = new Date().toISOString().slice(0, 10);
  if (!writer || writer.day !== day) {
    if (writer) {
      writer.stream.end();
      const old = writer.path;
      const out = fs.createWriteStream(old + '.gz');
      fs.createReadStream(old).pipe(zlib.createGzip()).pipe(out);
      out.on('finish', () => fs.unlink(old, () => {}));
    }
    const p = path.join(CONFIG.outDir, 'liquidations-' + day + '.jsonl');
    writer = { day, path: p, stream: fs.createWriteStream(p, { flags: 'a' }) };
    console.log('[' + new Date().toISOString() + '] opened ' + p);
  }
  writer.stream.write(JSON.stringify(obj) + '\n');
}

// ---------------------------------------------------------------- COUNTERS --
const stats = {
  started: new Date().toISOString(),
  binance: 0, bybit: 0, okx: 0,
  binanceUsd: 0, bybitUsd: 0, okxUsd: 0,
  reconnects: 0, lastError: null,
  lastEventAt: null,
};

// ---------------------------------------------------------------- HELPERS ---
function readPayload(ev, cb) {
  const d = ev.data;
  if (typeof d === 'string') return cb(d);
  if (d instanceof ArrayBuffer) return cb(Buffer.from(d).toString('utf8'));
  if (ArrayBuffer.isView(d)) return cb(Buffer.from(d.buffer, d.byteOffset, d.byteLength).toString('utf8'));
  if (d && typeof d.text === 'function') return void d.text().then(cb).catch(() => {});
}

const backoff = {};
function retry(name, fn) {
  stats.reconnects++;
  backoff[name] = Math.min((backoff[name] || 1000) * 2, 60000);
  const wait = backoff[name];
  console.log('[' + name + '] disconnected, reconnecting in ' + (wait / 1000) + 's');
  setTimeout(() => fn(), wait);
}

function record(venue, o) {
  o.venue = venue;
  write(o);
  stats[venue]++;
  stats[venue + 'Usd'] += o.usd || 0;
  stats.lastEventAt = new Date().toISOString();
}

// ---------------------------------------------------------------- BINANCE ---
function startBinance() {
  const tag = 'binance';
  let ws;
  try { ws = new WebSocket('wss://fstream.binance.com/ws/!forceOrder@arr'); }
  catch (e) { retry(tag, startBinance); return; }

  ws.onopen = () => { backoff[tag] = 1000; console.log('[binance] connected (market-wide)'); };
  ws.onmessage = (ev) => readPayload(ev, (text) => {
    try {
      const m = JSON.parse(text);
      const o = m.o || (m.data && m.data.o);
      if (!o) return;
      const px = parseFloat(o.ap || o.p), qty = parseFloat(o.q);
      record('binance', {
        t: o.T, s: o.s, side: o.S,
        victim: o.S === 'SELL' ? 'LONG' : 'SHORT',
        px, qty, usd: +(px * qty).toFixed(2),
      });
    } catch (e) { stats.lastError = 'bn:' + e.message; }
  });
  ws.onclose = () => retry(tag, startBinance);
  ws.onerror = () => { stats.lastError = 'bn-error'; try { ws.close(); } catch (e) {} };
}

// ------------------------------------------------------------------ BYBIT ---
function startBybit() {
  const tag = 'bybit';
  let ws, ping = null;
  try { ws = new WebSocket('wss://stream.bybit.com/v5/public/linear'); }
  catch (e) { retry(tag, startBybit); return; }

  ws.onopen = () => {
    backoff[tag] = 1000;
    console.log('[bybit] connected (' + CONFIG.bybitSymbols.length + ' symbols)');
    // Bybit caps args per request; send in batches of 10
    for (let i = 0; i < CONFIG.bybitSymbols.length; i += 10) {
      const args = CONFIG.bybitSymbols.slice(i, i + 10).map(s => 'allLiquidation.' + s);
      ws.send(JSON.stringify({ op: 'subscribe', args }));
    }
    ping = setInterval(() => { try { ws.send(JSON.stringify({ op: 'ping' })); } catch (e) {} }, 20000);
  };
  ws.onmessage = (ev) => readPayload(ev, (text) => {
    try {
      const m = JSON.parse(text);
      if (!m.topic || !Array.isArray(m.data)) return;
      for (const d of m.data) {
        const px = parseFloat(d.p), qty = parseFloat(d.v);
        // Bybit 'S' is the side of the position being closed's order: Buy order = shorts liquidated
        record('bybit', {
          t: d.T || m.ts, s: d.s, side: d.S,
          victim: d.S === 'Sell' ? 'LONG' : 'SHORT',
          px, qty, usd: +(px * qty).toFixed(2),
        });
      }
    } catch (e) { stats.lastError = 'bybit:' + e.message; }
  });
  ws.onclose = () => { if (ping) clearInterval(ping); retry(tag, startBybit); };
  ws.onerror = () => { stats.lastError = 'bybit-error'; try { ws.close(); } catch (e) {} };
}

// -------------------------------------------------------------------- OKX ---
function startOkx() {
  const tag = 'okx';
  let ws, ping = null;
  try { ws = new WebSocket('wss://ws.okx.com:8443/ws/v5/public'); }
  catch (e) { retry(tag, startOkx); return; }

  ws.onopen = () => {
    backoff[tag] = 1000;
    console.log('[okx] connected (market-wide swaps)');
    ws.send(JSON.stringify({ op: 'subscribe', args: [{ channel: 'liquidation-orders', instType: 'SWAP' }] }));
    ping = setInterval(() => { try { ws.send('ping'); } catch (e) {} }, 20000);
  };
  ws.onmessage = (ev) => readPayload(ev, (text) => {
    if (text === 'pong') return;
    try {
      const m = JSON.parse(text);
      if (!m.data || !Array.isArray(m.data)) return;
      for (const inst of m.data) {
        for (const d of (inst.details || [])) {
          const px = parseFloat(d.bkPx), qty = parseFloat(d.sz);
          record('okx', {
            t: +d.ts, s: inst.instId, side: d.side,
            victim: d.posSide === 'long' || d.side === 'sell' ? 'LONG' : 'SHORT',
            px, qty, usd: +(px * qty).toFixed(2),
          });
        }
      }
    } catch (e) { stats.lastError = 'okx:' + e.message; }
  });
  ws.onclose = () => { if (ping) clearInterval(ping); retry(tag, startOkx); };
  ws.onerror = () => { stats.lastError = 'okx-error'; try { ws.close(); } catch (e) {} };
}

// ----------------------------------------------------------------- STATUS ---
setInterval(() => {
  let bytes = 0;
  try { for (const f of fs.readdirSync(CONFIG.outDir)) bytes += fs.statSync(path.join(CONFIG.outDir, f)).size; } catch (e) {}
  const up = (Date.now() - Date.parse(stats.started)) / 3600000;
  const total = stats.binance + stats.bybit + stats.okx;
  const s = Object.assign({}, stats, {
    now: new Date().toISOString(),
    uptimeHours: +up.toFixed(2),
    totalEvents: total,
    totalUsd: Math.round(stats.binanceUsd + stats.bybitUsd + stats.okxUsd),
    diskMB: +(bytes / 1048576).toFixed(2),
    projectedMBPerDay: up > 0.2 ? +((bytes / 1048576) / (up / 24)).toFixed(2) : null,
  });
  fs.writeFileSync(path.join(CONFIG.outDir, 'status.json'), JSON.stringify(s, null, 1));
  console.log('[status] ' + s.now.slice(11, 19) +
    ' | binance ' + s.binance + ' | bybit ' + s.bybit + ' | okx ' + s.okx +
    ' | total ' + total + ' ($' + (s.totalUsd / 1e6).toFixed(1) + 'M)' +
    ' | ' + s.diskMB + 'MB' +
    (s.lastError ? '  err:' + s.lastError : ''));
}, CONFIG.statusMs);

console.log('LIQUIDATION RECORDER — writing to ' + CONFIG.outDir);
console.log('Three venues. A few MB a day. Leave it running for months.\n');
startBinance();
startBybit();
startOkx();

function shutdown() {
  console.log('\nstopping...');
  if (writer) writer.stream.end();
  setTimeout(() => process.exit(0), 400);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
