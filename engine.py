"""Hallucination Hedge Fund — self-contained paper engine + dashboard data emitter.
Runs hourly on GitHub Actions. Reads live Hyperliquid (+ Yahoo SPX) public data,
applies the frozen 4-sleeve / 2-pool strategy, marks a $100,000 paper book, and
writes data.json (consumed by index.html) and standalone.html (data inlined).
Paper only: no keys, no orders.
"""
import json, os, time
import requests
import pandas as pd
import numpy as np

API = 'https://api.hyperliquid.xyz/info'
COST = 5.5e-4; SLIP = 5e-4
START = 100_000.0
MAJORS = ['BTC', 'ETH', 'SOL']
BASE_H = [(48, 480), (72, 600), (72, 720), (96, 840), (120, 960), (168, 1440)]
FAST_H = [(max(2, int(f * .5)), max(8, int(s * .5))) for f, s in BASE_H]
BASE_D = [(2, 20), (3, 25), (3, 30), (4, 35), (5, 40), (7, 60)]

# dollar allocation of the $100k book (the pie)
ALLOC = {'combo': 50_000, 'spx': 20_000, 'ign_majors': 12_000, 'ign_alts': 12_000, 'ign_spx': 6_000}
# risk per ignition event as fraction of that sleeve's own slice
IGN_RISK = {'ign_majors': 0.03, 'ign_alts': 0.015, 'ign_spx': 0.03}
HOLD_DAYS = 20
SLEEVE_META = {
    'combo': ('Core combo (BTC + ETH)', 'A', 'Trend long/short with bear-fit short leg + capitulation veto'),
    'spx': ('S&P 500 trend', 'A', 'Long-or-flat index trend (diversifier)'),
    'ign_majors': ('Ignition — majors', 'B', 'Fresh-trend breakout bursts on BTC/ETH/SOL'),
    'ign_alts': ('Ignition — alts', 'B', 'BTC-gated alt breakouts, top-10 concurrent'),
    'ign_spx': ('Ignition — S&P', 'B', 'Fresh-trend bursts on the S&P 500'),
}


def post(body):
    for k in range(5):
        try:
            r = requests.post(API, json=body, timeout=25)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        time.sleep(2 * (k + 1))
    return None


def candles(coin, interval, hours):
    now = int(time.time() * 1000)
    r = post({'type': 'candleSnapshot', 'req': {'coin': coin, 'interval': interval,
                                                'startTime': now - hours * 3600_000, 'endTime': now}})
    if not r:
        return None
    df = pd.DataFrame([{'t': x['t'], 'o': float(x['o']), 'h': float(x['h']),
                        'l': float(x['l']), 'c': float(x['c'])} for x in r])
    df.index = pd.to_datetime(df['t'], unit='ms', utc=True)
    return df[~df.index.duplicated()].sort_index()


def frac(c, pairs, direction='L'):
    if direction == 'L':
        return sum(float(c.ewm(span=f).mean().iloc[-1] > c.ewm(span=s).mean().iloc[-1]) for f, s in pairs) / len(pairs)
    return sum(float(c.ewm(span=f).mean().iloc[-1] < c.ewm(span=s).mean().iloc[-1]) for f, s in pairs) / len(pairs)


def frac_series(c, pairs):
    return sum((c.ewm(span=f).mean() > c.ewm(span=s).mean()).astype(float) for f, s in pairs) / len(pairs)


def atr_frac(dfd, n=20):
    tr = np.maximum(dfd['h'] - dfd['l'], np.maximum((dfd['h'] - dfd['c'].shift(1)).abs(),
                                                    (dfd['l'] - dfd['c'].shift(1)).abs()))
    return float((tr.rolling(n).mean() / dfd['c']).iloc[-1])


def yahoo_spx():
    try:
        r = requests.get('https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?range=2y&interval=1d',
                         headers={'User-Agent': 'Mozilla/5.0'}, timeout=25).json()
        q = r['chart']['result'][0]; ts = q['timestamp']; qq = q['indicators']['quote'][0]
        return pd.DataFrame({'o': qq['open'], 'h': qq['high'], 'l': qq['low'], 'c': qq['close']},
                            index=pd.to_datetime(ts, unit='s', utc=True)).dropna()
    except Exception:
        return None


def main():
    now = pd.Timestamp.utcnow().floor('h')
    st = json.load(open('state.json')) if os.path.exists('state.json') else dict(
        ts=None, t0=str(now), px={}, combo_pos={}, spx_pos=0.0, spx_px=None,
        mult={k: 1.0 for k in ALLOC}, events=[], closed=[], alt_last_scan=None)
    trades = []  # events opened/closed this tick

    H = {}
    for a in MAJORS:
        d = candles(a, '1h', 4000)
        if d is not None:
            H[a] = d
    px = {a: float(H[a]['c'].iloc[-1]) for a in H}

    # ---- sleeve 1: core combo ----
    combo_assets = [a for a in ['BTC', 'ETH'] if a in H]
    tgt, meta = {}, {}
    for a in combo_assets:
        c = H[a]['c']
        fl = frac(c, BASE_H, 'L'); fs = frac(c, FAST_H, 'S')
        veto = bool(((c / c.shift(240) - 1).iloc[-168:] < -0.25).any())
        tgt[a] = max(-1.0, min(1.0, (1.0 if fl >= 0.5 else 0.0) - (0.0 if veto else (1.0 if fs >= 0.5 else 0.0))))
        meta[a] = dict(fl=round(fl, 2), fs=round(fs, 2), veto=veto)
    if st['ts'] is not None:
        pc = 0.0
        for a in combo_assets:
            r = px[a] / st['px'].get(a, px[a]) - 1
            fh = post({'type': 'fundingHistory', 'coin': a, 'startTime': int(pd.Timestamp(st['ts']).timestamp() * 1000)})
            fund = sum(float(x['fundingRate']) for x in fh) if fh else 0.0
            pc += (st['combo_pos'].get(a, 0.0) * r - st['combo_pos'].get(a, 0.0) * fund) / max(1, len(combo_assets))
        st['mult']['combo'] *= (1 + pc)
    st.setdefault('combo_entry', {})
    for a in combo_assets:
        if tgt[a] != st['combo_pos'].get(a, 0.0):
            st['mult']['combo'] *= (1 - abs(tgt[a] - st['combo_pos'].get(a, 0.0)) * COST / max(1, len(combo_assets)))
            trades.append(dict(ts=str(now), sleeve='combo', coin=a, action='rotate',
                               to=tgt[a], price=px[a], note=f"long-votes {meta[a]['fl']}, short-votes {meta[a]['fs']}, veto {meta[a]['veto']}"))
            st['combo_pos'][a] = tgt[a]
            if tgt[a] != 0.0:
                st['combo_entry'][a] = px[a]          # avg entry of the current position
            else:
                st['combo_entry'].pop(a, None)
    # backfill entry for positions already open before this feature existed
    for a in combo_assets:
        if st['combo_pos'].get(a, 0.0) != 0.0 and a not in st['combo_entry']:
            rot = [t for t in st.get('trade_log', []) if t.get('sleeve') == 'combo' and t.get('coin') == a
                   and t.get('action') == 'rotate' and t.get('to', 0)]
            st['combo_entry'][a] = float(rot[-1]['price']) if rot else px.get(a)

    # ---- ignition helpers ----
    def close_check(sleeve, coin, dfh):
        for ev in [e for e in st['events'] if e['sleeve'] == sleeve and e['coin'] == coin and e['open']]:
            seg = dfh[dfh.index > pd.Timestamp(ev['opened'])]
            stopped = bool((seg['l'] <= ev['stop']).any()) if len(seg) else False
            expired = (now - pd.Timestamp(ev['opened'])).days >= HOLD_DAYS
            if stopped or expired:
                xpx = ev['stop'] * (1 - SLIP) if stopped else float(dfh['c'].iloc[-1])
                rr = ((xpx / ev['entry'] - 1) - 2 * COST) / ev['stop_frac']
                st['mult'][sleeve] *= (1 + IGN_RISK[sleeve] * rr)
                ev['open'] = False; ev['R'] = round(rr, 3); ev['closed'] = str(now); ev['exit'] = xpx
                st['closed'].append(ev)
                trades.append(dict(ts=str(now), sleeve=sleeve, coin=coin, action='close',
                                   price=round(xpx, 4), note=f"{'STOP' if stopped else 'TIMEOUT'} · {rr:+.2f}R"))

    def open_ev(sleeve, coin, entry, sf):
        st['events'].append(dict(sleeve=sleeve, coin=coin, entry=entry, stop=entry * (1 - sf),
                                 stop_frac=sf, opened=str(now), open=True))
        trades.append(dict(ts=str(now), sleeve=sleeve, coin=coin, action='open',
                           price=round(entry, 4), note=f"stop {100*sf:.1f}% below"))

    # ---- sleeve 2: majors ignition ----
    for a in [m for m in MAJORS if m in H]:
        close_check('ign_majors', a, H[a])
        fr = frac_series(H[a]['c'], BASE_H)
        if fr.iloc[-1] >= 0.5 and fr.iloc[-2] < 0.5 and not any(e['sleeve'] == 'ign_majors' and e['coin'] == a and e['open'] for e in st['events']):
            dfd = H[a].resample('1D').agg({'o': 'first', 'h': 'max', 'l': 'min', 'c': 'last'}).dropna()
            open_ev('ign_majors', a, px[a], max(0.015, min(0.10, 1.2 * atr_frac(dfd))))

    # ---- sleeve 3: alt ignition (scan daily) ----
    btc_bull = frac(H['BTC']['c'], BASE_H, 'L') >= 0.5 if 'BTC' in H else False
    if (now.hour == 0) or st.get('alt_last_scan') is None:
        st['alt_last_scan'] = str(now)
        mu = post({'type': 'metaAndAssetCtxs'})
        if mu:
            uni = [(u['name'], float(mu[1][i].get('dayNtlVlm', 0) or 0)) for i, u in enumerate(mu[0]['universe']) if not u.get('isDelisted')]
            alts = [n for n, v in sorted(uni, key=lambda x: -x[1])[:50] if n not in MAJORS and v > 5e6]
            for coin in alts:
                dfd = candles(coin, '1d', 24 * 200)
                if dfd is None or len(dfd) < 120:
                    continue
                close_check('ign_alts', coin, dfd)
                if len([e for e in st['events'] if e['sleeve'] == 'ign_alts' and e['open']]) >= 10 or not btc_bull:
                    continue
                fr = frac_series(dfd['c'], BASE_D)
                if fr.iloc[-1] >= 0.5 and fr.iloc[-2] < 0.5 and not any(e['coin'] == coin and e['sleeve'] == 'ign_alts' and e['open'] for e in st['events']):
                    open_ev('ign_alts', coin, float(dfd['c'].iloc[-1]), 0.03)
    else:
        for ev in [e for e in st['events'] if e['sleeve'] == 'ign_alts' and e['open']]:
            dfd = candles(ev['coin'], '1d', 24 * 30)
            if dfd is not None:
                close_check('ign_alts', ev['coin'], dfd)

    # ---- sleeve 4: SPX ----
    spx = yahoo_spx()
    if spx is not None and len(spx) > 90:
        cs = spx['c']; fr = frac_series(cs, BASE_D)
        stg = 1.0 if fr.iloc[-1] >= 0.5 else 0.0
        if st['ts'] is not None and st.get('spx_px'):
            st['mult']['spx'] *= (1 + st.get('spx_pos', 0.0) * (float(cs.iloc[-1]) / st['spx_px'] - 1))
        if stg != st.get('spx_pos', 0.0):
            st['mult']['spx'] *= (1 - abs(stg - st.get('spx_pos', 0.0)) * COST)
            trades.append(dict(ts=str(now), sleeve='spx', coin='SPX', action='rotate', to=stg,
                               price=round(float(cs.iloc[-1]), 2), note=f"long-votes {fr.iloc[-1]:.2f}"))
            st['spx_pos'] = stg
            st['spx_entry'] = float(cs.iloc[-1]) if stg != 0.0 else None
        st['spx_px'] = float(cs.iloc[-1])
        if st.get('spx_pos', 0.0) != 0.0 and not st.get('spx_entry'):   # backfill
            rot = [t for t in st.get('trade_log', []) if t.get('sleeve') == 'spx' and t.get('action') == 'rotate' and t.get('to', 0)]
            st['spx_entry'] = float(rot[-1]['price']) if rot else float(cs.iloc[-1])
        close_check('ign_spx', 'SPX', spx)
        if fr.iloc[-1] >= 0.5 and fr.iloc[-2] < 0.5 and not any(e['sleeve'] == 'ign_spx' and e['open'] for e in st['events']):
            open_ev('ign_spx', 'SPX', float(cs.iloc[-1]), max(0.008, 1.2 * atr_frac(spx)))

    st['px'] = px; st['ts'] = str(now)
    if trades:
        st.setdefault('trade_log', [])
        st['trade_log'] = (st.get('trade_log', []) + trades)[-500:]
    json.dump(st, open('state.json', 'w'), indent=1, default=str)

    # ---- mark OPEN ignition positions to current market (live unrealized) ----
    price_cache = {}

    def cur_price(coin):
        if coin in H:
            return float(H[coin]['c'].iloc[-1])
        if coin == 'SPX':
            return st.get('spx_px')
        if coin in price_cache:
            return price_cache[coin]
        d = candles(coin, '1h', 72)
        p = float(d['c'].iloc[-1]) if (d is not None and len(d)) else None
        price_cache[coin] = p
        return p

    open_positions = []
    live_mult = dict(st['mult'])  # display copy; realized closes already in st['mult']
    for e in [x for x in st['events'] if x['open']]:
        cp = cur_price(e['coin'])
        ur = usd = None
        if cp:
            ur = ((cp / e['entry'] - 1) - COST) / e['stop_frac']   # unrealized R (entry cost paid)
            usd = ALLOC[e['sleeve']] * IGN_RISK[e['sleeve']] * ur
            live_mult[e['sleeve']] += IGN_RISK[e['sleeve']] * ur    # mark sleeve to market
        open_positions.append(dict(sleeve=e['sleeve'], coin=e['coin'], entry=e['entry'], stop=e['stop'],
                                   cur=cp, move_pct=round(100 * (cp / e['entry'] - 1), 2) if cp else None,
                                   unreal_R=round(ur, 2) if ur is not None else None,
                                   unreal_usd=round(usd, 2) if usd is not None else None,
                                   since=e['opened'][:10]))

    # ---- holdings by asset (what the book actually holds right now) ----
    holdings = []
    for a in combo_assets:
        pos = st['combo_pos'].get(a, 0.0)
        if pos != 0.0:
            entry = st.get('combo_entry', {}).get(a) or px.get(a)
            cp = px.get(a)
            mv = round(100 * (cp / entry - 1) * (1 if pos > 0 else -1), 2) if (entry and cp) else None
            holdings.append(dict(asset=a, sleeve='combo', side='LONG' if pos > 0 else 'SHORT',
                                 entry=round(entry, 4) if entry else None, cur=round(cp, 4) if cp else None,
                                 move_pct=mv, exposure=round(ALLOC['combo'] / max(1, len(combo_assets)) * abs(pos))))
    if st.get('spx_pos', 0.0) != 0.0 and st.get('spx_px'):
        entry = st.get('spx_entry') or st.get('spx_px'); cp = st.get('spx_px'); pos = st['spx_pos']
        holdings.append(dict(asset='S&P 500', sleeve='spx', side='LONG' if pos > 0 else 'SHORT',
                             entry=round(entry, 2), cur=round(cp, 2),
                             move_pct=round(100 * (cp / entry - 1) * (1 if pos > 0 else -1), 2),
                             exposure=round(ALLOC['spx'] * abs(pos))))
    for e in [x for x in st['events'] if x['open']]:
        cp = cur_price(e['coin'])
        notional = (ALLOC[e['sleeve']] * IGN_RISK[e['sleeve']]) / e['stop_frac'] if e.get('stop_frac') else None
        holdings.append(dict(asset=e['coin'], sleeve=e['sleeve'], side='LONG',
                             entry=round(e['entry'], 4), cur=round(cp, 4) if cp else None,
                             move_pct=round(100 * (cp / e['entry'] - 1), 2) if cp else None,
                             exposure=round(notional) if notional else None))

    # ---- equity history (marked to market) ----
    sleeve_val = {k: ALLOC[k] * live_mult[k] for k in ALLOC}
    fund_val = sum(sleeve_val.values())
    hist = json.load(open('history.json')) if os.path.exists('history.json') else []
    hist.append(dict(t=str(now), fund=round(fund_val, 2), **{k: round(v, 2) for k, v in sleeve_val.items()}))
    hist = hist[-5000:]
    json.dump(hist, open('history.json', 'w'))

    # ---- dashboard data.json ----
    def series_of(key):
        return [[h['t'], h[key]] for h in hist]

    live_pos = {a: st['combo_pos'].get(a, 0.0) for a in combo_assets}
    data = dict(
        name='Hallucination Hedge Fund',
        updated=str(now), since=st['t0'], start=START,
        fund=dict(value=round(fund_val, 2), pnl=round(fund_val - START, 2),
                  pnl_pct=round(100 * (fund_val / START - 1), 3), series=series_of('fund')),
        combo_positions=live_pos, spx_position=st.get('spx_pos', 0.0),
        sleeves=[dict(key=k, name=SLEEVE_META[k][0], pool=SLEEVE_META[k][1], desc=SLEEVE_META[k][2],
                      alloc=ALLOC[k], alloc_pct=round(100 * ALLOC[k] / START, 1),
                      value=round(sleeve_val[k], 2), pnl=round(sleeve_val[k] - ALLOC[k], 2),
                      pnl_pct=round(100 * (live_mult[k] - 1), 3), series=series_of(k))
                 for k in ALLOC],
        open_positions=open_positions,
        holdings=holdings,
        closed=[dict(sleeve=e['sleeve'], coin=e['coin'], R=e.get('R'), opened=e['opened'][:10], closed=e.get('closed', '')[:10])
                for e in st['closed'][-100:]],
        trade_log=st.get('trade_log', [])[-60:],
    )
    json.dump(data, open('data.json', 'w'), indent=1, default=str)

    # ---- inline data into standalone.html for private/offline viewing ----
    if os.path.exists('index.html'):
        html = open('index.html').read()
        inj = '<script>window.__DATA__=' + json.dumps(data, default=str) + ';</script>'
        html = html.replace('<!--DATA-->', inj)
        open('standalone.html', 'w').write(html)
    print('ok fund', round(fund_val, 2), '| open', len(open_positions), '| closed', len(st['closed']))


if __name__ == '__main__':
    main()
