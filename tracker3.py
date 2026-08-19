"""Test Lab — hybrid short-term paper book ($100).
Engine 1: 8x fast-ignition longs on BTC+ETH, slow-BTC-bull gate, 1.2xATR24 stop, exit flip-down/7d. Risk 1%/trade.
Engine 2: alt bleed shorts at 2x clock, fresh 12h down-flip, $5M liq, capitulation veto, 3% stop, 10d timeout, 10 slots. Risk 0.5%/trade.
Paper only: no keys, no orders. Hourly via GitHub Actions. Emits data3.json for the dashboard Test Lab tab.
"""
import json, os, time
import requests
import pandas as pd
import numpy as np

API = 'https://api.hyperliquid.xyz/info'
COST = 5.5e-4; SLIP = 5e-4
BASE_H = [(48, 480), (72, 600), (72, 720), (96, 840), (120, 960), (168, 1440)]
FAST = [(max(2, f // 8), max(8, s // 8)) for f, s in BASE_H]
BASE_D = [(2, 20), (3, 25), (3, 30), (4, 35), (5, 40), (7, 60)]
E1_RISK = 0.01; E2_RISK = 0.005
E1_ASSETS = ['BTC', 'ETH']
E2_SLOTS = 10; E2_STOP = 0.03; E2_HOLD_HOURS = 240  # 20 bars x 12h = 10 days
START = 100.0

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
    df = pd.DataFrame([{'t': x['t'], 'T': x['T'], 'o': float(x['o']), 'h': float(x['h']),
                        'l': float(x['l']), 'c': float(x['c'])} for x in r])
    df.index = pd.to_datetime(df['t'], unit='ms', utc=True)
    df = df[~df.index.duplicated()].sort_index()
    return df.iloc[:-1] if len(df) > 1 and int(df['T'].iloc[-1]) > int(time.time() * 1000) else df
    
def votes(c, pairs):
    return sum((c.ewm(span=f).mean() > c.ewm(span=s).mean()).astype(float) for f, s in pairs) / len(pairs)

def main():
    now = pd.Timestamp.utcnow().floor('h')
    st = json.load(open('state3.json')) if os.path.exists('state3.json') else dict(
        t0=str(now), ts=None, equity=START, events=[], closed=[], alt_scan=None, hist=[])
    log = []

    def close_ev(ev, xpx, why, risk):
        rr_px = ((xpx / ev['entry'] - 1) if ev['side'] == 'L' else (1 - xpx / ev['entry']))
        rr = (rr_px - 2 * COST) / ev['stop_frac']
        fh = post({'type': 'fundingHistory', 'coin': ev['coin'],
                   'startTime': int(pd.Timestamp(ev['opened']).timestamp() * 1000)})
        fr = sum(float(x['fundingRate']) for x in fh) if fh else 0.0
        rr += (fr if ev['side'] == 'S' else -fr) / ev['stop_frac']
        st['equity'] *= (1 + risk * rr)
        ev.update(open=False, closed=str(now), exit=xpx, R=round(rr, 3), why=why)
        st['closed'].append(ev)
        log.append(dict(ts=str(now), engine=ev['engine'], coin=ev['coin'], action='close',
                        note=f"{why} {rr:+.2f}R"))

    # ---------- ENGINE 1 ----------
    H = {}
    for a in E1_ASSETS:
        d = candles(a, '1h', 4400)
        if d is not None and len(d) > 1500:
            H[a] = d
    arm_gate = None
    if 'BTC' in H:
        cb = H['BTC']['c']
        gg = [round((float(cb.ewm(span=s).mean().iloc[-1]) / float(cb.ewm(span=f).mean().iloc[-1]) - 1) * 100, 3)
              for f, s in BASE_H]
        arm_gate = dict(votes=len([g for g in gg if g < 0]), need=3, gaps=gg)
    gate = bool(arm_gate and arm_gate['votes'] >= 3)
    arm_e1 = {}
    for a in [x for x in E1_ASSETS if x in H]:
        d = H[a]; c = d['c']
        vf = votes(c, FAST)
        arm_e1[a] = dict(fast=int(round(float(vf.iloc[-1]) * 6)), hot=bool(vf.iloc[-1] >= 0.5))
        for ev in [e for e in st['events'] if e['engine'] == 1 and e['coin'] == a and e['open']]:
            seg = d[d.index > pd.Timestamp(ev['opened'])]
            stopped = bool((seg['l'] <= ev['stop']).any()) if len(seg) else False
            flipped = bool(vf.iloc[-1] < 0.5)
            expired = (now - pd.Timestamp(ev['opened'])) >= pd.Timedelta(days=7)
            if stopped or flipped or expired:
                close_ev(ev, ev['stop'] * (1 - SLIP) if stopped else float(c.iloc[-1]),
                         'STOP' if stopped else ('FLIP' if flipped else 'TIMEOUT'), E1_RISK)
            else:
                ev['cur'] = float(c.iloc[-1])
        if gate and len(vf) > 2 and vf.iloc[-1] >= 0.5 and vf.iloc[-2] < 0.5 \
           and not any(e['engine'] == 1 and e['coin'] == a and e['open'] for e in st['events']):
            tr = np.maximum(d['h'] - d['l'], np.maximum((d['h'] - d['c'].shift(1)).abs(),
                                                        (d['l'] - d['c'].shift(1)).abs()))
            atr24 = float(tr.rolling(24).mean().iloc[-1])
            e = float(c.iloc[-1]); sf = max(1.2 * atr24 / e, 0.005)
            st['events'].append(dict(engine=1, coin=a, side='L', entry=e, stop=e * (1 - sf),
                                     stop_frac=sf, opened=str(now), open=True, cur=e))
            log.append(dict(ts=str(now), engine=1, coin=a, action='open',
                            note=f"LONG @{e:.2f} stop {sf*100:.2f}%"))

    # ---------- ENGINE 2 ----------
    for ev in [e for e in st['events'] if e['engine'] == 2 and e['open']]:
        d = candles(ev['coin'], '1h', 26)
        if d is None or not len(d):
            continue
        seg = d[d.index > pd.Timestamp(ev.get('ts_check', ev['opened']))]
        stopped = bool((seg['h'] >= ev['stop']).any()) if len(seg) else False
        expired = (now - pd.Timestamp(ev['opened'])) >= pd.Timedelta(hours=E2_HOLD_HOURS)
        if stopped or expired:
            close_ev(ev, ev['stop'] * (1 + SLIP) if stopped else float(d['c'].iloc[-1]),
                     'STOP' if stopped else 'TIMEOUT', E2_RISK)
        else:
            ev['ts_check'] = str(now); ev['cur'] = float(d['c'].iloc[-1])
    if str(now.floor('12h')) != st.get('alt_scan_bar'):
        st['alt_scan_bar'] = str(now.floor('12h'))
        st['alt_scan'] = str(now)
        watch = []
        mu = post({'type': 'metaAndAssetCtxs'})
        if mu:
            uni = [(u['name'], float(mu[1][i].get('dayNtlVlm', 0) or 0))
                   for i, u in enumerate(mu[0]['universe']) if not u.get('isDelisted')]
            alts = [n for n, v in uni if n not in ('BTC', 'ETH', 'SOL') and v > 5e6]
            for coin in alts:
                if len([e for e in st['events'] if e['engine'] == 2 and e['open']]) >= E2_SLOTS:
                    break
                if any(e['engine'] == 2 and e['coin'] == coin and e['open'] for e in st['events']):
                    continue
                dd = candles(coin, '12h', 24 * 110)   # ~220 x 12h bars
                if dd is None or len(dd) < 130:
                    continue
                cl = dd['c']; v = votes(cl, BASE_D)
                veto = bool(((cl / cl.shift(10) - 1).iloc[-7:] < -0.25).any())
                v_now = float(v.iloc[-1])
                if v_now < 0.5 and v.iloc[-2] >= 0.5 and not veto:
                    e = float(cl.iloc[-1])
                    st['events'].append(dict(engine=2, coin=coin, side='S', entry=e,
                                             stop=e * (1 + E2_STOP), stop_frac=E2_STOP,
                                             opened=str(now), open=True, cur=e))
                    log.append(dict(ts=str(now), engine=2, coin=coin, action='open',
                                    note=f"SHORT @{e:.4f} stop +3% (12h clock)"))
                elif 0.5 <= v_now < 0.75:
                    dg = [(float(cl.ewm(span=f).mean().iloc[-1]) / float(cl.ewm(span=s).mean().iloc[-1]) - 1) * 100
                          for f, s in BASE_D]
                    up = [g for g in dg if g > 0]
                    watch.append(dict(coin=coin, bulls=int(round(v_now * 6)),
                                      gap=round(min(up), 2) if up else None, veto=veto))
            st['watch'] = sorted(watch, key=lambda w: (w['bulls'], w['gap'] if w['gap'] is not None else 99.0))[:8]

    # ---------- mark open to market & emit ----------
    unreal = 0.0
    open_pos = []
    for e in [x for x in st['events'] if x['open']]:
        cp = e.get('cur')
        ur = None
        if cp:
            ur = (((cp / e['entry'] - 1) if e['side'] == 'L' else (1 - cp / e['entry'])) - COST) / e['stop_frac']
            unreal += (E1_RISK if e['engine'] == 1 else E2_RISK) * ur * st['equity']
        open_pos.append(dict(engine=e['engine'], coin=e['coin'], side=e['side'],
                             entry=e['entry'], stop=e['stop'], cur=cp,
                             unreal_R=round(ur, 2) if ur is not None else None,
                             since=e['opened'][:16]))
    eq_live = st['equity'] + unreal
    st.setdefault('trade_log', [])
    st['trade_log'] = (st.get('trade_log', []) + log)[-300:]
    st['hist'] = (st.get('hist', []) + [[str(now), round(eq_live, 4)]])[-5000:]
    st['ts'] = str(now)
    json.dump(st, open('state3.json', 'w'), indent=1, default=str)

    def eng_stats(n):
        cl = [e for e in st['closed'] if e['engine'] == n]
        rs = [e['R'] for e in cl]
        return dict(closed=len(cl), open=len([e for e in open_pos if e['engine'] == n]),
                    win_pct=round(100 * len([r for r in rs if r > 0]) / max(1, len(rs)), 1),
                    avg_R=round(float(np.mean(rs)), 3) if rs else None,
                    tot_R=round(float(np.sum(rs)), 2) if rs else 0.0,
                    best=round(max(rs), 2) if rs else None, worst=round(min(rs), 2) if rs else None)
    data = dict(
        name='Test Lab — Hybrid Book', updated=str(now), since=st['t0'], start=START,
        equity=round(eq_live, 4), realized=round(st['equity'], 4),
        pnl=round(eq_live - START, 4), pnl_pct=round(100 * (eq_live / START - 1), 3),
        gate_bull=gate, series=st['hist'],
        arming=dict(gate=arm_gate, e1=arm_e1,
                    e2=dict(slots=E2_SLOTS,
                            used=len([e for e in st['events'] if e['engine'] == 2 and e['open']]),
                            last_scan=st.get('alt_scan'), watchlist=st.get('watch', []))),
        e1=eng_stats(1), e2=eng_stats(2),
        open_positions=open_pos,
        closed=[dict(engine=e['engine'], coin=e['coin'], side=e['side'], R=e['R'],
                     why=e.get('why'), opened=e['opened'][:16], closed=e['closed'][:16])
                for e in st['closed'][-60:]],
        trade_log=st['trade_log'][-60:],
    )
    json.dump(data, open('data3.json', 'w'), indent=1, default=str)
    print('lab ok', round(eq_live, 2), '| open', len(open_pos), '| closed', len(st['closed']))

if __name__ == '__main__':
    main()
