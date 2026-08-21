#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["pandas", "numpy", "requests"]
# ///
"""prop_paper.py - forward paper-trade of the majors challenge strategy.

Runs the EXACT backtested logic (majors 4h vote-flip, both directions, 3%
stop, TP+5R, 14-day timeout, 6 slots) live, and tracks four paper challenge
accounts in parallel:

    0.50% risk on BrightFunded (2-step 8%+5%, static 10% DD, 5% daily)
    0.75% risk on BrightFunded
    0.50% risk on Breakout      (1-step 10%,   static  6% DD, 3% daily)
    0.75% risk on Breakout

All four trade the SAME signals - they differ only in bet size and firm
rules - so this is four honest views of one strategy. Each account passes
(+target, advancing phases), blows (hits a drawdown floor), or keeps going.
Once an account passes or blows it FREEZES; that is the outcome we're
measuring.

Faithful to the backtest on purpose: operates only on CLOSED 4h bars, stops
and take-profits checked on the bar's high/low, so the forward result is
directly comparable to the numbers that justified running this at all. It is
paper - no keys, no orders, no exchange writes. State in prop_state.json,
human-readable status in prop_status.txt, rewritten every run.

Runs on the GitHub Actions job (hourly), acting only when a new 4h bar has
closed. Idempotent: re-running on the same bar does nothing. Results take
weeks - median pass was 31-54 days in backtest.
"""
import json
import os
import time
import urllib.request

import numpy as np
import pandas as pd

API = "https://api.hyperliquid.xyz/info"
COINS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "ADA", "LINK"]
# 4h clock, adopted 2026-08-21: on matched dates it caught each trend flip up
# to 8h earlier than 12h, lifting per-trade edge (+0.226R -> +0.265R) with no
# loss of pass rate. The EMA spans are the proven 12h spans x3, so the SMOOTHING
# HORIZON is unchanged - only the sampling grid is finer. Timeout x3 too, so the
# hold stays ~10 days.
INTERVAL = "4h"
BASE_D = [(6, 60), (9, 75), (9, 90), (12, 105), (15, 120), (21, 180)]
COST = 5.5e-4; SLIP = 5e-4
# HOLD_BARS = 84 (14 days x 6 4h-bars). The 2D exit sweep (21 Aug) found 14d
# beats the inherited 10d on a BROAD plateau: BrightFunded pass 82% vs 74%,
# identical per-trade edge (+0.284R), ~3 days slower, and positive in EVERY
# year incl 2026 (10d was -0.04R in 2026). A 5R target is a 15% move - majors
# often need >10 days to travel it, so 10d was closing winners early. TP_R=5
# confirmed optimal in the same sweep.
STOP = 0.03; TP_R = 5; HOLD_BARS = 84; SLOTS = 6
HERE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(HERE, "prop_state.json")     # full internal state
STATUS = os.path.join(HERE, "prop_status.txt")    # human-readable console/log
SITE = os.path.join(HERE, "prop.json")            # compact feed for the website

# Each account's real challenge shape. phases = the profit targets to clear in
# sequence; max_dd/daily = static drawdown floors measured from phase start.
FIRMS = {
    "BrightFunded": dict(phases=[0.08, 0.05], max_dd=0.10, daily=0.05),
    "Breakout":     dict(phases=[0.10],       max_dd=0.06, daily=0.03),
}
ACCOUNTS = [(f"{firm}-{int(r*10000)}bp", firm, r)
            for firm in FIRMS for r in (0.005, 0.0075)]


def post(body):
    for i in range(4):
        try:
            req = urllib.request.Request(API, json.dumps(body).encode(),
                                         {"Content-Type": "application/json"})
            return json.loads(urllib.request.urlopen(req, timeout=25).read())
        except Exception:
            time.sleep(1 + i)
    return None


def candles(coin, days=220):
    end = int(time.time() * 1000)
    r = post({"type": "candleSnapshot", "req": dict(
        coin=coin, interval=INTERVAL, startTime=end - days * 86400000, endTime=end)})
    if not r or len(r) < 130:
        return None
    df = pd.DataFrame([{"t": x["t"], "T": x["T"], "h": float(x["h"]),
                        "l": float(x["l"]), "c": float(x["c"])} for x in r])
    df.index = pd.to_datetime(df["t"], unit="ms", utc=True)
    df = df[~df.index.duplicated()].sort_index()
    if int(df["T"].iloc[-1]) > end:        # drop the still-forming bar
        df = df.iloc[:-1]
    return df


def votes(c):
    return sum((c.ewm(span=f).mean() > c.ewm(span=s).mean()).astype(float)
               for f, s in BASE_D) / len(BASE_D)


def fresh_account():
    return dict(equity=1.0, phase=0, phase_start=1.0, day_start=1.0,
                day="", status="active", opened=0, wins=0, losses=0)


def load():
    if os.path.exists(STATE):
        st = json.load(open(STATE))
        # if the timeframe changed, start clean rather than mixing old-clock
        # positions/stops into new-clock logic
        if st.get("interval") == INTERVAL:
            return st
    return dict(last_bar="", positions=[], closed=[], interval=INTERVAL,
                accounts={name: fresh_account() for name, _, _ in ACCOUNTS},
                started=None)


def save(st):
    tmp = STATE + ".tmp"
    json.dump(st, open(tmp, "w"), indent=1, default=str)
    os.replace(tmp, STATE)


def apply_R(acc, firm, r, risk, day_key):
    """Push one closed-trade R through one account; update phase/status."""
    if acc["status"] != "active":
        return
    if acc["day"] != day_key:
        acc["day"] = day_key; acc["day_start"] = acc["equity"]
    acc["equity"] *= (1 + risk * r)
    acc["wins" if r > 0 else "losses"] += 1
    rules = FIRMS[firm]
    # blow checks (static DD from phase start, plus daily)
    if acc["equity"] <= acc["phase_start"] * (1 - rules["max_dd"]) or \
       acc["equity"] <= acc["day_start"] * (1 - rules["daily"]):
        acc["status"] = "BLOWN"
        return
    # target check for the current phase
    target = acc["phase_start"] * (1 + rules["phases"][acc["phase"]])
    if acc["equity"] >= target:
        acc["phase"] += 1
        if acc["phase"] >= len(rules["phases"]):
            acc["status"] = "PASSED"
        else:
            acc["phase_start"] = acc["equity"]      # next phase, floor resets
            acc["day_start"] = acc["equity"]


def main():
    st = load()
    data = {c: candles(c) for c in COINS}
    data = {c: d for c, d in data.items() if d is not None}
    if not data:
        print("no candle data this run"); return

    # the just-closed bar we're processing (max last-timestamp across coins)
    bar = max(d.index[-1] for d in data.values())
    if str(bar) == st["last_bar"]:
        print(f"bar {bar} already processed - nothing to do"); return
    if st["started"] is None:
        st["started"] = str(bar)
    day_key = bar.strftime("%Y-%m-%d")

    # ---- 1. resolve open positions on this new bar -----------------------
    still_open = []
    for pos in st["positions"]:
        d = data.get(pos["coin"])
        if d is None or bar <= pd.Timestamp(pos["last_bar"]):
            still_open.append(pos); continue
        new = d[d.index > pd.Timestamp(pos["last_bar"])]
        closed_r = None; why = None
        for ts, row in new.iterrows():
            long = pos["dir"] == "L"
            stop_hit = (row["l"] <= pos["stop"]) if long else (row["h"] >= pos["stop"])
            tp_hit = (row["h"] >= pos["tp"]) if long else (row["l"] <= pos["tp"])
            pos["bars"] += 1
            if stop_hit:                     # stop assumed first on a both-touch bar
                xpx = pos["stop"] * (1 - SLIP if long else 1 + SLIP)
                why = "STOP"
            elif tp_hit:
                xpx = pos["tp"]; why = "TP"
            elif pos["bars"] >= HOLD_BARS:
                xpx = row["c"]; why = "TIMEOUT"
            else:
                continue
            mv = (xpx / pos["entry"] - 1) if long else (1 - xpx / pos["entry"])
            closed_r = (mv - 2 * COST) / STOP
            pos["last_bar"] = str(ts)
            break
        if closed_r is None:
            pos["last_bar"] = str(bar); still_open.append(pos)
        else:
            st["closed"].append(dict(coin=pos["coin"], dir=pos["dir"],
                                     R=round(closed_r, 3), why=why, closed=str(bar)))
            for name, firm, risk in ACCOUNTS:
                apply_R(st["accounts"][name], firm, closed_r, risk, day_key)
    st["positions"] = still_open

    # ---- 2. scan for fresh vote-flips, open new positions ----------------
    open_coins = {p["coin"] for p in st["positions"]}
    for coin, d in data.items():
        if len(st["positions"]) >= SLOTS:
            break
        if coin in open_coins:
            continue
        v = votes(d["c"])
        if len(v) < 3:
            continue
        vn, vp = float(v.iloc[-1]), float(v.iloc[-2])
        entry = float(d["c"].iloc[-1])
        direction = None
        if vn < 0.5 <= vp:
            direction = "S"
        elif vn >= 0.5 > vp:
            direction = "L"
        if direction:
            long = direction == "L"
            st["positions"].append(dict(
                coin=coin, dir=direction, entry=entry,
                stop=entry * (1 - STOP if long else 1 + STOP),
                tp=entry * (1 + TP_R * STOP if long else 1 - TP_R * STOP),
                opened=str(bar), last_bar=str(bar), bars=0))
            open_coins.add(coin)
            for name, _, _ in ACCOUNTS:
                if st["accounts"][name]["status"] == "active":
                    st["accounts"][name]["opened"] += 1

    st["last_bar"] = str(bar)
    save(st)
    write_status(st, bar)
    print(f"bar {bar} processed | open {len(st['positions'])} | "
          f"closed total {len(st['closed'])}")


def write_status(st, bar):
    lines = []
    lines.append("MAJORS CHALLENGE - forward paper trade")
    lines.append(f"started {st['started']}  |  latest bar {bar}  "
                 f"|  trades closed {len(st['closed'])}")
    lines.append("")
    lines.append(f"{'account':22} {'risk':>5} {'equity':>8} {'phase':>7} "
                 f"{'status':>7}  W/L")
    lines.append("-" * 62)
    for name, firm, risk in ACCOUNTS:
        a = st["accounts"][name]
        eq = a["equity"]
        phase = (f"{a['phase']+1}/{len(FIRMS[firm]['phases'])}"
                 if a["status"] == "active" else "-")
        lines.append(f"{name:22} {100*risk:4.2f}% {100*(eq-1):+7.2f}% "
                     f"{phase:>7} {a['status']:>7}  {a['wins']}/{a['losses']}")
    lines.append("")
    opn = st["positions"]
    lines.append(f"open positions ({len(opn)}/{SLOTS}):")
    for p in opn:
        lines.append(f"  {p['dir']} {p['coin']:5} @ {p['entry']:.4f}  "
                     f"stop {p['stop']:.4f}  tp {p['tp']:.4f}  bar {p['bars']}/{HOLD_BARS}")
    recent = st["closed"][-8:]
    if recent:
        lines.append("")
        lines.append("last closed:")
        for c in recent:
            lines.append(f"  {c['dir']} {c['coin']:5} {c['R']:+.2f}R  {c['why']}")
    txt = "\n".join(lines)
    open(STATUS, "w").write(txt + "\n")
    print("\n" + txt)

    # compact feed the website reads (prop.json)
    feed = dict(
        started=st["started"], last_bar=str(bar),
        closed_total=len(st["closed"]),
        accounts=[dict(
            name=name, firm=firm, risk=risk,
            equity=round(st["accounts"][name]["equity"], 5),
            pnl_pct=round(100 * (st["accounts"][name]["equity"] - 1), 2),
            phase=st["accounts"][name]["phase"],
            phases=len(FIRMS[firm]["phases"]),
            target=FIRMS[firm]["phases"], max_dd=FIRMS[firm]["max_dd"],
            status=st["accounts"][name]["status"],
            wins=st["accounts"][name]["wins"],
            losses=st["accounts"][name]["losses"])
            for name, firm, risk in ACCOUNTS],
        positions=[dict(coin=p["coin"], dir=p["dir"], entry=p["entry"],
                        stop=p["stop"], tp=p["tp"], bars=p["bars"])
                   for p in st["positions"]],
        recent=st["closed"][-12:])
    tmp = SITE + ".tmp"
    json.dump(feed, open(tmp, "w"), indent=1, default=str)
    os.replace(tmp, SITE)


if __name__ == "__main__":
    main()
