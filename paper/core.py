"""Pure rule logic for the 合約量化組 (futures quant paper group), rules v0.1.

No I/O here so the live loop (quant.py) and the backtest share exactly the
same signal, sizing and stop-management code. Paper only; never places orders.
"""
from __future__ import annotations

import math
from typing import Any

RULES_VERSION = "v0.2"
GROUP = "合約量化組"

START_EQUITY = 100.0
RISK_PCT = 0.02
TAKER_FEE = 0.0005
MAX_POSITIONS = 3
MAX_LEVERAGE = 3
MAX_TOTAL_NOTIONAL_X = 3.0
ATR_LEN = 14
ATR_MULT = 1.5
MAX_STOP_PCT = 0.10
BREAKOUT_LOOKBACK = 20
VOL_RATIO_MIN = 2.0
VOL_AVG_DAYS = 7
MIN_QUOTE_VOL_24H = 20_000_000.0
MIN_LISTED_DAYS = 30
BREAKEVEN_R = 2.0
EXPIRY_DAYS = 7
LIQ_MIN_MULT = 2.0
DEFAULT_MMR = 0.005
SIGNAL_MAX_AGE_S = 60 * 60  # do not enter on a bar closed more than 60 min ago
BTC_NEW_LOW_LOOKBACK = 20


def atr_wilder(bars: list[dict[str, float]], n: int = ATR_LEN) -> float | None:
    """Wilder ATR on closed bars (dicts with high/low/close)."""
    if len(bars) < n + 1:
        return None
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i]["high"], bars[i]["low"], bars[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[:n]) / n
    for tr in trs[n:]:
        atr = (atr * (n - 1) + tr) / n
    return atr


def breakout_signal(bars: list[dict[str, float]], tf: str, lookback: int | None = None,
                    vol_n: int | None = None) -> dict[str, Any] | None:
    """Check the LAST bar in `bars` (closed) for a 20-period breakout with volume.

    long : close > highest high of the previous 20 bars
    short: close < lowest low of the previous 20 bars
    volume ratio = signal bar quote volume / average quote volume of the
    previous 7 days of bars (7 bars on 1d, 42 bars on 4h).
    """
    if vol_n is None:
        vol_n = {"1d": VOL_AVG_DAYS, "4h": VOL_AVG_DAYS * 6, "1h": VOL_AVG_DAYS * 24, "15m": 96}.get(tf, VOL_AVG_DAYS * 6)
    lb = lookback or BREAKOUT_LOOKBACK
    need = max(lb, vol_n) + 1
    if len(bars) < need:
        return None
    sig = bars[-1]
    prev = bars[-1 - lb:-1]
    hh = max(b["high"] for b in prev)
    ll = min(b["low"] for b in prev)
    vprev = bars[-1 - vol_n:-1]
    vavg = sum(b["qvol"] for b in vprev) / len(vprev)
    if vavg <= 0:
        return None
    ratio = sig["qvol"] / vavg
    side = None
    level = None
    if sig["close"] > hh:
        side, level = "BUY", hh
    elif sig["close"] < ll:
        side, level = "SELL", ll
    if side is None:
        return None
    return {
        "side": side,
        "tf": tf,
        "level": level,
        "close": sig["close"],
        "vol_ratio": ratio,
        "vol_ok": ratio >= VOL_RATIO_MIN,
        "bar_close_ms": sig["close_ms"],
    }


def btc_regime(btc4h: list[dict[str, float]], level: float | None, mode: str = "spot_level") -> dict[str, Any]:
    """Direction filter. Same definition as the spot team's BTC filter.

    spot_level: last closed BTC 4h close >= level AND that close is not the
      lowest close of the last 20 closed 4h bars -> longs allowed (spot 'pass').
      close < level -> shorts only. close >= level but a 20-bar closing low ->
      neutral (no new trades either way).
    sma20 (backtest proxy, generic): close >= SMA20 of 4h closes -> longs;
      below -> shorts.
    """
    if not btc4h:
        return {"allow": None, "reason": "no BTC data"}
    last = btc4h[-1]["close"]
    closes = [b["close"] for b in btc4h[-BTC_NEW_LOW_LOOKBACK:]]
    new_low = len(closes) >= BTC_NEW_LOW_LOOKBACK and last <= min(closes)
    if mode == "sma20":
        sma = sum(closes) / len(closes)
        allow = "BUY" if last >= sma else "SELL"
        return {"allow": allow, "btc_close": last, "ref": sma, "mode": mode, "new_low": new_low,
                "reason": f"BTC 4h close {last:.2f} {'≥' if allow == 'BUY' else '<'} SMA20 {sma:.2f}"}
    if level is None:
        return {"allow": None, "reason": "no filter level"}
    if last < level:
        allow = "SELL"
        reason = f"BTC 4h close {last:.2f} < filter {level:.0f} → shorts only"
    elif new_low:
        allow = None
        reason = f"BTC 4h close {last:.2f} ≥ filter {level:.0f} but 20-bar closing low → no trades"
    else:
        allow = "BUY"
        reason = f"BTC 4h close {last:.2f} ≥ filter {level:.0f} → longs only"
    return {"allow": allow, "btc_close": last, "ref": level, "mode": mode, "new_low": new_low, "reason": reason}


def liq_price(entry: float, side: str, lev: int, mmr: float) -> float:
    """Approximate isolated-margin liquidation price (fees ignored)."""
    if side == "BUY":
        return entry * (1 - 1 / lev) / (1 - mmr)
    return entry * (1 + 1 / lev) / (1 + mmr)


def floor_step(x: float, step: float) -> float:
    if not step or step <= 0:
        return x
    return math.floor(x / step + 1e-9) * step


def size_position(
    *,
    wallet: float,
    entry: float,
    side: str,
    atr: float,
    open_notional: float,
    open_margin: float,
    mmr: float = DEFAULT_MMR,
    step: float = 0.0,
    min_notional: float = 0.0,
    risk_pct: float | None = None,
    max_positions: int | None = None,
    fee: float | None = None,
    atr_mult: float | None = None,
    max_stop_pct: float | None = None,
) -> dict[str, Any]:
    """Return a sizing dict; 'ok' False with 'skip' reason when rules say skip."""
    fee = TAKER_FEE if fee is None else fee
    r_dist = (ATR_MULT if atr_mult is None else atr_mult) * atr
    stop_pct = r_dist / entry
    stop = entry - r_dist if side == "BUY" else entry + r_dist
    out: dict[str, Any] = {"ok": False, "entry": entry, "stop": stop, "R": r_dist, "stop_pct": stop_pct}
    if stop <= 0:
        out["skip"] = "stop <= 0"
        return out
    msp = MAX_STOP_PCT if max_stop_pct is None else max_stop_pct
    if stop_pct > msp:
        out["skip"] = f"stop distance {stop_pct*100:.2f}% > {msp*100:.0f}%"
        return out
    risk_usdt = (RISK_PCT if risk_pct is None else risk_pct) * wallet
    per_unit = r_dist + fee * (entry + stop)
    qty = risk_usdt / per_unit
    slot_margin = wallet / (MAX_POSITIONS if max_positions is None else max_positions)
    notional = qty * entry
    lev = max(1, math.ceil(notional / slot_margin - 1e-9))
    capped = False
    if lev > MAX_LEVERAGE:
        lev = MAX_LEVERAGE
        notional = slot_margin * MAX_LEVERAGE
        qty = notional / entry
        capped = True
    room = MAX_TOTAL_NOTIONAL_X * wallet - open_notional
    if notional > room:
        if room <= 0:
            out["skip"] = "total notional cap 3×equity reached"
            return out
        notional = room
        qty = notional / entry
        capped = True
    qty = floor_step(qty, step)
    notional = qty * entry
    if qty <= 0 or notional < max(min_notional, 1e-9):
        out["skip"] = f"notional {notional:.2f} < exchange min {min_notional:g}"
        return out
    margin = notional / lev
    entry_fee = notional * fee
    if open_margin + margin + entry_fee > wallet:
        out["skip"] = "not enough free margin"
        return out
    liq = liq_price(entry, side, lev, mmr)
    liq_dist = abs(entry - liq)
    if liq_dist < LIQ_MIN_MULT * r_dist:
        out["skip"] = f"liq {liq:.6g} closer than 2× stop distance"
        return out
    est_loss = qty * r_dist + fee * qty * (entry + stop)
    out.update(
        ok=True, qty=qty, notional=notional, leverage=lev, margin=margin, liq=liq, mmr=mmr,
        entry_fee=entry_fee, risk_usdt=est_loss, risk_pct=est_loss / wallet, size_capped=capped,
    )
    return out


def step_bar(pos: dict[str, Any], high: float, low: float) -> float | None:
    """Advance one bar. Returns stop price if the stop was hit in this bar.

    Conservative ordering: check the stop in force at bar start against the
    adverse extreme first, then update best price and trailing stop.
    """
    side = pos["side"]
    stop = pos["stop"]
    if side == "BUY" and low <= stop:
        return stop
    if side == "SELL" and high >= stop:
        return stop
    entry, r = pos["entry_price"], pos["R"]
    if side == "BUY":
        pos["best"] = max(pos.get("best", entry), high)
        if pos["best"] - entry >= BREAKEVEN_R * r:
            new = max(entry, pos["best"] - r)
            if new > stop:
                pos["stop"] = new
                pos["trail_armed"] = True
    else:
        pos["best"] = min(pos.get("best", entry), low)
        if entry - pos["best"] >= BREAKEVEN_R * r:
            new = min(entry, pos["best"] + r)
            if new < stop:
                pos["stop"] = new
                pos["trail_armed"] = True
    return None


def worse_fill(side: str, stop: float, observed: float) -> float:
    """Stop exits fill at the worse of the stop and the price actually observed."""
    return min(stop, observed) if side == "BUY" else max(stop, observed)


def close_pnl(pos: dict[str, Any], exit_price: float) -> dict[str, float]:
    sign = 1 if pos["side"] == "BUY" else -1
    qty = pos["qty"]
    gross = sign * (exit_price - pos["entry_price"]) * qty
    exit_fee = (pos.get("fee_rate") or TAKER_FEE) * qty * exit_price
    liquidated = False
    margin = pos.get("margin") or 0.0
    funding = pos.get("funding", 0.0)
    if margin and gross + funding < -margin:
        gross = -margin - funding  # isolated: loss capped at margin
        liquidated = True
        exit_fee = 0.0
    fees = pos.get("entry_fee", 0.0) + exit_fee
    net = gross - fees + funding
    risk = pos.get("risk_usdt") or (qty * pos["R"])
    return {"gross": gross, "exit_fee": exit_fee, "fees": fees, "funding": funding, "net": net,
            "R_multiple": net / risk if risk else 0.0, "liquidated": liquidated}
