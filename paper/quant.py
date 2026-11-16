#!/usr/bin/env python3
"""合約量化組 live PAPER loop (rules v0.1). No AI, no API keys, no orders.

Reads only public Binance USDT-M futures market data, simulates isolated-margin
paper positions, writes the ledger under /workspace/trading-journal/futures/,
and drops open/close events into /workspace/futures/outbox.jsonl, which
gate.py forwards to the wake webhook with its normal throttle.

  python3 quant.py            # loop (started by watcher/watchdog.sh)
  python3 quant.py --once     # one cycle, writes ledger
  python3 quant.py --dry-run  # one cycle, prints decisions, writes nothing
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
import core  # noqa: E402
import paper_paths as paths  # noqa: E402

TZ = ZoneInfo("Asia/Kuala_Lumpur")
BASES = ("https://www.binance.com", "https://fapi.binance.com")  # fapi.* returns 451 from the box and from US runners
HERE = Path(__file__).resolve().parent
LEDGER = Path("/workspace/trading-journal/futures")
POSITIONS = LEDGER / "positions.json"
TRADES = LEDGER / "trades.csv"
EQUITY = LEDGER / "equity.csv"
STATE = HERE / "state.json"
STATUS = HERE / "status.json"
HEARTBEAT = HERE / "heartbeat.json"
OUTBOX = HERE / "outbox.jsonl"
DECISIONS = HERE / "decisions.jsonl"
LOG = HERE / "quant.log"
PID = HERE / "quant.python.pid"
PENDING_PLANS = Path("/workspace/trading-journal/pending_plans.json")
MAX_REPLAY_BARS = {"4h": 6, "1h": 12, "15m": 32}
BTC_FILTER_DEFAULT = 67_568.0
LOOP_S = 60
STABLES = {"USDC", "FDUSD", "TUSD", "USDP", "DAI", "BUSD", "USDE", "USD1", "RLUSD", "PYUSD", "EUR", "XUSD"}
SCAN_GRACE_S = 30
EQUITY_EVERY_S = 3600
H4_MS = 4 * 3600 * 1000
TF_MS = {"4h": H4_MS, "1h": 3600_000, "15m": 900_000}

# ---- rule-set profiles (v0.2, 2026-11-16). All PAPER. ----
PROFILES = {
    "4h": dict(name="4h", group="合約量化組", rules="v0.2", tf="4h", ledger="/workspace/trading-journal/futures", run=str(Path(__file__).resolve().parent),
               risk=0.02, atr_mult=1.5, max_pos=3, max_same_dir=1, expiry_days=7, slip=0.0, daily=True, regime="spot_level", kl_limit=62,
               top_n=None, emit=("futures_open", "futures_close", "futures_refill"), id_prefix="FQ", lookback=20),
    "1h": dict(name="1h", group="合約1h組", rules="v0.2-1h", tf="1h", ledger="/workspace/trading-journal/futures_1h", run="/workspace/futures/run_1h",
               risk=0.01, atr_mult=2.0, max_pos=5, max_same_dir=2, expiry_days=2, slip=0.0002, daily=False, regime="spot_level", kl_limit=200,
               top_n=100, emit=("futures_close", "futures_refill"), id_prefix="F1H", lookback=20),
    "15m": dict(name="15m", group="合約高頻組", rules="hf-15m-v0.1", tf="15m", ledger="/workspace/trading-journal/futures_hf15m", run="/workspace/futures/run_hf15m",
                risk=0.005, atr_mult=1.5, max_pos=12, max_same_dir=6, expiry_days=1, slip=0.0002, daily=False, regime="none", kl_limit=120,
                top_n=60, emit=("futures_refill",), id_prefix="FHF", lookback=20),
}
P: dict = PROFILES["4h"]
REFILL_BELOW = 10.0

TRADE_COLS = [
    "mode", "group", "rules", "trade_id", "symbol", "side", "signal_tf", "signal_bar_close_myt", "vol_ratio",
    "entry_time_myt", "entry_price", "qty", "notional_usdt", "leverage", "margin_usdt", "liq_price", "mmr",
    "initial_stop", "R_dist", "risk_usdt", "exit_time_myt", "exit_price", "exit_reason", "gross_pnl",
    "fees", "funding", "net_pnl", "R_multiple", "equity_after", "price_source", "notes",
]
EQ_COLS = ["mode", "time_myt", "wallet_usdt", "unrealized_usdt", "equity_usdt", "open_positions", "realized_total", "note"]

log = logging.getLogger("quant")


def configure(name: str) -> None:
    """Point every module-level path/param at the chosen profile."""
    global P, LEDGER, POSITIONS, TRADES, EQUITY, STATE, STATUS, HEARTBEAT, DECISIONS, LOG, PID, REFILLS
    global HERE, OUTBOX, PENDING_PLANS
    P = dict(PROFILES[name])
    # Prefer env-based roots so the same code runs on the box and on GitHub Actions.
    J, F = paths.journal(), paths.futures_dir()
    P["ledger"] = str({"4h": J / "futures", "1h": J / "futures_1h", "15m": J / "futures_hf15m"}[name])
    P["run"] = str({"4h": F, "1h": F / "run_1h", "15m": F / "run_hf15m"}[name])
    LEDGER = Path(P["ledger"])
    run = Path(P["run"])
    run.mkdir(parents=True, exist_ok=True)
    LEDGER.mkdir(parents=True, exist_ok=True)
    HERE = F
    POSITIONS, TRADES, EQUITY = LEDGER / "positions.json", LEDGER / "trades.csv", LEDGER / "equity.csv"
    REFILLS = LEDGER / "refills.jsonl"
    STATE, STATUS, HEARTBEAT = run / "state.json", run / "status.json", run / "heartbeat.json"
    DECISIONS, LOG, PID = run / "decisions.jsonl", run / "quant.log", run / "quant.python.pid"
    OUTBOX = F / "outbox.jsonl"
    PENDING_PLANS = J / "pending_plans.json"


def update_lifetime() -> None:
    try:
        sys.path.insert(0, str(paths.journal()))
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import lifetime  # noqa
        lifetime.rebuild()
    except Exception as e:  # never break trading on stats
        log.warning("lifetime stats update failed: %s", e)


def now() -> datetime:
    return datetime.now(TZ)


def fmt(dt: datetime | None = None) -> str:
    return (dt or now()).astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S MYT")


def ms_fmt(ms: int) -> str:
    return fmt(datetime.fromtimestamp(ms / 1000, tz=timezone.utc))


def parse_myt(s: str) -> datetime:
    return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)


def jload(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def jsave(path: Path, obj) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


class Api:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers["User-Agent"] = "paper-futures-quant"
        self.base = BASES[0]
        self._cache: dict[str, tuple[float, Any]] = {}

    def get(self, path: str, params: dict | None = None, ttl: float = 0):
        key = path + json.dumps(params or {}, sort_keys=True)
        c = self._cache.get(key)
        if ttl and c and time.time() - c[0] < ttl:
            return c[1]
        last = None
        for base in (self.base,) + tuple(b for b in BASES if b != self.base):
            for i in range(3):
                try:
                    r = self.s.get(base + path, params=params, timeout=(5, 15))
                    if r.status_code == 200:
                        data = r.json()
                        self.base = base
                        if ttl:
                            self._cache[key] = (time.time(), data)
                        return data
                    last = f"HTTP {r.status_code}"
                    if r.status_code in (451, 403, 400):
                        break
                except requests.RequestException as e:
                    last = type(e).__name__
                time.sleep(0.5 * (i + 1))
        raise RuntimeError(f"GET {path} failed: {last}")

    def klines(self, sym, iv, limit=None, start=None):
        p = {"symbol": sym, "interval": iv}
        if limit:
            p["limit"] = limit
        if start:
            p["startTime"] = start
        raw = self.get("/fapi/v1/klines", p)
        return [{"open_ms": int(r[0]), "close_ms": int(r[6]) + 1, "open": float(r[1]), "high": float(r[2]),
                 "low": float(r[3]), "close": float(r[4]), "qvol": float(r[7])} for r in raw]

    def closed_klines(self, sym, iv, limit):
        now_ms = int(time.time() * 1000)
        return [b for b in self.klines(sym, iv, limit=limit) if b["close_ms"] <= now_ms]


def decision(kind: str, dry: bool, **kw) -> None:
    rec = {"time": fmt(), "kind": kind, **kw}
    log.info("decision %s %s", kind, json.dumps(kw, ensure_ascii=False, default=str)[:400])
    if not dry:
        with DECISIONS.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")


def emit(dry: bool, typ: str, symbol: str, details: dict) -> None:
    rec = {"id": f"fq_{now().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:6]}", "time": fmt(), "type": typ,
           "symbol": symbol, "group": P["group"], "details": {"group": P["group"], "rules": P["rules"], **details}}
    if typ not in P["emit"]:
        return  # this set reports in the 4h summary instead of waking the event routine
    if dry:
        log.info("dry-run: would emit %s %s", typ, symbol)
        return
    with OUTBOX.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def missed_signal_bars(now_ms: int, last_scan_bar_ms: int, tf: str) -> list[int]:
    """Closed signal bars after last_scan, in time order, no lookahead.

    If last_scan_bar_ms is 0, only the latest eligible bar is returned so a
    first run or --force-scan does not replay a long history.
    """
    tf_ms = TF_MS[tf]
    latest = now_ms // tf_ms * tf_ms
    if now_ms - latest < SCAN_GRACE_S * 1000:
        latest -= tf_ms
    if latest <= 0:
        return []
    if last_scan_bar_ms <= 0:
        return [latest]
    start = last_scan_bar_ms + tf_ms
    cap = MAX_REPLAY_BARS.get(tf, 8)
    start = max(start, latest - tf_ms * (cap - 1))
    if start > latest:
        return []
    return list(range(start, latest + 1, tf_ms))


def btc_filter_level() -> float:
    """Spot team's BTC filter level, read from the newest pending plans."""
    doc = jload(PENDING_PLANS, {}) or {}
    levels = []
    for p in doc.get("plans") or []:
        txt = str(p.get("btc_filter") or "")
        for m in re.findall(r"\d{1,3}(?:,\d{3})+|\d{5,6}", txt):
            v = float(m.replace(",", ""))
            if 10_000 <= v <= 1_000_000:
                levels.append(v)
    return max(levels) if levels else BTC_FILTER_DEFAULT


def spot_btc_4h() -> list[dict]:
    """BTC filter uses SPOT BTCUSDT 4h closes, same source as the spot team's gate."""
    try:
        r = requests.get("https://data-api.binance.vision/api/v3/klines",
                         params={"symbol": "BTCUSDT", "interval": "4h", "limit": 40}, timeout=(5, 15))
        r.raise_for_status()
        now_ms = int(time.time() * 1000)
        return [{"open_ms": int(x[0]), "close_ms": int(x[6]) + 1, "open": float(x[1]), "high": float(x[2]),
                 "low": float(x[3]), "close": float(x[4]), "qvol": float(x[7])} for x in r.json() if int(x[6]) < now_ms]
    except Exception as e:
        log.warning("spot BTC 4h fetch failed (%s); using futures BTC", type(e).__name__)
        return []


def ensure_ledger() -> None:
    LEDGER.mkdir(parents=True, exist_ok=True)
    if not POSITIONS.exists():
        jsave(POSITIONS, {"group": P["group"], "mode": "paper", "market": "Binance USDT-M perpetual (paper)",
                          "rules": P["rules"], "cycle": 1, "start_equity_usdt": core.START_EQUITY,
                          "wallet_usdt": core.START_EQUITY, "created": fmt(), "positions": []})
    if not TRADES.exists():
        with TRADES.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(TRADE_COLS)
    if not EQUITY.exists():
        with EQUITY.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(EQ_COLS)
            w.writerow(["paper-futures", fmt(), "100.00", "0.00", "100.00", 0, "0.00", f"{P['group']}起始基準（模擬）"])


class Quant:
    def __init__(self, dry: bool = False):
        self.dry = dry
        self.api = Api()
        self.state = jload(STATE, {}) or {}
        self.stop = False

    # ---------- market meta ----------
    def meta(self) -> dict[str, dict]:
        info = self.api.get("/fapi/v1/exchangeInfo", ttl=6 * 3600)
        out = {}
        for s in info["symbols"]:
            if s.get("contractType") != "PERPETUAL" or s.get("quoteAsset") != "USDT" or s.get("status") != "TRADING":
                continue
            if s.get("underlyingType") not in (None, "COIN") or s.get("baseAsset") in STABLES:
                continue  # skip index / pre-market / stablecoin contracts
            step, min_notional = 0.0, 5.0
            for f in s.get("filters", []):
                if f["filterType"] == "MARKET_LOT_SIZE":
                    step = max(step, float(f["stepSize"]))
                elif f["filterType"] == "LOT_SIZE" and not step:
                    step = float(f["stepSize"])
                elif f["filterType"] == "MIN_NOTIONAL":
                    min_notional = float(f.get("notional") or 5)
            out[s["symbol"]] = {"onboard": int(s.get("onboardDate") or 0), "step": step, "min_notional": min_notional}
        return out

    def mmr(self, symbol: str, notional: float) -> float:
        try:
            doc = self.api.get("/bapi/futures/v1/friendly/future/common/brackets", ttl=24 * 3600)
            for b in doc["data"]["brackets"]:
                if b["symbol"] == symbol:
                    for t in b["riskBrackets"]:
                        if t["bracketNotionalFloor"] <= notional < t["bracketNotionalCap"]:
                            return float(t["bracketMaintenanceMarginRate"])
        except Exception as e:
            log.warning("bracket lookup failed: %s", type(e).__name__)
        return core.DEFAULT_MMR

    # ---------- ledger ----------
    def doc(self) -> dict:
        return jload(POSITIONS, {}) or {}

    def save_doc(self, d: dict) -> None:
        if not self.dry:
            jsave(POSITIONS, d)

    # ---------- position management ----------
    def manage(self, d: dict, prices: dict[str, float], marks: dict[str, float]) -> list[str]:
        closed = []
        for pos in list(d["positions"]):
            sym = pos["symbol"]
            price = prices.get(sym)
            if price is None:
                continue
            last_ms = int(pos.get("last_check_ms") or pos["entry_ms"])
            now_ms = int(time.time() * 1000)
            # funding since last check
            try:
                fr = self.api.get("/fapi/v1/fundingRate", {"symbol": sym, "startTime": int(pos.get("last_funding_ms") or pos["entry_ms"]) + 1, "limit": 100})
                sign = 1 if pos["side"] == "BUY" else -1
                for x in fr:
                    ft = int(x["fundingTime"])
                    if ft <= pos["entry_ms"]:
                        continue
                    mark = float(x.get("markPrice") or 0) or marks.get(sym) or price
                    amt = -sign * pos["qty"] * mark * float(x["fundingRate"])
                    pos["funding"] = round(pos.get("funding", 0.0) + amt, 8)
                    pos["last_funding_ms"] = ft
                    decision("funding", self.dry, symbol=sym, rate=x["fundingRate"], amount=round(amt, 6), at=ms_fmt(ft))
            except Exception as e:
                log.warning("funding fetch %s failed: %s", sym, e)
            # bars since last check (1m; 5m if the gap is long, e.g. after box sleep)
            gap_min = (now_ms - last_ms) / 60000
            iv = "1m" if gap_min <= 900 else ("5m" if gap_min <= 7000 else "15m")
            try:
                bars = [b for b in self.api.klines(sym, iv, start=last_ms - 60000, limit=1500) if b["close_ms"] > last_ms]
            except Exception as e:
                log.warning("klines %s failed: %s", sym, e)
                bars = []
            hit = None
            for b in bars:
                hit = core.step_bar(pos, b["high"], b["low"])
                if hit is not None:
                    break
            if hit is None:
                hit = core.step_bar(pos, price, price)
            pos["last_check_ms"] = now_ms
            pos["last_price"] = price
            pos["high_since_entry"] = max([pos.get("high_since_entry", pos["entry_price"]), price] + [b["high"] for b in bars])
            pos["low_since_entry"] = min([pos.get("low_since_entry", pos["entry_price"]), price] + [b["low"] for b in bars])
            reason = None
            fill = None
            if hit is not None:
                # If we walked historical bars, fill at the first-touch stop, not a later observed price.
                fill = hit if bars else core.worse_fill(pos["side"], hit, price)
                reason = "trail_stop" if pos.get("trail_armed") else "stop"
                if pos.get("trail_armed") and abs(hit - pos["entry_price"]) < 1e-12:
                    reason = "breakeven_stop"
            elif now() >= parse_myt(pos["exit_deadline"]):
                fill, reason = price, "expiry_7d"
            if reason:
                self.close(d, pos, fill, reason, price)
                closed.append(sym)
            elif pos.get("trail_armed") and pos["stop"] != pos.get("_last_logged_stop"):
                decision("trail_update", self.dry, symbol=sym, stop=pos["stop"], best=pos.get("best"))
                pos["_last_logged_stop"] = pos["stop"]
        return closed

    def close(self, d: dict, pos: dict, fill: float, reason: str, observed: float) -> None:
        slip = float(pos.get("slippage") or 0.0)
        if slip:
            fill = fill * (1 - slip) if pos["side"] == "BUY" else fill * (1 + slip)
        p = core.close_pnl(pos, fill)
        if p["liquidated"]:
            reason = "liquidated"
        d["wallet_usdt"] = round(float(d.get("wallet_usdt", core.START_EQUITY)) + p["net"], 6)
        d["positions"].remove(pos)
        row = {
            "mode": "paper", "group": P["group"], "rules": P["rules"], "trade_id": pos["id"], "symbol": pos["symbol"],
            "side": pos["side"], "signal_tf": pos["signal_tf"], "signal_bar_close_myt": pos["signal_bar_close_myt"],
            "vol_ratio": pos["vol_ratio"], "entry_time_myt": pos["entry_time"], "entry_price": pos["entry_price"],
            "qty": pos["qty"], "notional_usdt": round(pos["notional"], 4), "leverage": pos["leverage"],
            "margin_usdt": round(pos["margin"], 4), "liq_price": pos["liq_price"], "mmr": pos["mmr"],
            "initial_stop": pos["initial_stop"], "R_dist": pos["R"], "risk_usdt": round(pos["risk_usdt"], 4),
            "exit_time_myt": fmt(), "exit_price": fill, "exit_reason": reason, "gross_pnl": round(p["gross"], 6),
            "fees": round(p["fees"], 6), "funding": round(p["funding"], 6), "net_pnl": round(p["net"], 6),
            "R_multiple": round(p["R_multiple"], 3), "equity_after": round(d["wallet_usdt"], 4),
            "price_source": f"Binance USDT-M public ({self.api.base.replace('https://', '')}) last {observed} @ {fmt()}",
            "notes": f"stop {pos['stop']} observed {observed}; stop exits fill at worse of stop/observed",
        }
        decision("close", self.dry, symbol=pos["symbol"], reason=reason, fill=fill, net=round(p["net"], 4), R=round(p["R_multiple"], 2))
        if not self.dry:
            with TRADES.open("a", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, TRADE_COLS).writerow(row)
            self.write_equity(d, f"平倉 {pos['symbol']} {reason}")
            self.save_doc(d)
            update_lifetime()
        emit(self.dry, "futures_close", pos["symbol"], {
            "side": pos["side"], "reason": reason, "entry": pos["entry_price"], "exit": fill,
            "net_pnl": round(p["net"], 4), "R": round(p["R_multiple"], 2), "wallet": round(d["wallet_usdt"], 2)})

    # ---------- signals ----------
    def scan(self, d: dict, prices: dict[str, float], bar_close_ms: int) -> None:
        meta = self.meta()
        t24 = {t["symbol"]: float(t["quoteVolume"]) for t in self.api.get("/fapi/v1/ticker/24hr")}
        level = btc_filter_level()
        btc4 = spot_btc_4h() or self.api.closed_klines("BTCUSDT", "4h", 40)
        reg = core.btc_regime(btc4, level, "spot_level")
        if P["regime"] == "none":
            reg = {**reg, "allow": "ANY", "reason": "both directions allowed (hf set; BTC filter only reported)"}
        decision("regime", self.dry, **{k: reg.get(k) for k in ("allow", "btc_close", "ref", "new_low", "reason")})
        self.state["last_regime"] = reg
        age_s = time.time() - bar_close_ms / 1000
        daily = datetime.fromtimestamp(bar_close_ms / 1000, tz=timezone.utc).hour == 0
        cands, seen = [], []
        now_ms = int(time.time() * 1000)
        universe = [s for s in meta if t24.get(s, 0) >= core.MIN_QUOTE_VOL_24H]
        if P["top_n"]:
            universe = set(sorted(universe, key=lambda s: -t24.get(s, 0))[: P["top_n"]])
        for sym, m in meta.items():
            if sym not in universe:
                continue
            if t24.get(sym, 0) < core.MIN_QUOTE_VOL_24H:
                continue
            if m["onboard"] and now_ms - m["onboard"] < core.MIN_LISTED_DAYS * 86400_000:
                continue
            try:
                h4 = [b for b in self.api.closed_klines(sym, P["tf"], P["kl_limit"]) if b["close_ms"] <= bar_close_ms]
            except Exception as e:
                log.warning("%s klines %s: %s", P["tf"], sym, e)
                continue
            if not h4 or h4[-1]["close_ms"] != bar_close_ms:
                continue
            sigs = [core.breakout_signal(h4, P["tf"], P["lookback"])]
            if daily and P["daily"]:
                try:
                    d1 = [b for b in self.api.closed_klines(sym, "1d", 30) if b["close_ms"] <= bar_close_ms]
                    sigs.append(core.breakout_signal(d1, "1d"))
                except Exception:
                    pass
            atr = core.atr_wilder(h4[-62:])
            for s in sigs:
                if not s:
                    continue
                row = {"symbol": sym, **s, "atr": atr}
                seen.append(row)
                if s["vol_ok"] and atr:
                    cands.append(row)
        self.state["last_scan"] = {"time": fmt(), "bar_close": ms_fmt(bar_close_ms), "universe": sum(1 for s in meta if t24.get(s, 0) >= core.MIN_QUOTE_VOL_24H),
                                   "breakouts": [{k: (round(v, 4) if isinstance(v, float) else v) for k, v in r.items() if k in ("symbol", "side", "tf", "vol_ratio", "close", "level")} for r in seen][:30],
                                   "regime": reg.get("allow")}
        decision("scan", self.dry, bar_close=ms_fmt(bar_close_ms), breakouts=len(seen), vol_ok=len(cands), allow=reg.get("allow"))
        # Missed bars are replayed in order by cycle(); do not drop a bar for age.
        best: dict[str, dict] = {}
        for c in sorted(cands, key=lambda c: -c["vol_ratio"]):
            best.setdefault(c["symbol"], c)
        for c in best.values():
            sym = c["symbol"]
            base = {"symbol": sym, "side": c["side"], "tf": c["tf"], "vol_ratio": round(c["vol_ratio"], 2)}
            if reg.get("allow") not in (c["side"], "ANY"):
                decision("skip", self.dry, **base, reason=f"direction filter: {reg.get('reason')}")
                continue
            if any(p["symbol"] == sym for p in d["positions"]):
                decision("skip", self.dry, **base, reason="already holding")
                continue
            if len(d["positions"]) >= P["max_pos"]:
                decision("skip", self.dry, **base, reason=f"max {P['max_pos']} positions")
                continue
            if sum(1 for p in d["positions"] if p["side"] == c["side"]) >= P["max_same_dir"]:
                decision("skip", self.dry, **base, reason=f"same-direction cap {P['max_same_dir']} (v0.2 correlation cap)")
                continue
            # Enter at the signal bar close (no lookahead on a later live tick).
            price = c.get("close") or prices.get(sym)
            if not price:
                continue
            wallet = float(d.get("wallet_usdt", core.START_EQUITY))
            price = price * (1 + P["slip"]) if c["side"] == "BUY" else price * (1 - P["slip"])  # adverse slippage
            est_notional = P["risk"] * wallet / (P["atr_mult"] * c["atr"] / price) if c["atr"] else 0
            sz = core.size_position(
                wallet=wallet, entry=price, side=c["side"], atr=c["atr"],
                open_notional=sum(p["notional"] for p in d["positions"]), open_margin=sum(p["margin"] for p in d["positions"]),
                mmr=self.mmr(sym, est_notional), step=meta[sym]["step"], min_notional=meta[sym]["min_notional"],
                risk_pct=P["risk"], max_positions=P["max_pos"], atr_mult=P["atr_mult"])
            if not sz["ok"]:
                decision("skip", self.dry, **base, reason=sz["skip"])
                continue
            self.open(d, c, sz, bar_close_ms)

    def open(self, d: dict, c: dict, sz: dict, bar_close_ms: int) -> None:
        t = datetime.fromtimestamp(bar_close_ms / 1000, tz=timezone.utc).astimezone(TZ)
        pos = {
            "id": f"{P['id_prefix']}-{t.strftime('%Y%m%d-%H%M')}-{c['symbol']}", "symbol": c["symbol"], "side": c["side"],
            "rules": P["rules"], "cycle": d.get("cycle", 1), "slippage": P["slip"],
            "mode": "paper", "status": "open", "group": P["group"], "market": "usdm_perp", "margin_type": "isolated",
            "signal_tf": c["tf"], "signal_bar_close_myt": ms_fmt(bar_close_ms), "vol_ratio": round(c["vol_ratio"], 2),
            "breakout_level": c["level"], "entry_time": fmt(t), "entry_ms": bar_close_ms,
            "entry_price": sz["entry"], "qty": sz["qty"], "remaining_qty": sz["qty"], "notional": sz["notional"],
            "leverage": sz["leverage"], "margin": sz["margin"], "liq_price": round(sz["liq"], 8), "mmr": sz["mmr"],
            "stop": sz["stop"], "initial_stop": sz["stop"], "R": sz["R"], "stop_pct": round(sz["stop_pct"] * 100, 3),
            "risk_usdt": sz["risk_usdt"], "entry_fee": sz["entry_fee"], "funding": 0.0, "best": sz["entry"],
            "high_since_entry": sz["entry"], "low_since_entry": sz["entry"], "trail_armed": False, "targets": [],
            "exit_deadline": fmt(t + timedelta(days=P["expiry_days"])), "last_check_ms": bar_close_ms,
        }
        d["positions"].append(pos)
        decision("open", self.dry, symbol=pos["symbol"], side=pos["side"], entry=pos["entry_price"], stop=pos["stop"],
                 qty=pos["qty"], lev=pos["leverage"], liq=pos["liq_price"], risk=round(pos["risk_usdt"], 3))
        emit(self.dry, "futures_open", pos["symbol"], {
            "side": pos["side"], "entry": pos["entry_price"], "stop": pos["stop"], "leverage": pos["leverage"],
            "notional": round(pos["notional"], 2), "risk_usdt": round(pos["risk_usdt"], 3), "tf": pos["signal_tf"],
            "vol_ratio": pos["vol_ratio"]})
        if not self.dry:
            self.write_equity(d, f"開倉 {pos['symbol']} {pos['side']}")

    # ---------- equity / status ----------
    def unrealized(self, d: dict, marks: dict[str, float]) -> float:
        u = 0.0
        for p in d["positions"]:
            px = marks.get(p["symbol"]) or p.get("last_price") or p["entry_price"]
            u += (1 if p["side"] == "BUY" else -1) * (px - p["entry_price"]) * p["qty"] - p["entry_fee"] + p.get("funding", 0.0)
        return u

    def write_equity(self, d: dict, note: str) -> None:
        marks = self.state.get("_marks") or {}
        u = self.unrealized(d, marks)
        w = float(d.get("wallet_usdt", core.START_EQUITY))
        with EQUITY.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["paper-futures", fmt(), f"{w:.4f}", f"{u:.4f}", f"{w+u:.4f}", len(d["positions"]),
                                    f"{w-core.START_EQUITY:.4f}", note])
        self.state["last_equity_ts"] = time.time()

    def write_status(self, d: dict, marks: dict[str, float]) -> None:
        w = float(d.get("wallet_usdt", core.START_EQUITY))
        u = self.unrealized(d, marks)
        st = {
            "group": P["group"], "set": P["name"], "mode": "paper", "rules": P["rules"], "updated_at": fmt(),
            "cycle": d.get("cycle", 1), "refills": max(0, int(d.get("cycle", 1)) - 1),
            "params": {k: P[k] for k in ("tf", "risk", "atr_mult", "max_pos", "max_same_dir", "expiry_days", "slip", "regime")},
            "wallet_usdt": round(w, 4), "unrealized_usdt": round(u, 4), "equity_usdt": round(w + u, 4),
            "pnl_pct": round((w + u - core.START_EQUITY) / core.START_EQUITY * 100, 3),
            "regime": {k: self.state.get("last_regime", {}).get(k) for k in ("allow", "btc_close", "ref", "reason")},
            "last_scan": self.state.get("last_scan"),
            "positions": [{k: p.get(k) for k in ("id", "symbol", "side", "entry_time", "entry_price", "qty", "notional", "leverage",
                                                  "margin", "liq_price", "stop", "initial_stop", "R", "trail_armed", "funding",
                                                  "last_price", "exit_deadline", "signal_tf", "vol_ratio")} for p in d["positions"]],
            "data_source": self.api.base.replace("https://", "") + " /fapi (public)",
        }
        if not self.dry:
            jsave(STATUS, st)
        self.state["_status"] = st

    def maybe_refill(self, d: dict) -> None:
        """Paper wallet depleted (< 10 USDT, no open positions) -> fresh 100 USDT, next cycle (dante 2026-11-16)."""
        w = float(d.get("wallet_usdt", core.START_EQUITY))
        if d["positions"] or w >= REFILL_BELOW:
            return
        cyc = int(d.get("cycle", 1))
        ev = {"time": fmt(), "group": P["group"], "set": P["name"], "rules": P["rules"], "ended_cycle": cyc,
              "new_cycle": cyc + 1, "wallet_before": round(w, 4), "refill_usdt": core.START_EQUITY}
        decision("refill", self.dry, **ev)
        if self.dry:
            return
        with REFILLS.open("a", encoding="utf-8") as f:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
        d["cycle"] = cyc + 1
        d["wallet_usdt"] = core.START_EQUITY
        self.save_doc(d)
        self.write_equity(d, f"補充本金 第{cyc + 1}輪起始 100 USDT（前一輪餘 {w:.2f}）")
        emit(self.dry, "futures_refill", "-", ev)
        update_lifetime()

    # ---------- cycle ----------
    def cycle(self) -> None:
        if not self.dry and not paths.can_write():
            log.warning("standing down: writer_lock=%s expected=%s mode=%s root=%s", paths.current_writer(), paths.expected_writer(), paths.mode(), paths.root())
            self.state["_standdown"] = True
            jsave(HEARTBEAT, {"ok": True, "updated_at": fmt(), "pid": os.getpid(), "standdown": True,
                              "writer": paths.current_writer(), "expected": paths.expected_writer()})
            return
        ensure_ledger() if not self.dry else None
        d = self.doc() or {"wallet_usdt": core.START_EQUITY, "positions": []}
        d.setdefault("positions", [])
        prices = {x["symbol"]: float(x["price"]) for x in self.api.get("/fapi/v1/ticker/price")}
        marks = {x["symbol"]: float(x["markPrice"]) for x in self.api.get("/fapi/v1/premiumIndex")}
        self.state["_marks"] = {p["symbol"]: marks.get(p["symbol"]) for p in d["positions"]}
        self.manage(d, prices, marks)
        self.maybe_refill(d)
        now_ms = int(time.time() * 1000)
        for bar_close in missed_signal_bars(now_ms, int(self.state.get("last_scan_bar_ms") or 0), P["tf"]):
            self.scan(d, prices, bar_close)
            self.state["last_scan_bar_ms"] = bar_close
            # Catch up stops on anything opened on this (possibly historical) bar.
            self.manage(d, prices, marks)
            self.maybe_refill(d)
        self.save_doc(d)
        if not self.dry and time.time() - float(self.state.get("last_equity_ts") or 0) >= EQUITY_EVERY_S:
            self.write_equity(d, "定時快照")
        self.write_status(d, marks)
        if not self.dry:
            jsave(STATE, {k: v for k, v in self.state.items() if not k.startswith("_")})
            jsave(HEARTBEAT, {"ok": True, "updated_at": fmt(), "pid": os.getpid(), "positions": len(d["positions"]),
                              "wallet_usdt": d.get("wallet_usdt"), "source": self.api.base})

    def run(self) -> int:
        PID.write_text(f"{os.getpid()}\n")
        signal.signal(signal.SIGTERM, lambda *_: setattr(self, "stop", True))
        log.info("quant starting pid=%s set=%s rules=%s paper-only", os.getpid(), P["name"], P["rules"])
        while not self.stop:
            t0 = time.time()
            try:
                self.cycle()
            except Exception as e:
                log.exception("cycle failed: %s", e)
                try:
                    jsave(HEARTBEAT, {"ok": False, "updated_at": fmt(), "pid": os.getpid(), "error": type(e).__name__})
                except Exception:
                    pass
            while not self.stop and time.time() - t0 < LOOP_S:
                time.sleep(1)
        log.info("quant stopped")
        return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force-scan", action="store_true", help="re-scan the latest closed bar")
    ap.add_argument("--profile", default="4h", choices=sorted(PROFILES))
    a = ap.parse_args()
    configure(a.profile)
    loop_mode = not (a.once or a.dry_run)
    handlers = [] if loop_mode else [logging.StreamHandler(sys.stdout)]
    if not a.dry_run:
        handlers.append(logging.FileHandler(LOG, encoding="utf-8"))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=handlers)
    q = Quant(dry=a.dry_run)
    if a.force_scan or a.dry_run:
        q.state["last_scan_bar_ms"] = 0
    if a.once or a.dry_run:
        q.cycle()
        st = q.state.get("_status", {})
        stood = bool(q.state.get("_standdown"))
        print(json.dumps({
            "standdown": stood,
            "writer": paths.current_writer(),
            "expected": paths.expected_writer(),
            "root": str(paths.root()),
            "heartbeat": str(HEARTBEAT),
            "regime": st.get("regime"),
            "last_scan": st.get("last_scan"),
            "positions": st.get("positions"),
            "equity": st.get("equity_usdt"),
        }, ensure_ascii=False, indent=1))
        return 2 if stood else 0
    return q.run()


if __name__ == "__main__":
    sys.exit(main())
