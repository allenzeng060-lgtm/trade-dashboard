#!/usr/bin/env python3
"""現貨實驗組 automatic PAPER loop, rules exp-v2-auto (Chief 2026-11-16). No AI, no keys, no orders.

Trigger: a bullish watcher alert (break_24h_high / price_move_5m_up / price_move_15m_up / volume_spike_5m)
in the last 60 min. Confirm on Binance SPOT public data (data-api.binance.vision):
  last closed 15m close > highest high of the previous 20 closed 15m bars, and that bar's quote volume
  >= 2x the average of the previous 96 bars (24h). Long only (spot).
Risk 1% of wallet per trade; stop = entry - 3 x ATR14(15m), stop must be 1%..10% away; notional <= wallet/4;
max 4 positions; one per symbol; 4h cooldown per symbol after exit. Fees 0.1%/side, slippage 0.05%/side.
Exit: stop (worse of stop/observed price), +2R -> stop to entry then trail 1R behind best, 24h expiry.
NVDA earnings window 2026-11-17 21:00 .. 2026-11-19 05:20 MYT: no new entries.
Wallet < 10 USDT with no positions -> refill to 100 USDT (new cycle), logged in refills.jsonl.
"""
from __future__ import annotations
import argparse, csv, json, logging, os, signal, sys, time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import requests

def _code_dir() -> Path:
    cands = [
        Path(os.environ["PAPER_CODE"]) if os.environ.get("PAPER_CODE") else None,
        Path(__file__).resolve().parent,  # repo paper/ next to core.py
        Path(__file__).resolve().parent.parent / "futures",
        Path("/workspace/futures"),
    ]
    for d in cands:
        if d and (d / "core.py").exists():
            return d
    return Path("/workspace/futures")

sys.path.insert(0, str(_code_dir()))
import core  # noqa: E402  (shared ATR / trailing-stop logic)
import paper_paths as paths  # noqa: E402

TZ = ZoneInfo("Asia/Kuala_Lumpur")
RULES = "exp-v2-auto"
GROUP = "現貨實驗組"
BASE = "https://data-api.binance.vision"

def _bind_paths():
    global LEDGER, POS, TRADES, EQUITY, REFILLS, STATUS, HERE, STATE, HEARTBEAT, LOG, PID, DECISIONS, ALERTS, OUTBOX
    LEDGER = paths.journal() / "experiment"
    POS, TRADES, EQUITY, REFILLS = LEDGER / "positions.json", LEDGER / "trades.csv", LEDGER / "equity.csv", LEDGER / "refills.jsonl"
    STATUS = LEDGER / "status.json"
    HERE = paths.spot_exp_dir()
    HERE.mkdir(parents=True, exist_ok=True)
    LEDGER.mkdir(parents=True, exist_ok=True)
    STATE, HEARTBEAT, LOG, PID, DECISIONS = HERE / "state.json", HERE / "heartbeat.json", HERE / "spot_exp.log", HERE / "spot_exp.python.pid", HERE / "decisions.jsonl"
    ALERTS = Path(os.environ.get("PAPER_ALERTS", str(paths.watcher_dir() / "alerts.jsonl")))
    OUTBOX = paths.futures_dir() / "outbox.jsonl"

LEDGER = Path("/workspace/trading-journal/experiment")
POS = TRADES = EQUITY = REFILLS = STATUS = HERE = STATE = HEARTBEAT = LOG = PID = DECISIONS = ALERTS = OUTBOX = Path("/tmp")
_bind_paths()
BULLISH = {"break_24h_high", "price_move_5m_up", "price_move_15m_up", "volume_spike_5m"}
START = 100.0
FEE = 0.001
SLIP = 0.0005
RISK = 0.01
ATR_MULT = 3.0
MIN_STOP, MAX_STOP = 0.01, 0.10
MAX_POS = 4
MIN_QVOL = 3_000_000.0
LOOKBACK, VOL_N, VOL_MIN = 20, 96, 2.0
EXPIRY_H = 24
COOLDOWN_S = 4 * 3600
ALERT_WINDOW_S = 3600
REFILL_BELOW = 10.0
NO_ENTRY_WINDOWS = [("2026-11-17 21:00", "2026-11-19 05:20")]
EXCLUDE = {"PAXGUSDT", "XAUTUSDT", "USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "USDPUSDT", "EURUSDT", "USD1USDT", "RLUSDUSDT", "BFUSDUSDT"}
LOOP_S = 60
COLS = ["mode(paper/live)", "trade_id", "記錄時間(UTC+8)", "交易對", "方向", "規則版本", "進場理由", "Scout觸發條件", "Filter結論",
        "Analyst觀點", "Risk結論", "下單時間(UTC+8)", "成交價", "成交量", "成交金額(USDT)", "手續費", "手續費幣種", "出場時間(UTC+8)",
        "出場價", "出場量", "已實現損益(USDT)", "損益%", "資料來源", "備註", "價格來源與取價時間", "復盤(虧損必填)", "BTC濾網之上(是/否)"]
log = logging.getLogger("spot_exp")
S = requests.Session(); S.headers["User-Agent"] = "paper-spot-exp"


def now(): return datetime.now(TZ)
def fmt(t=None): return (t or now()).strftime("%Y-%m-%d %H:%M:%S MYT")
def jload(p, d):
    try: return json.loads(Path(p).read_text())
    except Exception: return d
def jsave(p, o):
    tmp = Path(str(p) + ".tmp"); tmp.write_text(json.dumps(o, ensure_ascii=False, indent=1)); os.replace(tmp, p)
def get(path, params=None):
    for i in range(3):
        try:
            r = S.get(BASE + path, params=params, timeout=(5, 15))
            if r.status_code == 200: return r.json()
            if r.status_code in (418, 429): time.sleep(30)
        except requests.RequestException: pass
        time.sleep(1 + i)
    raise RuntimeError(f"GET {path} failed")
def klines(sym, iv, limit=None, start=None):
    p = {"symbol": sym, "interval": iv}
    if limit: p["limit"] = limit
    if start: p["startTime"] = start
    return [{"open_ms": int(x[0]), "close_ms": int(x[6]) + 1, "open": float(x[1]), "high": float(x[2]), "low": float(x[3]),
             "close": float(x[4]), "qvol": float(x[7])} for x in get("/api/v3/klines", p)]
def decision(kind, dry, **kw):
    log.info("decision %s %s", kind, json.dumps(kw, ensure_ascii=False, default=str)[:300])
    if not dry:
        with DECISIONS.open("a", encoding="utf-8") as f: f.write(json.dumps({"time": fmt(), "kind": kind, **kw}, ensure_ascii=False, default=str) + "\n")
def in_no_entry():
    n = now()
    for a, b in NO_ENTRY_WINDOWS:
        if datetime.strptime(a, "%Y-%m-%d %H:%M").replace(tzinfo=TZ) <= n < datetime.strptime(b, "%Y-%m-%d %H:%M").replace(tzinfo=TZ):
            return True
    return False


class Bot:
    def __init__(self, dry=False):
        self.dry = dry
        self.state = jload(STATE, {})
        self.stop = False

    def doc(self):
        d = jload(POS, {}) or {}
        if d.get("rules") != RULES:  # migrate the old (empty) experiment ledger header
            d = {"group": GROUP, "mode": "paper", "rules": RULES, "start_equity_usdt": START, "cycle": 1,
                 "wallet_usdt": float(d.get("wallet_usdt", START)), "max_concurrent": MAX_POS,
                 "created": d.get("created") or fmt(), "auto_since": fmt(), "positions": [], "closed": d.get("closed", [])}
        return d

    def save(self, d):
        if not self.dry: jsave(POS, d)

    def alerts(self):
        """New bullish watcher alerts -> candidates {symbol: last alert time}."""
        cands = self.state.setdefault("cands", {})
        off = int(self.state.get("alerts_offset", 0))
        try:
            size = ALERTS.stat().st_size
            if off > size or not self.state.get("alerts_init"):
                off = max(0, size - 200_000); self.state["alerts_init"] = True
            with ALERTS.open("rb") as f:
                f.seek(off); chunk = f.read()
            end = chunk.rfind(b"\n")
            if end >= 0:
                for raw in chunk[: end + 1].splitlines():
                    try: a = json.loads(raw)
                    except Exception: continue
                    if a.get("signal") in BULLISH:
                        ts = datetime.fromisoformat(a["time"]).timestamp()
                        cands[a["symbol"]] = max(ts, cands.get(a["symbol"], 0))
                off += end + 1
            self.state["alerts_offset"] = off
        except OSError as e:
            log.warning("alerts read failed: %s", e)
        cut = time.time() - ALERT_WINDOW_S
        for s in [s for s, t in cands.items() if t < cut]: cands.pop(s)
        return cands

    def manage(self, d, prices):
        for pos in list(d["positions"]):
            sym = pos["symbol"]; price = prices.get(sym)
            if not price: continue
            now_ms = int(time.time() * 1000); last = int(pos["last_check_ms"])
            iv = "1m" if now_ms - last < 900 * 60000 else "5m"
            try: bars = [b for b in klines(sym, iv, limit=1000, start=last - 60000) if b["close_ms"] > last]
            except Exception: bars = []
            hit = None
            for b in bars:
                hit = core.step_bar(pos, b["high"], b["low"])
                if hit is not None: break
            if hit is None: hit = core.step_bar(pos, price, price)
            pos["last_check_ms"] = now_ms; pos["last_price"] = price
            if hit is not None:
                reason = "trail_stop" if pos.get("trail_armed") else "stop"
                fill = hit if bars else core.worse_fill("BUY", hit, price)
                self.close(d, pos, fill, reason, price)
            elif time.time() * 1000 - pos["entry_ms"] >= EXPIRY_H * 3600_000:
                self.close(d, pos, price, "expiry_24h", price)

    def close(self, d, pos, fill, reason, observed):
        fill = fill * (1 - SLIP)
        qty = pos["qty"]
        exit_fee = fill * qty * FEE
        net = (fill - pos["entry_price"]) * qty - pos["entry_fee"] - exit_fee
        d["wallet_usdt"] = round(d["wallet_usdt"] + net, 6)
        d["positions"].remove(pos)
        self.state.setdefault("cooldown", {})[pos["symbol"]] = time.time()
        R = net / pos["risk_usdt"] if pos["risk_usdt"] else 0
        row = {"mode(paper/live)": "paper", "trade_id": pos["id"], "記錄時間(UTC+8)": fmt(), "交易對": pos["symbol"], "方向": "BUY",
               "規則版本": RULES, "進場理由": pos["reason"], "Scout觸發條件": pos["trigger"], "Filter結論": "自動（不經 Filter）",
               "Analyst觀點": "-", "Risk結論": "程式風控 1%", "下單時間(UTC+8)": pos["entry_time"], "成交價": pos["entry_price"],
               "成交量": qty, "成交金額(USDT)": round(pos["notional"], 4), "手續費": round(pos["entry_fee"], 6), "手續費幣種": "USDT",
               "出場時間(UTC+8)": fmt(), "出場價": round(fill, 10), "出場量": qty, "已實現損益(USDT)": round(net, 4),
               "損益%": round(net / pos["notional"] * 100, 3), "資料來源": "data-api.binance.vision（公開行情）",
               "備註": f"{reason}; R={R:.2f}; 出場費 {exit_fee:.4f}; 含滑價 0.05%/邊; cycle {pos.get('cycle', 1)}",
               "價格來源與取價時間": f"last {observed} @ {fmt()}",
               "復盤(虧損必填)": (f"自動：{reason}，{R:.2f}R；規則化樣本，累積 30 筆後統一復盤" if net < 0 else ""),
               "BTC濾網之上(是/否)": pos.get("btc_above", "")}
        decision("close", self.dry, symbol=pos["symbol"], reason=reason, fill=fill, net=round(net, 4), R=round(R, 2))
        if not self.dry:
            new = not TRADES.exists() or TRADES.stat().st_size == 0
            with TRADES.open("a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, COLS)
                if new: w.writeheader()
                w.writerow(row)
            self.equity(d, f"平倉 {pos['symbol']} {reason}")
            self.save(d)
            try:
                sys.path.insert(0, str(paths.journal())); import lifetime; lifetime.rebuild()
            except Exception as e: log.warning("lifetime: %s", e)

    def scan(self, d, prices, t24):
        cands = self.alerts()
        if in_no_entry():
            return
        bar = int(time.time() * 1000) // 900_000 * 900_000
        done = self.state.setdefault("checked", {})
        cool = self.state.setdefault("cooldown", {})
        for sym in sorted(cands, key=lambda s: -cands[s]):
            if len(d["positions"]) >= MAX_POS: break
            if sym in EXCLUDE or sym.endswith(("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")) or not sym.endswith("USDT"): continue
            if any(p["symbol"] == sym for p in d["positions"]): continue
            if time.time() - cool.get(sym, 0) < COOLDOWN_S: continue
            if done.get(sym) == bar: continue
            done[sym] = bar
            if t24.get(sym, 0) < MIN_QVOL: continue
            try: bars = [b for b in klines(sym, "15m", limit=VOL_N + LOOKBACK + 2) if b["close_ms"] <= bar]
            except Exception: continue
            sig = core.breakout_signal(bars, "15m", LOOKBACK, VOL_N)
            if not sig or sig["side"] != "BUY" or sig["vol_ratio"] < VOL_MIN:
                continue
            atr = core.atr_wilder(bars[-40:])
            price = sig.get("close") or prices.get(sym)
            if not atr or not price: continue
            entry = price * (1 + SLIP)
            r = ATR_MULT * atr
            sp = r / entry
            base = {"symbol": sym, "vol_ratio": round(sig["vol_ratio"], 2), "stop_pct": round(sp * 100, 2)}
            if sp < MIN_STOP or sp > MAX_STOP:
                decision("skip", self.dry, **base, reason=f"stop {sp*100:.2f}% outside 1-10%"); continue
            w = d["wallet_usdt"]
            qty = RISK * w / (r + FEE * (2 * entry - r))
            notional = min(qty * entry, w / MAX_POS)
            if notional < 5:
                decision("skip", self.dry, **base, reason="notional < 5"); continue
            qty = notional / entry
            t = now()
            pos = {"id": f"SX-{t.strftime('%Y%m%d-%H%M')}-{sym}", "symbol": sym, "side": "BUY", "rules": RULES, "cycle": d.get("cycle", 1),
                   "entry_time": fmt(t), "entry_ms": int(t.timestamp() * 1000), "entry_price": entry, "qty": qty, "notional": notional,
                   "entry_fee": notional * FEE, "stop": entry - r, "initial_stop": entry - r, "R": r, "best": entry,
                   "risk_usdt": qty * r + FEE * qty * (2 * entry - r), "trail_armed": False, "last_check_ms": int(t.timestamp() * 1000),
                   "last_price": price, "reason": f"15m 收盤 {sig['close']} > 前20根高 {sig['level']}，量比 {sig['vol_ratio']:.2f}",
                   "trigger": "watcher 多方警示（1h 內）", "exit_deadline": fmt(t + timedelta(hours=EXPIRY_H))}
            d["positions"].append(pos)
            decision("open", self.dry, **base, entry=entry, stop=pos["stop"], notional=round(notional, 2), risk=round(pos["risk_usdt"], 3))
            if not self.dry: self.equity(d, f"開倉 {sym}")

    def refill(self, d):
        if d["positions"] or d["wallet_usdt"] >= REFILL_BELOW: return
        cyc = int(d.get("cycle", 1))
        ev = {"time": fmt(), "group": GROUP, "set": "spot_exp", "rules": RULES, "ended_cycle": cyc, "new_cycle": cyc + 1,
              "wallet_before": round(d["wallet_usdt"], 4), "refill_usdt": START}
        decision("refill", self.dry, **ev)
        if self.dry: return
        with REFILLS.open("a", encoding="utf-8") as f: f.write(json.dumps(ev, ensure_ascii=False) + "\n")
        d["cycle"] = cyc + 1; d["wallet_usdt"] = START
        self.equity(d, f"補充本金 第{cyc+1}輪起始 100 USDT")
        with OUTBOX.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"id": f"sx_{int(time.time())}", "time": fmt(), "type": "futures_refill", "symbol": "-",
                                "group": GROUP, "details": ev}, ensure_ascii=False) + "\n")

    def unreal(self, d):
        return sum((p.get("last_price", p["entry_price"]) - p["entry_price"]) * p["qty"] - p["entry_fee"] for p in d["positions"])

    def equity(self, d, note):
        u = self.unreal(d)
        with EQUITY.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["paper-experiment", fmt(), f"{d['wallet_usdt'] + u:.2f}", "", f"{d['wallet_usdt'] - START:.2f}", "", f"{note}（{RULES}）"])
        self.state["last_eq"] = time.time()

    def cycle(self):
        if not self.dry and not paths.can_write():
            log.warning("standing down: writer_lock=%s expected=%s mode=%s", paths.current_writer(), paths.expected_writer(), paths.mode())
            jsave(HEARTBEAT, {"ok": True, "updated_at": fmt(), "pid": os.getpid(), "standdown": True,
                              "writer": paths.current_writer(), "expected": paths.expected_writer()})
            return {"standdown": True, "writer": paths.current_writer()}
        d = self.doc()
        prices = {x["symbol"]: float(x["price"]) for x in get("/api/v3/ticker/price")}
        t24 = self.state.get("_t24")
        if not t24 or time.time() - self.state.get("_t24_ts", 0) > 600:
            t24 = {x["symbol"]: float(x["quoteVolume"]) for x in get("/api/v3/ticker/24hr", {"type": "MINI"})}
            self.state["_t24"], self.state["_t24_ts"] = t24, time.time()
        self.manage(d, prices)
        self.refill(d)
        self.scan(d, prices, t24)
        self.save(d)
        u = self.unreal(d)
        st = {"group": GROUP, "rules": RULES, "mode": "paper", "updated_at": fmt(), "cycle": d.get("cycle", 1),
              "refills": d.get("cycle", 1) - 1, "wallet_usdt": round(d["wallet_usdt"], 4), "unrealized_usdt": round(u, 4),
              "equity_usdt": round(d["wallet_usdt"] + u, 4), "no_entry_window": in_no_entry(),
              "candidates_1h": len(self.state.get("cands", {})),
              "positions": [{k: p.get(k) for k in ("id", "symbol", "entry_time", "entry_price", "qty", "notional", "stop", "initial_stop",
                                                   "trail_armed", "last_price", "exit_deadline")} for p in d["positions"]]}
        if not self.dry:
            jsave(STATUS, st)
            if time.time() - self.state.get("last_eq", 0) > 3600: self.equity(d, "定時快照")
            jsave(STATE, {k: v for k, v in self.state.items() if k != "_t24"})
            jsave(HEARTBEAT, {"ok": True, "updated_at": fmt(), "pid": os.getpid(), "positions": len(d["positions"])})
        return st

    def run(self):
        PID.write_text(f"{os.getpid()}\n")
        signal.signal(signal.SIGTERM, lambda *_: setattr(self, "stop", True))
        log.info("spot_exp starting pid=%s rules=%s paper-only", os.getpid(), RULES)
        while not self.stop:
            t0 = time.time()
            try: self.cycle()
            except Exception as e:
                log.exception("cycle failed: %s", e)
                try: jsave(HEARTBEAT, {"ok": False, "updated_at": fmt(), "pid": os.getpid(), "error": type(e).__name__})
                except Exception: pass
            while not self.stop and time.time() - t0 < LOOP_S: time.sleep(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--once", action="store_true")
    a = ap.parse_args()
    _bind_paths()
    hs = [logging.StreamHandler(sys.stdout)] if (a.dry_run or a.once) else []
    if not a.dry_run:
        HERE.mkdir(parents=True, exist_ok=True)
        hs.append(logging.FileHandler(LOG, encoding="utf-8"))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=hs)
    b = Bot(dry=a.dry_run)
    if a.dry_run or a.once:
        if a.dry_run:
            b.state["alerts_init"] = False
        print(json.dumps(b.cycle(), ensure_ascii=False, indent=1, default=str)); return 0
    return b.run()


if __name__ == "__main__":
    sys.exit(main())
