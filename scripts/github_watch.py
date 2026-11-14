#!/usr/bin/env python3
"""Paper-trading watch job for GitHub Actions.

Reads scrubbed state from the `data` branch, checks public Binance prices,
and POSTs the existing trade webhook only when a stop, target, trailing-stop
update, expiry, plan fill, or plan invalidation/expiry needs action.

Never places orders or touches a Binance account. Never logs, prints, or
writes TRADE_WEBHOOK_URL, TRADE_WEBHOOK_AUTH, or GITHUB_TOKEN.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

OWNER = os.environ.get("WATCH_OWNER") or "allenzeng060-lgtm"
REPO = os.environ.get("WATCH_REPO") or "trade-dashboard"
DATA_BRANCH = "data"
WATCH_STATE_PATH = "data/watch_state.json"
WATCH_EMITTED_PATH = "data/watch_emitted.json"
DATA_JSON_PATH = "data/data.json"
API = "https://api.github.com"
PRIMARY_BASE = "https://data-api.binance.vision"
FALLBACK_BASE = "https://api.binance.com"
# 合約量化組 (paper futures) prices: USDT-M public endpoints; fapi.binance.com
# can return 451 by region, www.binance.com/fapi usually works; falls back to
# the same symbol on spot when futures endpoints are unreachable.
FUTURES_BASES = ("https://www.binance.com", "https://fapi.binance.com")
UA = "paper-github-watch/1.0"
LOCAL_TZ = ZoneInfo("Asia/Kuala_Lumpur")
CONNECT_TIMEOUT_S = 5.0
READ_TIMEOUT_S = 10.0
HTTP_TIMEOUT_S = CONNECT_TIMEOUT_S + READ_TIMEOUT_S
MAX_RETRIES = 3
KLINE_LIMIT = 30
NEW_LOW_LOOKBACK = 20
BTC_FILTER_DEFAULT = 67_568.0
# Publisher force-pushes data.json every 600s while the box is awake.
# If the snapshot is older than this, treat the box as asleep and send.
BOX_FRESH_S = 12 * 60
POS_KLINE_INTERVAL = "5m"
POS_KLINE_LIMIT = 120  # 10 hours of 5m bars, covers a long box sleep
NUM_RE = re.compile(r"\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+\.\d+|\d+")
TOKEN_PAT = re.compile(
    r"(github_pat_[A-Za-z0-9_]+|ghp_[A-Za-z0-9]{20,}|gho_[A-Za-z0-9]{20,}|ghu_[A-Za-z0-9]{20,}"
    r"|ghs_[A-Za-z0-9]{20,}|Bearer\s+\S+|ngrok[^\s\"']{8,})",
    re.I,
)


class ApiError(RuntimeError):
    pass


def now_local() -> datetime:
    return datetime.now(LOCAL_TZ)


def fmt_myt(dt: datetime | None = None) -> str:
    dt = dt or now_local()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    else:
        dt = dt.astimezone(LOCAL_TZ)
    return dt.strftime("%Y-%m-%d %H:%M:%S MYT")


def log(msg: str) -> None:
    text = TOKEN_PAT.sub("[redacted]", str(msg))
    print(f"{fmt_myt()} watch: {text}", flush=True)


def is_dry_run() -> bool:
    v = (os.environ.get("DRY_RUN") or "").strip().lower()
    if v in {"1", "true", "yes", "y"}:
        return True
    if v in {"0", "false", "no", "n"}:
        return False
    # Local / unexpected runs default to dry-run so a webhook is never sent
    # accidentally. Scheduled Actions jobs set DRY_RUN=false explicitly.
    return os.environ.get("GITHUB_ACTIONS") != "true"


def parse_float_text(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "").replace("，", "")
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        found = NUM_RE.search(text)
        if not found:
            return default
        try:
            return float(found.group(0).replace(",", ""))
        except ValueError:
            return default


def parse_floats(text: str) -> list[float]:
    return [float(m.replace(",", "")) for m in NUM_RE.findall(text or "")]


def parse_myt(text: Any) -> datetime | None:
    if text is None:
        return None
    if isinstance(text, datetime):
        return text if text.tzinfo else text.replace(tzinfo=LOCAL_TZ)
    raw = str(text).strip()
    if not raw:
        return None
    m = re.search(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})(?::(\d{2}))?", raw)
    if not m:
        return None
    second = m.group(3) or "00"
    dt = datetime.strptime(f"{m.group(1)} {m.group(2)}:{second}", "%Y-%m-%d %H:%M:%S")
    if raw.endswith("Z") or "+00:00" in raw:
        return dt.replace(tzinfo=timezone.utc).astimezone(LOCAL_TZ)
    m_off = re.search(r"([+-]\d{2}):?(\d{2})$", raw)
    if m_off and "MYT" not in raw:
        off = timezone(timedelta(hours=int(m_off.group(1)), minutes=int(m_off.group(2))))
        return dt.replace(tzinfo=off).astimezone(LOCAL_TZ)
    return dt.replace(tzinfo=LOCAL_TZ)


def _normalize_token(raw: str) -> str:
    t = (raw or "").strip()
    if t.lower().startswith("bearer "):
        t = t[7:].strip()
    if t.lower().startswith("token "):
        t = t[6:].strip()
    return t


def github_token() -> str:
    return _normalize_token(
        os.environ.get("GITHUB_TOKEN") or os.environ.get("GITHUB_DASHBOARD_TOKEN") or ""
    )


def redact(msg: Any) -> str:
    return TOKEN_PAT.sub("[redacted]", str(msg))[:240]


def github_api(method: str, path: str, body: dict | None = None, timeout: int = 30):
    tok = github_token()
    data = None if body is None else json.dumps(body).encode()
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": UA,
        "Content-Type": "application/json",
    }
    if tok:
        headers["Authorization"] = f"Bearer {tok}"
    req = urllib.request.Request(API + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read() if e.fp else b""
        try:
            payload = json.loads(raw) if raw else {}
        except Exception:
            payload = {"message": raw.decode("utf-8", "replace")[:200]}
        if isinstance(payload, dict) and "message" in payload:
            payload["message"] = redact(payload.get("message"))
        return e.code, payload
    except urllib.error.URLError as e:
        return 0, {"message": type(e).__name__}


def github_file(path: str, branch: str) -> tuple[dict[str, Any] | None, str | None]:
    st, info = github_api("GET", f"/repos/{OWNER}/{REPO}/contents/{path}?ref={branch}")
    if st == 404:
        return None, None
    if st != 200 or not isinstance(info, dict):
        raise ApiError(f"GET {path} failed: HTTP {st} {redact((info or {}).get('message'))}")
    content = info.get("content") or ""
    if info.get("encoding") == "base64" or content:
        raw = base64.b64decode(content.replace("\n", ""))
        try:
            doc = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ApiError(f"{path} is not valid JSON") from exc
        if not isinstance(doc, dict):
            raise ApiError(f"{path} is not a JSON object")
        return doc, info.get("sha")
    return None, info.get("sha")


def put_github_file(path: str, content: bytes, branch: str, message: str, sha: str | None) -> str:
    body = {
        "message": message,
        "content": base64.b64encode(content).decode("ascii"),
        "branch": branch,
    }
    if sha:
        body["sha"] = sha
    st, res = github_api("PUT", f"/repos/{OWNER}/{REPO}/contents/{path}", body, timeout=60)
    if st in (200, 201):
        return "pushed"
    raise ApiError(f"PUT {path} failed: HTTP {st} {redact((res or {}).get('message'))}")


class PublicClient:
    def __init__(self) -> None:
        self._ticker_cache: dict[str, tuple[float, float]] = {}
        self._kline_cache: dict[tuple[str, str, int], tuple[float, list[dict[str, Any]]]] = {}
        self.host_used = PRIMARY_BASE

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        qs = f"?{urllib.parse.urlencode(params)}" if params else ""
        last_err: Exception | None = None
        for base in (PRIMARY_BASE, FALLBACK_BASE):
            url = base + path + qs
            for attempt in range(1, MAX_RETRIES + 1):
                req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
                try:
                    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as r:
                        raw = r.read()
                        self.host_used = base
                        return json.loads(raw)
                except urllib.error.HTTPError as exc:
                    last_err = exc
                    if exc.code == 451:
                        break
                    if exc.code >= 500:
                        time.sleep(min(0.4 * attempt, 2.0))
                        continue
                    break
                except (urllib.error.URLError, TimeoutError, OSError) as exc:
                    last_err = exc
                    time.sleep(min(0.4 * attempt, 2.0))
        raise ApiError(type(last_err).__name__ if last_err else "request failed")

    def ticker_price(self, symbol: str) -> float:
        now = time.time()
        cached = self._ticker_cache.get(symbol)
        if cached and now - cached[0] < 3.0:
            return cached[1]
        data = self._get("/api/v3/ticker/price", {"symbol": symbol})
        price = float(data["price"])
        self._ticker_cache[symbol] = (now, price)
        return price

    def closed_klines(self, symbol: str, interval: str = "4h", limit: int = KLINE_LIMIT) -> list[dict[str, Any]]:
        now = time.time()
        key = (symbol, interval, limit)
        cached = self._kline_cache.get(key)
        if cached and now - cached[0] < 15.0:
            return cached[1]
        raw = self._get("/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        now_ms = int(now * 1000)
        out: list[dict[str, Any]] = []
        for row in raw:
            close_ms = int(row[6])
            if close_ms >= now_ms:
                continue
            out.append(
                {
                    "open_time": int(row[0]),
                    "close_time": close_ms,
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "open_myt": fmt_myt(datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc)),
                    "close_myt": fmt_myt(datetime.fromtimestamp(close_ms / 1000, tz=timezone.utc)),
                }
            )
        self._kline_cache[key] = (now, out)
        return out


class FuturesClient(PublicClient):
    """Futures (USDT-M) prices with spot fallback for the same symbol."""

    def _fget(self, path: str, params: dict[str, Any]) -> Any:
        qs = f"?{urllib.parse.urlencode(params)}"
        last_err: Exception | None = None
        for base in FUTURES_BASES:
            req = urllib.request.Request(base + path + qs, headers={"User-Agent": UA, "Accept": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as r:
                    data = json.loads(r.read())
                    self.host_used = base + "/fapi"
                    return data
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
                last_err = exc
                continue
        raise ApiError(type(last_err).__name__ if last_err else "futures request failed")

    def ticker_price(self, symbol: str) -> float:
        try:
            data = self._fget("/fapi/v1/ticker/price", {"symbol": symbol})
            return float(data["price"])
        except (ApiError, KeyError, TypeError, ValueError):
            log(f"futures ticker unavailable for {symbol}; trying spot")
            return super().ticker_price(symbol)

    def closed_klines(self, symbol: str, interval: str = "4h", limit: int = KLINE_LIMIT) -> list[dict[str, Any]]:
        try:
            raw = self._fget("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        except ApiError:
            return super().closed_klines(symbol, interval, limit)
        now_ms = int(time.time() * 1000)
        out: list[dict[str, Any]] = []
        for row in raw:
            close_ms = int(row[6])
            if close_ms >= now_ms:
                continue
            out.append({"open_time": int(row[0]), "close_time": close_ms, "open": float(row[1]), "high": float(row[2]),
                        "low": float(row[3]), "close": float(row[4]), "volume": float(row[5])})
        return out


def parse_trigger(text: str) -> dict[str, Any]:
    raw = text or ""
    t = raw.replace("–", "-").replace("—", "-").replace("－", "-")
    m = re.search(
        r"4h\s*收盤\s*(>=|≥|>)\s*([0-9.,]+).{0,40}參考\s*([0-9.,]+).{0,20}上限\s*([0-9.,]+)",
        t,
    )
    if m:
        return {
            "style": "close_then_limit",
            "close_op": ">" if m.group(1) == ">" else ">=",
            "close_level": float(m.group(2).replace(",", "")),
            "entry": float(m.group(3).replace(",", "")),
            "limit_low": None,
            "limit_high": float(m.group(4).replace(",", "")),
            "limit_cap": float(m.group(4).replace(",", "")),
            "raw": raw,
        }
    m = re.search(
        r"限價單掛\s*([0-9.,]+)\s*-\s*([0-9.,]+).{0,40}參考\s*([0-9.,]+).{0,80}"
        r"4h\s*收盤須\s*(>=|≥|>)\s*([0-9.,]+)",
        t,
    )
    if m:
        low = float(m.group(1).replace(",", ""))
        high = float(m.group(2).replace(",", ""))
        return {
            "style": "limit_band",
            "close_op": ">" if m.group(4) == ">" else ">=",
            "close_level": float(m.group(5).replace(",", "")),
            "entry": float(m.group(3).replace(",", "")),
            "limit_low": min(low, high),
            "limit_high": max(low, high),
            "limit_cap": max(low, high),
            "raw": raw,
        }
    nums = parse_floats(t)
    log("unrecognized trigger phrasing, using numeric fallback")
    return {
        "style": "unknown",
        "close_op": ">=",
        "close_level": nums[0] if nums else None,
        "entry": nums[1] if len(nums) > 1 else None,
        "limit_low": None,
        "limit_high": None,
        "limit_cap": None,
        "raw": raw,
    }


def parse_btc_threshold(text: str) -> float:
    m = re.search(r"≥\s*([0-9,]+(?:\.\d+)?)", text or "")
    if m:
        return float(m.group(1).replace(",", ""))
    nums = parse_floats(text or "")
    return nums[0] if nums else BTC_FILTER_DEFAULT


def parse_invalidation(items: list[Any]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "pre_entry_close_lt": None,
        "no_chase_above": None,
        "breakdown_below": None,
        "manual": [],
    }
    for item in items or []:
        text = str(item)
        if "持倉中" in text:
            continue
        if "不追" in text or "超過不追" in text:
            nums = parse_floats(text)
            if nums:
                result["no_chase_above"] = nums[0]
            continue
        m = re.search(r"成交前.{0,8}4h\s*收盤\s*<\s*([0-9.,]+)", text)
        if m:
            result["pre_entry_close_lt"] = float(m.group(1).replace(",", ""))
            continue
        m = re.search(r"跌破\s*([0-9.,]+)", text)
        if m:
            result["breakdown_below"] = float(m.group(1).replace(",", ""))
            continue
        if any(word in text for word in ("GovPool", "提案", "公告", "金庫", "重查")):
            result["manual"].append(text)
    return result


def close_meets(close: float, op: str, level: float) -> bool:
    if op == ">":
        return close > level
    return close >= level


def candle_touched_limit(candle: dict[str, Any], side: str, low: float | None, high: float | None) -> bool:
    if low is None and high is None:
        return False
    lo = low if low is not None else high
    hi = high if high is not None else low
    if lo is None or hi is None:
        return False
    if side != "SELL":
        return candle["low"] <= hi and candle["high"] >= lo
    return candle["high"] >= lo and candle["low"] <= hi


def price_in_limit_range(price: float | None, side: str, low: float | None, high: float | None) -> bool:
    if price is None:
        return False
    lo = low if low is not None else high
    hi = high if high is not None else low
    if lo is None or hi is None:
        return False
    return lo <= price <= hi


def live_price_fillable(price: float | None, side: str, trig: dict[str, Any]) -> bool:
    if price is None:
        return False
    style = trig.get("style")
    if style == "limit_band":
        return price_in_limit_range(price, side, trig.get("limit_low"), trig.get("limit_high"))
    if style == "close_then_limit":
        cap = trig.get("limit_cap")
        if cap is None:
            return False
        if side != "SELL":
            return price <= cap
        return price >= cap
    return False


def evaluate_btc_filter(client: PublicClient, threshold: float) -> dict[str, Any]:
    candles = client.closed_klines("BTCUSDT", "4h", KLINE_LIMIT)
    if not candles:
        return {"ok": False, "reason": "no_closed_btc_klines", "threshold": threshold}
    last = candles[-1]
    prior = candles[-(NEW_LOW_LOOKBACK + 1) : -1]
    min_prior_close = min(c["close"] for c in prior) if prior else None
    close_is_new_low = min_prior_close is not None and last["close"] < min_prior_close
    close_ok = last["close"] >= threshold
    new_low_ok = not close_is_new_low
    ok = close_ok and new_low_ok
    return {
        "ok": ok,
        "threshold": threshold,
        "btc_close": last["close"],
        "close_ok": close_ok,
        "new_low_ok": new_low_ok,
        "reason": None if ok else ("btc_close_below_threshold" if not close_ok else "btc_close_is_new_low"),
        "candle_close_myt": last["close_myt"],
    }


def normalize_plan(raw: dict[str, Any], created: datetime | None) -> dict[str, Any] | None:
    status = str(raw.get("status") or raw.get("plan_status") or "pending").lower()
    if status != "pending":
        return None
    symbol = str(raw.get("symbol") or "").upper()
    if not symbol:
        return None
    trigger = parse_trigger(str(raw.get("trigger") or ""))
    if raw.get("entry") is not None and trigger.get("entry") is None:
        trigger["entry"] = parse_float_text(raw.get("entry"))
    inv = parse_invalidation(raw.get("invalidation") or [])
    if trigger.get("style") == "close_then_limit" and trigger.get("limit_cap") and not inv["no_chase_above"]:
        inv["no_chase_above"] = trigger["limit_cap"]
    return {
        "symbol": symbol,
        "side": str(raw.get("side") or "BUY").upper(),
        "trigger_raw": raw.get("trigger"),
        "trigger": trigger,
        "btc_filter_raw": raw.get("btc_filter") or "",
        "btc_threshold": parse_btc_threshold(str(raw.get("btc_filter") or "")),
        "entry": parse_float_text(raw.get("entry"), trigger.get("entry")),
        "stop": parse_float_text(raw.get("stop")),
        "invalidation": inv,
        "entry_expiry": parse_myt(raw.get("entry_expiry")),
        "entry_expiry_raw": raw.get("entry_expiry"),
        "created": created,
    }


def normalize_position(item: dict[str, Any]) -> dict[str, Any] | None:
    if str(item.get("mode") or "paper").lower() != "paper":
        return None
    if str(item.get("status") or "open").lower() in {"closed", "cancelled", "canceled", "expired"}:
        return None
    symbol = str(item.get("symbol") or "").upper()
    if not symbol:
        return None
    targets: list[dict[str, Any]] = []
    for t in item.get("targets") or []:
        if isinstance(t, dict):
            price = parse_float_text(t.get("price") or t.get("target"))
            if price is None:
                continue
            targets.append(
                {
                    "price": price,
                    "portion": parse_float_text(t.get("portion"), 1.0),
                    "filled": bool(t.get("filled")),
                }
            )
        else:
            price = parse_float_text(t)
            if price is not None:
                targets.append({"price": price, "portion": 1.0, "filled": False})
    entry = parse_float_text(item.get("entry_price") or item.get("entry") or item.get("avg_entry"))
    stop = parse_float_text(item.get("stop") or item.get("stop_price"))
    initial_stop = parse_float_text(item.get("initial_stop") or item.get("stop"))
    r_val = parse_float_text(item.get("R"))
    if r_val is None and entry is not None and initial_stop is not None:
        r_val = abs(entry - initial_stop)
    return {
        "id": str(item.get("id") or item.get("trade_id") or symbol),
        "symbol": symbol,
        "side": str(item.get("side") or "BUY").upper(),
        "entry_price": entry,
        "entry_time": parse_myt(item.get("entry_time") or item.get("open_time")),
        "qty": parse_float_text(item.get("remaining_qty") or item.get("qty")),
        "remaining_qty": parse_float_text(item.get("remaining_qty") or item.get("qty")),
        "stop": stop,
        "initial_stop": initial_stop if initial_stop is not None else stop,
        "R": r_val,
        "targets": targets,
        "high_since_entry": parse_float_text(item.get("high_since_entry")),
        "low_since_entry": parse_float_text(item.get("low_since_entry")),
        "exit_deadline": parse_myt(item.get("exit_deadline")),
    }


def evaluate_plan(client: PublicClient, plan: dict[str, Any], btc: dict[str, Any], now: datetime) -> dict[str, Any]:
    symbol = plan["symbol"]
    trig = plan["trigger"]
    inv = plan["invalidation"]
    candles = client.closed_klines(symbol, "4h", KLINE_LIMIT)
    last = candles[-1] if candles else None
    created = plan.get("created")
    expiry = plan.get("entry_expiry")
    expired = bool(expiry and now >= expiry)
    close_level = trig.get("close_level")
    close_op = trig.get("close_op") or ">="
    close_ok = bool(last and close_level is not None and close_meets(last["close"], close_op, close_level))
    invalidated = False
    invalid_reason: str | None = None
    pre_lt = inv.get("pre_entry_close_lt")
    if last and pre_lt is not None and last["close"] < pre_lt:
        invalidated = True
        invalid_reason = f"pre_entry_4h_close_lt_{pre_lt}"
    breakdown = inv.get("breakdown_below")
    if last and breakdown is not None and (last["low"] < breakdown or last["close"] < breakdown):
        invalidated = True
        invalid_reason = invalid_reason or f"breakdown_below_{breakdown}"

    no_chase = inv.get("no_chase_above")
    above_cap = bool(last and no_chase is not None and last["close"] > no_chase)

    fill_candles: list[dict[str, Any]] = []
    if close_ok and not expired and not invalidated and last:
        for c in candles:
            if created and c["close_time"] <= int(created.timestamp() * 1000):
                continue
            if trig.get("style") == "limit_band":
                if candle_touched_limit(c, plan["side"], trig.get("limit_low"), trig.get("limit_high")):
                    fill_candles.append(c)
            elif trig.get("style") == "close_then_limit":
                cap = trig.get("limit_cap")
                entry = trig.get("entry")
                band_low = entry if entry is not None else cap
                if cap is not None and candle_touched_limit(c, plan["side"], band_low, cap):
                    fill_candles.append(c)
    live_price = None
    live_in_range = False
    try:
        live_price = client.ticker_price(symbol)
    except ApiError as exc:
        log(f"ticker failed for {symbol}: {type(exc).__name__}")
    if live_price is not None and close_ok and not expired and not invalidated:
        live_in_range = live_price_fillable(live_price, plan["side"], trig)

    fill_likely = (
        (bool(fill_candles) or live_in_range)
        and btc.get("ok")
        and close_ok
        and not expired
        and not invalidated
    )
    if above_cap and not fill_likely and trig.get("style") == "close_then_limit" and close_ok:
        invalidated = True
        invalid_reason = invalid_reason or f"no_chase_above_{no_chase}"

    actionable = None
    if expired:
        actionable = "plan_expired"
    elif invalidated:
        actionable = "plan_invalidated"
    elif fill_likely:
        actionable = "plan_triggered"

    return {
        "symbol": symbol,
        "side": plan["side"],
        "actionable": actionable,
        "fill_likely": fill_likely,
        "live_price": live_price,
        "live_in_range": live_in_range,
        "expired": expired,
        "invalidated": invalidated,
        "invalid_reason": invalid_reason,
        "close_ok": close_ok,
        "btc_ok": bool(btc.get("ok")),
        "close_level": close_level,
        "entry": plan.get("entry") or trig.get("entry"),
        "stop": plan.get("stop"),
        "entry_expiry": plan.get("entry_expiry_raw"),
        "fill_candle_close_myt": fill_candles[-1]["close_myt"] if fill_candles else None,
        "btc": {
            "ok": btc.get("ok"),
            "close": btc.get("btc_close"),
            "threshold": btc.get("threshold"),
            "reason": btc.get("reason"),
        },
    }


def recommended_stop(pos: dict[str, Any], high: float, low: float) -> tuple[float | None, bool]:
    entry = pos.get("entry_price")
    initial = pos.get("initial_stop")
    r_val = pos.get("R")
    side = pos.get("side") or "BUY"
    if entry is None or r_val is None or r_val <= 0:
        return pos.get("stop"), False
    if side != "SELL":
        moved = (high - entry) >= (2 * r_val)
        if not moved:
            return pos.get("stop") if pos.get("stop") is not None else initial, False
        trail = max(entry, high - r_val)
        current = pos.get("stop")
        if current is not None:
            trail = max(trail, current)
        return trail, True
    moved = (entry - low) >= (2 * r_val)
    if not moved:
        return pos.get("stop") if pos.get("stop") is not None else initial, False
    trail = min(entry, low + r_val)
    current = pos.get("stop")
    if current is not None:
        trail = min(trail, current)
    return trail, True


def position_range(client: PublicClient, pos: dict[str, Any], price: float, state_highs: dict[str, float]) -> tuple[float, float]:
    """Recover high/low across a box-sleep gap using 5m klines plus tracked values."""
    pos_id = pos["id"]
    values_high = [v for v in (pos.get("high_since_entry"), state_highs.get(pos_id), price, pos.get("entry_price")) if v is not None]
    values_low = [v for v in (pos.get("low_since_entry"), state_highs.get(pos_id + ":low"), price, pos.get("entry_price")) if v is not None]
    try:
        candles = client.closed_klines(pos["symbol"], POS_KLINE_INTERVAL, POS_KLINE_LIMIT)
        entry_time = pos.get("entry_time")
        cutoff_ms = int(entry_time.timestamp() * 1000) if entry_time else 0
        for c in candles:
            if c["close_time"] < cutoff_ms:
                continue
            values_high.append(c["high"])
            values_low.append(c["low"])
    except ApiError as exc:
        log(f"5m klines failed for {pos['symbol']}: {type(exc).__name__}")
    high = max(values_high) if values_high else price
    low = min(values_low) if values_low else price
    return high, low


def evaluate_position(
    client: PublicClient,
    pos: dict[str, Any],
    now: datetime,
    state_highs: dict[str, float],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    symbol = pos["symbol"]
    pos_id = pos["id"]
    try:
        price = client.ticker_price(symbol)
    except ApiError as exc:
        log(f"ticker failed for {symbol}: {type(exc).__name__}")
        return events

    high, low = position_range(client, pos, price, state_highs)
    state_highs[pos_id] = high
    state_highs[pos_id + ":low"] = low

    entry = pos.get("entry_price")
    side = pos.get("side") or "BUY"
    rec_stop, trail_armed = recommended_stop(pos, high, low)
    current_stop = pos.get("stop")
    effective_stop = rec_stop if rec_stop is not None else current_stop

    deadline = pos.get("exit_deadline")
    if deadline is None and pos.get("entry_time") is not None:
        deadline = pos["entry_time"] + timedelta(days=7)
    expired = bool(deadline and now >= deadline)

    # Use gap high/low so a pierce-and-bounce during box sleep is still caught.
    hit_stop = False
    if effective_stop is not None:
        if side != "SELL" and low <= effective_stop:
            hit_stop = True
        elif side == "SELL" and high >= effective_stop:
            hit_stop = True

    hit_targets: list[dict[str, Any]] = []
    for t in pos.get("targets") or []:
        if t.get("filled"):
            continue
        tp = t["price"]
        if side != "SELL" and high >= tp:
            hit_targets.append(t)
        elif side == "SELL" and low <= tp:
            hit_targets.append(t)

    base_details = {
        "position_id": pos_id,
        "price": price,
        "entry": entry,
        "side": side,
        "qty": pos.get("remaining_qty") or pos.get("qty"),
        "current_stop": current_stop,
        "recommended_stop": rec_stop,
        "trail_armed": trail_armed,
        "high_since_entry": high,
        "low_since_entry": low,
        "R": pos.get("R"),
        "exit_deadline": fmt_myt(deadline) if deadline else None,
    }
    if hit_stop:
        events.append(
            {
                "type": "position_stop",
                "symbol": symbol,
                "dedupe": f"position_stop:{pos_id}:{effective_stop}",
                "details": {**base_details, "stop_hit": effective_stop},
            }
        )
        return events
    if hit_targets:
        events.append(
            {
                "type": "position_target",
                "symbol": symbol,
                "dedupe": "position_target:%s:%s" % (pos_id, ",".join(str(t["price"]) for t in hit_targets)),
                "details": {**base_details, "targets_hit": hit_targets},
            }
        )
    if trail_armed and rec_stop is not None and current_stop is not None:
        moved = rec_stop > current_stop + 1e-12 if side != "SELL" else rec_stop < current_stop - 1e-12
        if moved:
            events.append(
                {
                    "type": "position_trailing_update",
                    "symbol": symbol,
                    "dedupe": f"position_trailing_update:{pos_id}:{rec_stop}",
                    "details": {**base_details, "new_stop": rec_stop},
                }
            )
    if expired:
        events.append(
            {
                "type": "position_expiry",
                "symbol": symbol,
                "dedupe": f"position_expiry:{pos_id}:{fmt_myt(deadline) if deadline else ''}",
                "details": base_details,
            }
        )
    return events


def event_summary(record: dict[str, Any]) -> str:
    details = record.get("details") or {}
    typ = record.get("type")
    symbol = record.get("symbol") or ""
    if typ == "plan_triggered":
        return f"{symbol} 計畫觸發（可成交）"
    if typ == "plan_expired":
        return f"{symbol} 計畫已過期"
    if typ == "plan_invalidated":
        return f"{symbol} 計畫失效：{details.get('invalid_reason')}"
    if typ == "position_stop":
        return f"{symbol} 持倉觸及停損 {details.get('stop_hit')}"
    if typ == "position_target":
        return f"{symbol} 持倉觸及目標"
    if typ == "position_trailing_update":
        return f"{symbol} 建議移動停損至 {details.get('new_stop')}"
    if typ == "position_expiry":
        return f"{symbol} 持倉到期應出場"
    if typ == "futures_stop":
        return f"【合約量化組】{symbol} 模擬持倉觸及停損 {details.get('stop_hit')}（box 醒來後 quant.py 會自動平倉）"
    if typ == "futures_expiry":
        return f"【合約量化組】{symbol} 模擬持倉 7 天到期（box 醒來後 quant.py 會自動平倉）"
    return typ or "event"


def stable_event_id(dedupe: str) -> str:
    digest = hashlib.sha1(dedupe.encode("utf-8")).hexdigest()[:16]
    return f"ga_{digest}"


def build_wake_payload(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "source": "github-actions",
        "time": fmt_myt(),
        "count": len(records),
        "events": [
            {
                "id": r.get("id"),
                "type": r.get("type"),
                "symbol": r.get("symbol"),
                "time": r.get("time"),
                "summary": event_summary(r),
                "dedupe": r.get("dedupe"),
            }
            for r in records
        ],
    }


def webhook_configured() -> bool:
    return bool((os.environ.get("TRADE_WEBHOOK_URL") or "").strip())


def _normalize_auth_value(raw: str) -> str:
    text = (raw or "").strip()
    if text.lower().startswith("authorization:"):
        text = text.split(":", 1)[1].strip()
    return text


def post_webhook(payload: dict[str, Any]) -> int | None:
    url = (os.environ.get("TRADE_WEBHOOK_URL") or "").strip()
    if not url:
        log("webhook not configured")
        return None
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    auth = (os.environ.get("TRADE_WEBHOOK_AUTH") or "").strip()
    if auth:
        headers["Authorization"] = _normalize_auth_value(auth)
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as r:
            log(f"webhook POST status={r.status}")
            return int(r.status)
    except urllib.error.HTTPError as exc:
        log(f"webhook POST status={exc.code}")
        return int(exc.code)
    except TimeoutError:
        log("webhook POST failed: timeout")
        return None
    except (urllib.error.URLError, OSError) as exc:
        log(f"webhook POST failed: {type(exc).__name__}")
        return None


def parse_emitted(doc: dict[str, Any] | None) -> dict[str, float]:
    if not doc:
        return {}
    raw = doc.get("emitted") if isinstance(doc.get("emitted"), dict) else {}
    out: dict[str, float] = {}
    for k, v in raw.items():
        try:
            out[str(k)] = float(v) if isinstance(v, (int, float)) else time.time()
        except (TypeError, ValueError):
            out[str(k)] = time.time()
    return out


def publisher_age_s(watch_state: dict[str, Any]) -> float | None:
    raw = watch_state.get("published_at") or watch_state.get("publisher_updated_at")
    dt = parse_myt(raw)
    if not dt:
        return None
    return max(0.0, (now_local() - dt).total_seconds())


def collect_events(client: PublicClient, state: dict[str, Any], now: datetime) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    created = parse_myt(state.get("plans_created"))
    plans = []
    for raw in state.get("plans") or []:
        if not isinstance(raw, dict):
            continue
        plan = normalize_plan(raw, created)
        if plan:
            plans.append(plan)
    positions = []
    for raw in state.get("positions") or []:
        if not isinstance(raw, dict):
            continue
        pos = normalize_position(raw)
        if pos:
            positions.append(pos)

    highs = {}
    for k, v in (state.get("position_highs") or {}).items():
        try:
            highs[str(k)] = float(v)
        except (TypeError, ValueError):
            continue

    snapshot: dict[str, Any] = {"plans": [], "positions": len(positions), "btc": None}
    events: list[dict[str, Any]] = []

    btc = None
    if plans:
        threshold = max(p["btc_threshold"] for p in plans)
        btc = evaluate_btc_filter(client, threshold)
        snapshot["btc"] = {"ok": btc.get("ok"), "close": btc.get("btc_close"), "reason": btc.get("reason")}
        for plan in plans:
            row = evaluate_plan(client, plan, btc, now)
            snapshot["plans"].append(
                {
                    "symbol": row["symbol"],
                    "actionable": row.get("actionable"),
                    "live_price": row.get("live_price"),
                    "close_ok": row.get("close_ok"),
                    "btc_ok": row.get("btc_ok"),
                    "fill_likely": row.get("fill_likely"),
                }
            )
            typ = row.get("actionable")
            if typ == "plan_triggered":
                if not row.get("fill_likely"):
                    continue
                dedupe = f"plan_triggered:{row['symbol']}:{row.get('entry')}:{row.get('close_level')}:fill=True"
            elif typ == "plan_expired":
                dedupe = f"plan_expired:{row['symbol']}:{row.get('entry_expiry')}"
            elif typ == "plan_invalidated":
                dedupe = f"plan_invalidated:{row['symbol']}:{row.get('invalid_reason')}"
            else:
                continue
            events.append({"type": typ, "symbol": row["symbol"], "dedupe": dedupe, "details": row})

    for pos in positions:
        for ev in evaluate_position(client, pos, now, highs):
            events.append(ev)

    # 合約量化組 paper futures: only stop / expiry matter (quant.py on the box
    # does all management itself once it is awake again).
    fut_positions = []
    for raw in state.get("futures_positions") or []:
        if isinstance(raw, dict):
            pos = normalize_position(raw)
            if pos:
                fut_positions.append(pos)
    snapshot["futures_positions"] = len(fut_positions)
    if fut_positions:
        fclient = FuturesClient()
        for pos in fut_positions:
            for ev in evaluate_position(fclient, pos, now, highs):
                kind = {"position_stop": "futures_stop", "position_expiry": "futures_expiry"}.get(ev["type"])
                if not kind:
                    continue
                details = dict(ev.get("details") or {})
                details["group"] = "合約量化組"
                events.append({"type": kind, "symbol": ev["symbol"], "dedupe": "futures:" + ev["dedupe"], "details": details})

    snapshot["highs_updated"] = highs
    return events, snapshot


def save_emitted(existing: dict[str, Any] | None, sha: str | None, new_keys: dict[str, float]) -> str:
    doc = existing if isinstance(existing, dict) else {}
    emitted = parse_emitted(doc)
    emitted.update(new_keys)
    # keep at most 500 newest keys
    if len(emitted) > 500:
        items = sorted(emitted.items(), key=lambda kv: kv[1])
        emitted = dict(items[-300:])
    payload = {
        "updated_at": fmt_myt(),
        "source": "github-actions",
        "emitted": emitted,
    }
    raw = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    return put_github_file(WATCH_EMITTED_PATH, raw, DATA_BRANCH, "record github-actions watch events", sha)


def main() -> int:
    dry = is_dry_run()
    log(
        f"start dry_run={str(dry).lower()} actions={os.environ.get('GITHUB_ACTIONS') or 'false'} "
        f"webhook_configured={str(webhook_configured()).lower()}"
    )
    client = PublicClient()
    try:
        state, _sha = github_file(WATCH_STATE_PATH, DATA_BRANCH)
        if not state:
            log("watch_state.json missing on data branch; publisher may not have pushed yet")
            return 1
        gha_doc, gha_sha = github_file(WATCH_EMITTED_PATH, DATA_BRANCH)
        emitted = parse_emitted(state)
        emitted.update(parse_emitted(gha_doc))

        now = now_local()
        events, snapshot = collect_events(client, state, now)
        host = client.host_used.replace("https://", "")
        prices = []
        for row in snapshot.get("plans") or []:
            if row.get("live_price") is not None:
                prices.append(f"{row['symbol']}={row['live_price']}")
        btc = snapshot.get("btc") or {}
        log(
            f"state_ok positions={len(state.get('positions') or [])} "
            f"futures_positions={len(state.get('futures_positions') or [])} "
            f"pending_plans={len(snapshot.get('plans') or [])} "
            f"price_host={host} prices={{ {', '.join(prices)} }} "
            f"btc_ok={btc.get('ok')} btc_close={btc.get('close')} "
            f"raw_events={len(events)}"
        )

        new_records: list[dict[str, Any]] = []
        for ev in events:
            dedupe = ev["dedupe"]
            if dedupe in emitted:
                continue
            rec = {
                "id": stable_event_id(dedupe),
                "time": fmt_myt(now),
                "type": ev["type"],
                "symbol": ev.get("symbol"),
                "details": ev.get("details") or {},
                "dedupe": dedupe,
                "processed": False,
            }
            new_records.append(rec)

        age = publisher_age_s(state)
        box_fresh = age is not None and age <= BOX_FRESH_S
        age_txt = "unknown" if age is None else f"{int(age)}s"
        type_txt = ", ".join(f"{r['type']}:{r.get('symbol') or ''}" for r in new_records)
        log(
            f"publisher_age={age_txt} box_fresh={str(box_fresh).lower()} "
            f"new_events={len(new_records)} types={type_txt}"
        )

        if not new_records:
            log("no new events; webhook not sent")
            return 0

        if dry:
            log("dry_run=true; webhook not sent and emitted list not updated")
            return 0

        if box_fresh:
            log("box publisher is fresh; leaving webhook to box gate.py")
            return 0

        if not webhook_configured():
            log("webhook not configured; events not sent")
            return 1

        status = post_webhook(build_wake_payload(new_records))
        if status is not None and status < 400:
            new_keys = {r["dedupe"]: time.time() for r in new_records}
            try:
                save_emitted(gha_doc, gha_sha, new_keys)
                log(f"recorded {len(new_keys)} emitted keys on data branch")
            except ApiError as exc:
                log(f"failed to record emitted keys: {exc}")
            return 0
        log("webhook post unsuccessful; emitted list not updated so it can retry")
        return 1
    except ApiError as exc:
        log(f"error: {exc}")
        return 1
    finally:
        pass


if __name__ == "__main__":
    raise SystemExit(main())
