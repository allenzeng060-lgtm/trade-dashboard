#!/usr/bin/env python3
"""Read-only public-data probe for GitHub-hosted runners. Paper planning only.

Prints HTTP status and the host that answered. No secrets, no ledger writes,
no orders. Safe to run on workflow_dispatch.
"""
from __future__ import annotations

import json
import ssl
import time
import urllib.error
import urllib.request

UA = "paper-connectivity-probe/1.0"
TIMEOUT = 12
CTX = ssl.create_default_context()

CHECKS = [
    ("spot", "https://data-api.binance.vision/api/v3/ticker/price?symbol=BTCUSDT"),
    ("spot", "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"),
    ("spot_klines", "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=2"),
    ("futures", "https://www.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT"),
    ("futures", "https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT"),
    ("futures_klines", "https://www.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=5m&limit=2"),
    ("futures_klines", "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=5m&limit=2"),
]


def probe(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT, context=CTX) as r:
            body = r.read(2000)
            ms = int((time.time() - t0) * 1000)
            price = None
            try:
                data = json.loads(body)
                if isinstance(data, dict) and "price" in data:
                    price = data["price"]
                elif isinstance(data, list) and data:
                    price = data[-1][4]  # kline close
            except Exception:
                pass
            return {"ok": True, "status": r.status, "ms": ms, "price": price, "error": None}
    except urllib.error.HTTPError as e:
        ms = int((time.time() - t0) * 1000)
        snippet = ""
        try:
            snippet = e.read(180).decode("utf-8", "replace")
        except Exception:
            pass
        return {"ok": False, "status": e.code, "ms": ms, "price": None, "error": snippet[:180]}
    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        return {"ok": False, "status": None, "ms": ms, "price": None, "error": f"{type(e).__name__}"}


def main() -> int:
    rows = []
    print("paper-connectivity probe (public data only, no keys, no writes)")
    for kind, url in CHECKS:
        result = probe(url)
        host = url.split("/")[2]
        path = "/" + "/".join(url.split("/")[3:]).split("?")[0]
        row = {"kind": kind, "host": host, "path": path, **result}
        rows.append(row)
        status = row["status"] if row["status"] is not None else "err"
        price = f" price={row['price']}" if row["price"] is not None else ""
        err = f" error={row['error']}" if row["error"] else ""
        print(f"  {kind:16} {host:28} HTTP {status}  {row['ms']}ms{price}{err}")

    spot_ok = any(r["kind"].startswith("spot") and r["ok"] for r in rows)
    fut_ok = any(r["kind"].startswith("futures") and r["ok"] for r in rows)
    fut_hosts = [r["host"] for r in rows if r["kind"].startswith("futures") and r["ok"]]
    print(f"summary spot_ok={str(spot_ok).lower()} futures_ok={str(fut_ok).lower()} futures_hosts={fut_hosts}")
    # Exit 0 even if some hosts fail, as long as one spot and one futures path works.
    # A total failure should fail the job so we notice.
    return 0 if (spot_ok and fut_ok) else 1


if __name__ == "__main__":
    raise SystemExit(main())
