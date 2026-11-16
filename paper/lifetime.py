#!/usr/bin/env python3
"""Lifetime PAPER stats per group and overall, never reset by refills (dante 2026-11-16).

Rebuilt from the ledgers every time (idempotent): python3 lifetime.py
Output: /workspace/trading-journal/lifetime_stats.json
gross_profit = sum of net PnL of winning trades; gross_loss = sum of net PnL of losing trades (negative).
fees are already inside net PnL (shown separately for info).
"""
from __future__ import annotations
import csv, json, os, tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

def _journal() -> Path:
    return Path(os.environ.get("PAPER_JOURNAL", os.environ.get("PAPER_ROOT", "/workspace") + "/trading-journal"))

def _futures() -> Path:
    return Path(os.environ.get("PAPER_FUTURES", os.environ.get("PAPER_ROOT", "/workspace") + "/futures"))

J = _journal()
OUT = J / "lifetime_stats.json"
START = 100.0
SPOT_FEE = 0.001

def _groups():
    J, F = _journal(), _futures()
    return [
        # key, label, kind, trades csv, refills, status json (for current equity)
        ("spot_formal", "現貨正式組", "spot", J / "trades.csv", J / "refills_spot_formal.jsonl", None),
        ("spot_experiment", "現貨實驗組", "spot", J / "experiment" / "trades.csv", J / "experiment" / "refills.jsonl", J / "experiment" / "status.json"),
        ("futures_4h", "合約量化組（4h）", "fut", J / "futures" / "trades.csv", J / "futures" / "refills.jsonl", F / "status.json"),
        ("futures_1h", "合約1h組", "fut", J / "futures_1h" / "trades.csv", J / "futures_1h" / "refills.jsonl", F / "run_1h" / "status.json"),
        ("futures_hf15m", "合約高頻組（15m）", "fut", J / "futures_hf15m" / "trades.csv", J / "futures_hf15m" / "refills.jsonl", F / "run_hf15m" / "status.json"),
    ]

GROUPS = _groups()


def f(x, d=0.0):
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return d


def rows(path: Path):
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        return [r for r in csv.DictReader(fh) if (r.get("mode") or r.get("mode(paper/live)") or "").startswith("paper")]


def trade_list(kind, path):
    out = []
    for r in rows(path):
        if kind == "spot":
            if not (r.get("出場時間(UTC+8)") or "").strip():
                continue  # still open
            net = f(r.get("已實現損益(USDT)"))
            fees = f(r.get("手續費")) + f(r.get("出場價")) * f(r.get("出場量")) * SPOT_FEE
            out.append({"net": net, "fees": fees, "funding": 0.0, "time": r.get("出場時間(UTC+8)"), "id": r.get("trade_id")})
        else:
            out.append({"net": f(r.get("net_pnl")), "fees": f(r.get("fees")), "funding": f(r.get("funding")),
                        "time": r.get("exit_time_myt"), "id": r.get("trade_id")})
    return out


def summarize(trs, refills):
    wins = [t for t in trs if t["net"] > 0]
    losses = [t for t in trs if t["net"] < 0]
    gp = sum(t["net"] for t in wins)
    gl = sum(t["net"] for t in losses)
    return {
        "trades": len(trs), "wins": len(wins), "losses": len(losses),
        "win_rate_pct": round(len(wins) / len(trs) * 100, 1) if trs else None,
        "gross_profit_usdt": round(gp, 4), "gross_loss_usdt": round(gl, 4), "net_pnl_usdt": round(gp + gl, 4),
        "profit_factor": round(gp / -gl, 3) if gl < 0 else None,
        "fees_paid_usdt": round(sum(t["fees"] for t in trs), 4), "funding_usdt": round(sum(t["funding"] for t in trs), 4),
        "refills": refills, "capital_injected_usdt": round(START * (1 + refills), 2), "current_cycle": refills + 1,
    }


def rebuild() -> dict:
    groups, all_tr, all_ref = {}, [], 0
    J = _journal()
    for key, label, kind, tpath, rpath, spath in _groups():
        trs = trade_list(kind, tpath)
        ref = sum(1 for line in rpath.open(encoding="utf-8") if line.strip()) if rpath.exists() else 0
        g = {"label": label, **summarize(trs, ref), "ledger": str(tpath)}
        if spath and spath.exists():
            try:
                st = json.loads(spath.read_text())
                g["current_equity_usdt"] = st.get("equity_usdt")
            except Exception:
                pass
        groups[key] = g
        all_tr += trs
        all_ref += ref
    total = summarize(all_tr, all_ref)
    total["capital_injected_usdt"] = round(START * len(GROUPS) + START * all_ref, 2)
    total.pop("current_cycle")
    doc = {"updated_at": datetime.now(ZoneInfo("Asia/Kuala_Lumpur")).strftime("%Y-%m-%d %H:%M:%S MYT"), "mode": "paper",
           "note": "累計統計（不因補充本金重置）。gross_profit=獲利單淨損益合計，gross_loss=虧損單淨損益合計，皆已扣手續費。",
           "groups": groups, "total": total}
    fd, tmp = tempfile.mkstemp(dir=str(J), prefix=".lifetime.")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=1)
    os.replace(tmp, OUT)
    return doc


if __name__ == "__main__":
    print(json.dumps(rebuild()["total"], ensure_ascii=False))
