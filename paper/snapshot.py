#!/usr/bin/env python3
"""Build dashboard paper snapshots from the current PAPER_* tree. No secrets."""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path

import paper_paths as paths

TOKEN_PAT = re.compile(
    r"(github_pat_[A-Za-z0-9_]+|ghp_[A-Za-z0-9]{20,}|gho_[A-Za-z0-9]{20,}|ghu_[A-Za-z0-9]{20,}"
    r"|ghs_[A-Za-z0-9]{20,}|Bearer\s+\S+|ngrok[^\s\"']{8,})",
    re.I,
)

POSITION_WATCH_KEYS = (
    "id", "symbol", "side", "mode", "status", "entry_price", "entry_time", "qty",
    "remaining_qty", "stop", "initial_stop", "R", "targets", "high_since_entry",
    "low_since_entry", "exit_deadline",
)


def _read_json(path: Path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _trades(path: Path, keep, n=20):
    if not path.exists():
        return []
    try:
        with path.open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        return [{k: r.get(k) for k in keep} for r in rows[-n:]]
    except Exception:
        return []


def futures_summary() -> dict:
    F, J = paths.futures_dir(), paths.journal()
    st = _read_json(F / "status.json", {}) or {}
    out = {
        "group": "合約量化組", "mode": "paper", "rules": st.get("rules"),
        "updated_at": st.get("updated_at"), "wallet_usdt": st.get("wallet_usdt"),
        "unrealized_usdt": st.get("unrealized_usdt"), "equity_usdt": st.get("equity_usdt"),
        "pnl_pct": st.get("pnl_pct"), "regime": st.get("regime"),
        "positions": st.get("positions") or [], "last_scan": st.get("last_scan"),
        "equity_series": [], "recent_trades": [],
    }
    eq = J / "futures" / "equity.csv"
    if eq.exists():
        try:
            with eq.open(encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
            out["equity_series"] = [{"t": r.get("time_myt"), "equity": float(r.get("equity_usdt") or 0)} for r in rows[-500:]]
        except Exception:
            pass
    keep = ("trade_id", "symbol", "side", "signal_tf", "entry_time_myt", "entry_price", "exit_time_myt",
            "exit_price", "exit_reason", "leverage", "net_pnl", "R_multiple", "funding", "fees")
    trades = J / "futures" / "trades.csv"
    out["recent_trades"] = _trades(trades, keep, 30)
    try:
        with trades.open(encoding="utf-8") as fh:
            rows = [r for r in csv.DictReader(fh) if r.get("net_pnl")]
        out["closed_trades"] = len(rows)
        out["win_rate"] = round(sum(1 for r in rows if float(r["net_pnl"]) > 0) / len(rows), 3) if rows else None
    except Exception:
        pass
    return out


def paper_sets() -> dict:
    F, J = paths.futures_dir(), paths.journal()
    specs = {
        "futures_1h": (F / "run_1h" / "status.json", J / "futures_1h" / "trades.csv"),
        "futures_hf15m": (F / "run_hf15m" / "status.json", J / "futures_hf15m" / "trades.csv"),
        "spot_experiment": (J / "experiment" / "status.json", J / "experiment" / "trades.csv"),
    }
    keep = ("trade_id", "symbol", "side", "entry_time_myt", "entry_price", "exit_time_myt", "exit_price",
            "exit_reason", "leverage", "net_pnl", "R_multiple", "fees")
    out = {}
    for key, (sp, tp) in specs.items():
        st = _read_json(sp, {}) or {}
        row = {k: st.get(k) for k in ("group", "rules", "updated_at", "cycle", "refills", "wallet_usdt",
                                       "unrealized_usdt", "equity_usdt", "pnl_pct", "params", "regime")}
        row["mode"] = "paper"
        row["positions"] = st.get("positions") or []
        row["recent_trades"] = _trades(tp, keep, 20) if tp.exists() else []
        out[key] = row
    return out


def futures_watch_positions() -> list:
    J = paths.journal()
    specs = [
        (J / "futures" / "positions.json", "合約量化組"),
        (J / "futures_1h" / "positions.json", "合約1h組"),
        (J / "futures_hf15m" / "positions.json", "合約高頻組"),
    ]
    rows = []
    for path, group in specs:
        doc = _read_json(path, {}) or {}
        for item in doc.get("positions") or []:
            if not isinstance(item, dict):
                continue
            if str(item.get("status") or "open").lower() != "open":
                continue
            row = {k: item.get(k) for k in POSITION_WATCH_KEYS}
            row["market"] = "usdm_perp"
            row["group"] = group
            rows.append(row)
    return rows


def build_all() -> dict:
    lifetime = _read_json(paths.journal() / "lifetime_stats.json", {}) or {}
    data_paper = {
        "futures_quant": futures_summary(),
        "paper_sets": paper_sets(),
        "lifetime_stats": lifetime,
        "paper_updated_at": paths.now_myt(),
        "paper_writer": paths.current_writer(),
    }
    watch_state = {
        "mode": "paper",
        "futures_positions": futures_watch_positions(),
        "paper_updated_at": data_paper["paper_updated_at"],
        "source": "paper cycle (no live orders)",
    }
    return {"data_paper": data_paper, "watch_state": watch_state}


def patch_data_json(path: Path, data_paper: dict) -> None:
    doc = _read_json(path, {}) or {}
    if not isinstance(doc, dict):
        doc = {}
    doc.update(data_paper)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(doc, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    tmp.replace(path)


def merge_watch_state(path: Path, paper_watch: dict) -> None:
    doc = _read_json(path, {}) or {}
    if not isinstance(doc, dict):
        doc = {}
    # Keep formal-spot positions, plans, emitted, highs from the existing file (box publisher).
    doc["futures_positions"] = paper_watch.get("futures_positions") or []
    doc["paper_updated_at"] = paper_watch.get("paper_updated_at")
    doc["mode"] = "paper"
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)
