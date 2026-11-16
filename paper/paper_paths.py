#!/usr/bin/env python3
"""Portable PAPER paths and the single-writer lock. No secrets.

Environment (all optional, box defaults shown):
  PAPER_ROOT      /workspace
  PAPER_JOURNAL   $PAPER_ROOT/trading-journal
  PAPER_FUTURES   $PAPER_ROOT/futures
  PAPER_SPOT_EXP  $PAPER_ROOT/spot_exp
  PAPER_WATCHER   $PAPER_ROOT/watcher
  PAPER_LOCK      $PAPER_JOURNAL/writer_lock.json
  PAPER_WRITER    box          # who *this process* is
  PAPER_MODE      live|shadow  # shadow may write its own tree regardless of lock
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from datetime import datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Kuala_Lumpur")


def root() -> Path:
    return Path(os.environ.get("PAPER_ROOT", "/workspace"))


def journal() -> Path:
    return Path(os.environ.get("PAPER_JOURNAL", str(root() / "trading-journal")))


def futures_dir() -> Path:
    return Path(os.environ.get("PAPER_FUTURES", str(root() / "futures")))


def spot_exp_dir() -> Path:
    return Path(os.environ.get("PAPER_SPOT_EXP", str(root() / "spot_exp")))


def watcher_dir() -> Path:
    return Path(os.environ.get("PAPER_WATCHER", str(root() / "watcher")))


def lock_path() -> Path:
    return Path(os.environ.get("PAPER_LOCK", str(journal() / "writer_lock.json")))


def expected_writer() -> str:
    return (os.environ.get("PAPER_WRITER") or "box").strip().lower()


def mode() -> str:
    return (os.environ.get("PAPER_MODE") or "live").strip().lower()


def now_myt() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S MYT")


def read_lock() -> dict:
    p = lock_path()
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
        return doc if isinstance(doc, dict) else {}
    except Exception:
        return {}


def current_writer() -> str:
    w = str(read_lock().get("writer") or "box").strip().lower()
    return w or "box"


def can_write() -> bool:
    if mode() == "shadow":
        return True
    return current_writer() == expected_writer()


def write_lock(writer: str, note: str = "") -> dict:
    doc = {
        "writer": writer,
        "mode": "paper",
        "updated_at": now_myt(),
        "note": note,
    }
    p = lock_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(p)
    return doc
