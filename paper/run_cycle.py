#!/usr/bin/env python3
"""One paper cycle for all automated sets (box or GitHub Actions).

Modes:
  shadow  write under data/shadow (or $PAPER_ROOT); never touch live ledgers
  live    write live ledgers only if writer_lock.writer matches PAPER_WRITER

No Binance API keys, no real orders, no secret printing.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import paper_paths as paths  # noqa: E402
import snapshot  # noqa: E402

TZ = ZoneInfo("Asia/Kuala_Lumpur")
TOKEN_PAT = snapshot.TOKEN_PAT
SETS = ("4h", "1h", "15m", "spot")


def now_myt() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S MYT")


def log(msg: str) -> None:
    print(f"{now_myt()} cycle: {TOKEN_PAT.sub('[redacted]', str(msg))}", flush=True)


def run(cmd: list[str], cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    # Never leak the token into argv logs; git uses the env credential helper already set up.
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=check, env=env, text=True,
                          capture_output=True)


def set_paper_env(root: Path, writer: str, mode: str) -> None:
    os.environ["PAPER_ROOT"] = str(root)
    os.environ["PAPER_JOURNAL"] = str(root / "trading-journal")
    os.environ["PAPER_FUTURES"] = str(root / "futures")
    os.environ["PAPER_SPOT_EXP"] = str(root / "spot_exp")
    os.environ["PAPER_WATCHER"] = str(root / "watcher")
    os.environ["PAPER_LOCK"] = str(root / "trading-journal" / "writer_lock.json")
    os.environ["PAPER_WRITER"] = writer
    os.environ["PAPER_MODE"] = mode
    os.environ["PAPER_ALERTS"] = str(root / "watcher" / "alerts.jsonl")
    os.environ.setdefault("PAPER_CODE", str(HERE))


def seed_tree(src_root: Path, dest_root: Path) -> None:
    """Copy ledger/status files into dest (no logs, no pyc, no secrets)."""
    pairs = [
        ("trading-journal/futures", True),
        ("trading-journal/futures_1h", True),
        ("trading-journal/futures_hf15m", True),
        ("trading-journal/experiment", True),
        ("trading-journal/lifetime_stats.json", False),
        ("trading-journal/trades.csv", False),
        ("trading-journal/positions.json", False),
        ("trading-journal/pending_plans.json", False),
        ("trading-journal/writer_lock.json", False),
        ("futures/status.json", False),
        ("futures/state.json", False),
        ("futures/heartbeat.json", False),
        ("futures/run_1h", True),
        ("futures/run_hf15m", True),
        ("spot_exp/state.json", False),
        ("spot_exp/heartbeat.json", False),
        ("watcher/alerts.jsonl", False),
        ("watcher/gate_state.json", False),
    ]
    dest_root.mkdir(parents=True, exist_ok=True)
    skip_names = {".tmp", "__pycache__", "quant.log", "spot_exp.log", "stdout.log",
                  "quant.python.pid", "spot_exp.python.pid", "quant.stdout.log"}
    for rel, is_dir in pairs:
        src = src_root / rel
        dst = dest_root / rel
        if not src.exists():
            continue
        if is_dir:
            dst.mkdir(parents=True, exist_ok=True)
            for f in src.iterdir():
                if f.name in skip_names or f.suffix in {".pyc", ".pid", ".tmp"}:
                    continue
                if f.is_file():
                    dst.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(f, dst / f.name)
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            if src.name == "alerts.jsonl" and src.stat().st_size > 250_000:
                data = src.read_bytes()[-200_000:]
                cut = data.find(b"\n")
                dst.write_bytes(data[cut + 1 :] if cut >= 0 else data)
            else:
                shutil.copy2(src, dst)


def run_set(kind: str) -> dict:
    t0 = time.time()
    if kind == "spot":
        candidates = [HERE / "spot_exp.py", Path("/workspace/spot_exp/spot_exp.py"), paths.spot_exp_dir() / "spot_exp.py"]
        script = next((c for c in candidates if c.exists()), candidates[0])
        cmd = [sys.executable, "-u", str(script), "--once"]
    else:
        script = HERE / "quant.py"
        cmd = [sys.executable, "-u", str(script), "--once", "--profile", kind]
    log(f"start set={kind}")
    proc = subprocess.run(cmd, cwd=str(HERE), text=True, capture_output=True)
    out = TOKEN_PAT.sub("[redacted]", (proc.stdout or "")[-4000:])
    err = TOKEN_PAT.sub("[redacted]", (proc.stderr or "")[-2000:])
    rec = {"set": kind, "rc": proc.returncode, "seconds": round(time.time() - t0, 2)}
    if proc.returncode != 0:
        log(f"set={kind} FAILED rc={proc.returncode} stderr={err[-500:]}")
        rec["stderr"] = err[-800:]
    else:
        log(f"set={kind} ok in {rec['seconds']}s")
        rec["stdout_tail"] = out[-800:]
    return rec


def git_commit_push(repo: Path, message: str, retries: int = 5) -> str:
    run(["git", "add", "-A"], cwd=repo)
    st = run(["git", "status", "--porcelain"], cwd=repo, check=False)
    if not (st.stdout or "").strip():
        log("no changes to commit")
        return "unchanged"
    run(["git", "-c", "user.name=paper-trade bot", "-c",
         "user.email=41898282+github-actions[bot]@users.noreply.github.com",
         "commit", "-m", message], cwd=repo)
    last = ""
    for i in range(retries):
        pull = run(["git", "pull", "--rebase", "--autostash", "origin", "data"], cwd=repo, check=False)
        if pull.returncode != 0:
            last = pull.stderr
            log(f"pull --rebase attempt {i+1} failed: {last[-200:]}")
            time.sleep(1 + i)
            continue
        push = run(["git", "push", "origin", "data"], cwd=repo, check=False)
        if push.returncode == 0:
            sha = run(["git", "rev-parse", "--short", "HEAD"], cwd=repo).stdout.strip()
            log(f"pushed data branch sha={sha}")
            return sha
        last = push.stderr
        log(f"push attempt {i+1} failed: {last[-200:]}")
        time.sleep(1 + i)
    raise RuntimeError(f"commit/push failed after retries: {last[-300:]}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["auto", "shadow", "live"], default=os.environ.get("PAPER_CYCLE_MODE", "auto"))
    ap.add_argument("--data-repo", default="", help="Checkout of the data branch (Actions)")
    ap.add_argument("--seed-from", default="", help="Source tree to copy ledgers from (usually the box /workspace)")
    ap.add_argument("--skip-commit", action="store_true")
    ap.add_argument("--sets", default="4h,1h,15m,spot")
    a = ap.parse_args()

    writer = os.environ.get("PAPER_WRITER") or "github"
    data_repo = Path(a.data_repo) if a.data_repo else None

    if a.mode == "auto":
        lock_doc = {}
        if data_repo:
            for cand in (data_repo / "data" / "writer_lock.json", data_repo / "data" / "ledgers" / "trading-journal" / "writer_lock.json"):
                if cand.exists():
                    try:
                        lock_doc = json.loads(cand.read_text(encoding="utf-8"))
                        break
                    except Exception:
                        pass
        w = str(lock_doc.get("writer") or "box").strip().lower()
        if w == "github":
            a.mode = "live"
        elif data_repo and ((data_repo / "data" / "shadow" / "ledgers" / "trading-journal").exists() or (data_repo / "data" / "ledgers" / "trading-journal").exists()):
            a.mode = "shadow"
        else:
            log("auto mode: writer is not github and no shadow tree; skipping")
            return 0
        log(f"auto mode resolved to {a.mode} (lock_writer={w or 'box'})")

    if data_repo:
        if a.mode == "shadow":
            dest = data_repo / "data" / "shadow" / "ledgers"
            live = data_repo / "data" / "ledgers"
            if a.seed_from:
                seed_tree(Path(a.seed_from), dest)
            elif not (dest / "trading-journal").exists() and (live / "trading-journal").exists():
                seed_tree(live, dest)
        else:
            dest = data_repo / "data" / "ledgers"
            if a.seed_from:
                seed_tree(Path(a.seed_from), dest)
        # Prefer alerts from the live data branch copy if shadow is missing them.
        alerts_live = data_repo / "data" / "alerts_tail.jsonl"
        if alerts_live.exists():
            dest.joinpath("watcher").mkdir(parents=True, exist_ok=True)
            shutil.copy2(alerts_live, dest / "watcher" / "alerts.jsonl")
    else:
        dest = Path(os.environ.get("PAPER_ROOT", "/tmp/paper-shadow"))
        if a.seed_from:
            seed_tree(Path(a.seed_from), dest)

    dest.mkdir(parents=True, exist_ok=True)
    set_paper_env(dest, writer=writer, mode=a.mode)

    lock_writer = paths.current_writer()
    if a.mode == "live" and lock_writer != writer:
        log(f"refusing live run: writer_lock={lock_writer} expected={writer}")
        return 2

    results = []
    wanted = [s.strip() for s in a.sets.split(",") if s.strip() in SETS]
    t0 = time.time()
    for kind in wanted:
        try:
            results.append(run_set(kind))
        except Exception:
            log(f"set={kind} exception {traceback.format_exc()[-500:]}")
            results.append({"set": kind, "rc": 1, "error": "exception"})

    try:
        sys.path.insert(0, str(HERE))
        sys.path.insert(0, str(Path("/workspace/trading-journal")))
        sys.path.insert(0, str(paths.journal()))
        import lifetime  # noqa
        lifetime.rebuild()
        log("lifetime_stats rebuilt")
    except Exception as e:
        log(f"lifetime rebuild failed: {type(e).__name__}: {e}")

    report = {
        "mode": a.mode,
        "writer": writer,
        "lock_writer": paths.current_writer(),
        "time": now_myt(),
        "seconds": round(time.time() - t0, 2),
        "root": str(dest),
        "results": [{"set": r.get("set"), "rc": r.get("rc"), "seconds": r.get("seconds")} for r in results],
        "paper_only": True,
    }
    snap = snapshot.build_all()
    if data_repo:
        if a.mode == "shadow":
            out_dir = data_repo / "data" / "shadow"
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            (out_dir / "data_paper.json").write_text(json.dumps(snap["data_paper"], ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            (out_dir / "watch_state_paper.json").write_text(json.dumps(snap["watch_state"], ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        else:
            snapshot.patch_data_json(data_repo / "data" / "data.json", snap["data_paper"])
            snapshot.merge_watch_state(data_repo / "data" / "watch_state.json", snap["watch_state"])
            # Keep a copy of the lock in data/ as well as inside ledgers.
            lock_src = dest / "trading-journal" / "writer_lock.json"
            if lock_src.exists():
                shutil.copy2(lock_src, data_repo / "data" / "writer_lock.json")
        if not a.skip_commit:
            git_commit_push(data_repo, f"paper {a.mode} cycle {now_myt()}")
    else:
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "cycle_report.json").write_text(json.dumps({**report, "data_paper": snap["data_paper"]}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    failed = [r for r in results if r.get("rc")]
    log(f"done mode={a.mode} failed={len(failed)} of {len(results)} in {report['seconds']}s")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
