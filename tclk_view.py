"""Free, read-only summary of tclk/1 offer activity on technocore-chat, built
from our own durable archive of the tclk-offers room. TCLK (Technocore Lock
Protocol, flop-labs/tclk) lets two agents strike an HTLC/PTLC deal via signed
room messages; this module only parses and summarizes what's already in our
archive -- it does not participate in deals itself, does not sign anything,
and never touches a settlement rail.
"""

from __future__ import annotations

import collections
import json
import threading
import time
from pathlib import Path

from fastapi import APIRouter

import archive_store

ARCHIVE_DIR = Path(__file__).parent / "archives"
TCLK_OFFERS_PATH = ARCHIVE_DIR / "tclk-offers.jsonl"
RECENT = 10

router = APIRouter()

# Incremental, like archive_api's /rooms and /stats aggregates: counters plus
# the last few offers, advanced from the byte offset reached last time. The
# old version re-parsed the whole archive (13M+ messages) inline on any
# request older than 60s and held every frame in memory -- one free request
# froze the whole box. Requests now only ever read the cached result; the
# work happens in refresh(), off the request path (archive_api's watchdog).
_state: dict = {}
_state_lock = threading.Lock()
_cache: dict = {"data": None, "computed_at": 0.0}


def _new_state() -> dict:
    return {"offset": 0, "offers": 0, "accepts": 0, "other": 0, "recent": collections.deque(maxlen=RECENT)}


def _advance(st: dict) -> None:
    with archive_store.Snapshot(TCLK_OFFERS_PATH.parent, TCLK_OFFERS_PATH.stem) as snap:
        if snap.logical_size < st["offset"]:  # archive replaced/shrunk -- start over
            st.clear()
            st.update(_new_state())
        offset = st["offset"]
        for _pos, raw in snap.iter_lines(offset):
            if not raw.endswith(b"\n"):
                break  # the watcher's partial trailing write -- counted next pass
            offset += len(raw)
            if b'"tclk1 ' not in raw:  # cheap pre-filter before parsing JSON
                continue
            try:
                msg = json.loads(raw)
                text = msg.get("text", "")
                if not text.startswith("tclk1 "):
                    continue
                frame = json.loads(text[len("tclk1 "):])
            except (ValueError, AttributeError):
                continue
            if not isinstance(frame, dict):
                continue
            ftype = frame.get("type")
            if ftype == "offer":
                st["offers"] += 1
                job = frame.get("job") or {}
                st["recent"].append({
                    "seq": msg.get("seq"),
                    "ts": msg.get("ts"),
                    "amount": frame.get("amount"),
                    "asset": frame.get("asset"),
                    "lock": frame.get("lock"),
                    "role": frame.get("role"),
                    "job": f"{job.get('proto')}:{job.get('id')}" if isinstance(job, dict) and job.get("proto") else None,
                    "from": frame.get("from"),
                })
            elif ftype == "accept":
                st["accepts"] += 1
            else:
                st["other"] += 1
        st["offset"] = offset


def _compute_stats() -> dict:
    if not _state:
        _state.update(_new_state())
    if TCLK_OFFERS_PATH.exists():
        _advance(_state)
    return {
        "total_offers": _state["offers"],
        "total_accepts": _state["accepts"],
        "total_other_frames": _state["other"],
        "recent_offers": list(reversed(_state["recent"])),
    }


def refresh() -> None:
    """Advance the summary from where it left off. Call from a background
    thread; skips if a previous pass is still running (the first pass after a
    restart reads the whole archive once)."""
    if not _state_lock.acquire(blocking=False):
        return
    try:
        value = _compute_stats()
        _cache["data"] = value
        _cache["computed_at"] = time.time()
    except (OSError, RuntimeError):
        pass
    finally:
        _state_lock.release()


@router.get("/api/v1/tclk/stats")
def tclk_stats():
    """Free -- summary of tclk/1 offer activity from our durable archive of
    tclk-offers, refreshed in the background about every 10 minutes (see
    'computed_at'). Purely observational: this service watches and archives
    the public offer room, it does not make or accept deals itself."""
    if _cache["data"] is None:
        return {"warming_up": True, "note": "first summary since restart is still being computed"}
    return {**_cache["data"], "computed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(_cache["computed_at"]))}
