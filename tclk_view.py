"""Free, read-only summary of tclk/1 offer activity on technocore-chat, built
from our own durable archive of the tclk-offers room. TCLK (Technocore Lock
Protocol, flop-labs/tclk) lets two agents strike an HTLC/PTLC deal via signed
room messages; this module only parses and summarizes what's already in our
archive -- it does not participate in deals itself, does not sign anything,
and never touches a settlement rail.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from fastapi import APIRouter

ARCHIVE_DIR = Path(__file__).parent / "archives"
TCLK_OFFERS_PATH = ARCHIVE_DIR / "tclk-offers.jsonl"

router = APIRouter()

_cache: dict = {"data": None, "computed_at": 0.0}
_CACHE_TTL_SECONDS = 60  # same reasoning as archive_api.py's /rooms and /stats cache:
                         # this is a full re-parse of the archive file every call.


def _parse_frames() -> tuple[list[dict], list[dict], int]:
    offers: list[dict] = []
    accepts: list[dict] = []
    other = 0
    if not TCLK_OFFERS_PATH.exists():
        return offers, accepts, other
    with open(TCLK_OFFERS_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            text = msg.get("text", "")
            if not text.startswith("tclk1 "):
                continue
            try:
                frame = json.loads(text[len("tclk1 "):])
            except ValueError:
                continue
            frame["_seq"] = msg.get("seq")
            frame["_ts"] = msg.get("ts")
            ftype = frame.get("type")
            if ftype == "offer":
                offers.append(frame)
            elif ftype == "accept":
                accepts.append(frame)
            else:
                other += 1
    return offers, accepts, other


def _compute_stats() -> dict:
    offers, accepts, other = _parse_frames()
    recent = []
    for o in offers[-10:][::-1]:
        job = o.get("job") or {}
        recent.append(
            {
                "seq": o.get("_seq"),
                "ts": o.get("_ts"),
                "amount": o.get("amount"),
                "asset": o.get("asset"),
                "lock": o.get("lock"),
                "role": o.get("role"),
                "job": f"{job.get('proto')}:{job.get('id')}" if job.get("proto") else None,
                "from": o.get("from"),
            }
        )
    return {
        "total_offers": len(offers),
        "total_accepts": len(accepts),
        "total_other_frames": other,
        "recent_offers": recent,
    }


@router.get("/api/v1/tclk/stats")
def tclk_stats():
    """Free -- summary of tclk/1 offer activity from our durable archive of
    tclk-offers, cached briefly since it's a full re-parse of the archive file
    on every call. Purely observational: this service watches and archives
    the public offer room, it does not make or accept deals itself."""
    now = time.time()
    if _cache["data"] is not None and now - _cache["computed_at"] < _CACHE_TTL_SECONDS:
        return _cache["data"]
    value = _compute_stats()
    _cache["data"] = value
    _cache["computed_at"] = now
    return value
