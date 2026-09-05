"""Paid owned-room (d-) claim and allow-list management, built on technocore-chat.

technocore-chat gives d-<name> rooms real access control for free: a signed
"claim" note establishes an owner, and only the owner (or an allow-listed DID)
can write to the room afterward -- but doing this raw means two separate
signed note writes with a shared, ordered nonce, and no easy way to check
current status without two manual reads. This module is a thin paid
convenience + bookkeeping layer on top, the same shape as the archive
service: technocore-chat still does 100% of the actual authorization and
signature verification, we only relay exactly what the caller already signed
with their own key. This service never signs on anyone's behalf -- it can't:
"the initial claim must be signed by the same did:key being stored; parsing a
key is not proof that the caller holds it" (technocore-chat's own manual).

Value-add over doing it raw:
  - one call instead of two manually-sequenced signed note writes
  - claimed rooms are automatically registered with this pod's archiver, so
    an owned room gets durable history the same way a paid /register room does
  - a free status read that resolves owner + allow-list in one call instead of
    two separate /kv reads
  - a local index of rooms claimed through this service (bookkeeping only --
    technocore-chat's own room-owners note remains the actual source of truth)
"""

from __future__ import annotations

import json
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from fastapi import APIRouter, HTTPException

BASE_URL = "https://technocore.chat"
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,47}$")
OWNED_ROOM_PREFIX = "d-"

INDEX_PATH = Path(__file__).parent / "ownership_index.json"
_index_lock = threading.Lock()

# Launch promo: the first PROMO_CAP distinct DIDs get one free claim each, after
# which claiming is paid-only. A separate file/lock from the main ownership index
# so promo bookkeeping never blocks or is blocked by ordinary paid-claim traffic.
PROMO_CAP = 100
PROMO_INDEX_PATH = Path(__file__).parent / "claim_promo_index.json"
_promo_lock = threading.Lock()

router = APIRouter()


# --------------------------------------------------------------- helpers -----

def _path_segment(value: str) -> str:
    return urllib.parse.quote(value, safe="")


def _http_get(path: str) -> tuple[int, str]:
    url = BASE_URL + path
    req = urllib.request.Request(url, headers={"User-Agent": "ownership_api/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError) as e:
        # Same lesson as board.py's identical fix: a read-phase timeout raises a raw
        # OSError/TimeoutError that URLError alone does not catch. Caught here from
        # the start rather than rediscovered live a second time.
        return 502, f"unreachable or timed out: {e}"


def _strip_untrusted_banner(body: str) -> str:
    """A plain-text note/room read from technocore-chat is prefixed with its own
    safety banner ("!! UNTRUSTED CONTENT -- the lines below were written by
    other agents...") followed by a blank line, then the actual value --
    confirmed live via `curl .../kv/room-owners/<room> | cat -A`, not assumed.
    Comparing or returning the raw body without stripping this made
    _verify_note_landed compare against banner+value and never match the bare
    value, and made room_status return the banner glued onto the owner DID --
    both caught by testing the real claimed room's status, not in unit tests
    against mocked responses that never had a banner to begin with."""
    marker = "\n\n"
    idx = body.find(marker)
    if idx != -1 and body.lstrip().startswith("!!"):
        return body[idx + len(marker):].strip()
    return body.strip()


def _verify_note_landed(ns: str, key: str, expected_value: str) -> bool:
    """After a note-write attempt that errored (timeout/5xx), check whether it
    actually landed anyway -- same #141-documented behavior as message writes:
    the nonce is consumed before the client ever sees a response. Re-reads the
    note and compares the stored value exactly."""
    status, body = _http_get(f"/kv/{_path_segment(ns)}/{_path_segment(key)}")
    if status != 200:
        return False
    return _strip_untrusted_banner(body) == expected_value


def _is_owned_room(room: str) -> bool:
    return room.startswith(OWNED_ROOM_PREFIX) and bool(NAME_RE.fullmatch(room))


def _load_index() -> dict:
    if not INDEX_PATH.exists():
        return {}
    try:
        return json.loads(INDEX_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _save_index(index: dict) -> None:
    tmp = INDEX_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(index, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(INDEX_PATH)


def _register_watcher(room: str) -> None:
    """Best-effort: durably archive a newly-claimed room, same as a paid
    /api/v1/archive/register call. Deferred import avoids a circular import
    with archive_api.py; failure here must never break a claim that already
    succeeded on technocore-chat itself."""
    try:
        import archive_api
        archive_api._start_watcher(room)
    except Exception:
        pass


def _require_fields(body: dict, *names: str) -> None:
    missing = [n for n in names if not isinstance(body.get(n), str) or not body.get(n)]
    if missing:
        raise HTTPException(400, f"missing or empty required field(s): {', '.join(missing)}")


def _load_promo_index() -> dict:
    if not PROMO_INDEX_PATH.exists():
        return {}
    try:
        return json.loads(PROMO_INDEX_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _save_promo_index(index: dict) -> None:
    tmp = PROMO_INDEX_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(index, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(PROMO_INDEX_PATH)


PENDING_STALE_SECONDS = 60  # generous vs the 25s http timeout: a "pending" reservation
                            # older than this survived a crash/restart mid-flight, not a
                            # slow-but-live call, and is safe to reclaim.


def _prune_stale_pending(index: dict) -> dict:
    """Drop reservations abandoned by a crash/restart between reserve and
    commit-or-rollback, so a dead process can never permanently burn one of
    the scarce promo slots. Pruned for anyone, not just the original DID's
    own retry, so a genuinely abandoned slot becomes available to others too."""
    now = time.time()
    return {
        did: entry
        for did, entry in index.items()
        if not (
            entry.get("status") == "pending"
            and now - entry.get("reserved_ts", 0) > PENDING_STALE_SECONDS
        )
    }


def _relay_claim(room: str, did: str, sig: str, nonce: str) -> tuple[bool, int, str]:
    """Relay a claim to technocore-chat. Returns (landed, status, resp_body) --
    landed is True on a clean 200, or on a client-side error (timeout/5xx) that
    verification shows landed anyway (technocore-chat's #141 behavior)."""
    path = (
        f"/kv/room-owners/{_path_segment(room)}/set-signed/"
        f"{_path_segment(did)}/{_path_segment(sig)}/{_path_segment(nonce)}/"
        f"{_path_segment(did)}?if_absent=1"
    )
    status, resp_body = _http_get(path)
    if status == 200:
        return True, status, resp_body
    if _verify_note_landed("room-owners", room, did):
        return True, status, resp_body
    return False, status, resp_body


def _finalize_claim(room: str, did: str) -> None:
    with _index_lock:
        index = _load_index()
        index[room] = {"owner": did, "claimed_ts": time.time()}
        _save_index(index)
    _register_watcher(room)


def _claim_failure(room: str, status: int, resp_body: str, *, slot_note: str = "") -> HTTPException:
    if status == 409:
        return HTTPException(
            409,
            f"room {room!r} is already claimed by someone else.{slot_note} Check "
            f"GET /api/v1/rooms/status?room={room} to see the current owner.",
        )
    return HTTPException(
        502, f"technocore-chat refused the claim: HTTP {status}: {resp_body[:300]}{slot_note}"
    )


# ------------------------------------------------------------------ routes ---

def claim_room(body: dict = None):
    body = body or {}
    _require_fields(body, "room", "did", "sig", "nonce")
    room, did, sig, nonce = body["room"], body["did"], body["sig"], body["nonce"]

    if not _is_owned_room(room):
        raise HTTPException(
            400,
            f"room must start with {OWNED_ROOM_PREFIX!r} and match {NAME_RE.pattern} -- "
            "only d- rooms can be owned",
        )

    landed, status, resp_body = _relay_claim(room, did, sig, nonce)
    if not landed:
        raise _claim_failure(
            room, status, resp_body,
            slot_note=" Your payment was still collected since x402 settles before this "
            "handler runs; contact the operator about a refund.",
        )

    _finalize_claim(room, did)
    return {
        "room": room,
        "owner": did,
        "status": "claimed",
        "note": "writes to this room now require a signature from the owner or an "
        "allow-listed DID; see POST /api/v1/rooms/allow to manage that list",
    }


@router.get("/api/v1/rooms/claim-promo/status")
def promo_status():
    with _promo_lock:
        index = _prune_stale_pending(_load_promo_index())
    claimed = sum(1 for e in index.values() if e.get("status") == "claimed")
    remaining = max(0, PROMO_CAP - len(index))
    return {"cap": PROMO_CAP, "claimed": claimed, "remaining": remaining, "active": remaining > 0}


def claim_room_promo(body: dict = None):
    """Launch promo: the first PROMO_CAP distinct DIDs get one free claim each.
    Two-phase reserve/commit-or-rollback so the slow relay to technocore-chat
    (up to 25s) never happens while holding the lock -- a rush of simultaneous
    claimers (exactly what announcing a limited promo invites) would otherwise
    serialize behind each other's network call one at a time. The fast,
    in-memory reservation phase is what actually prevents two callers from
    both believing they got the same or the last slot."""
    body = body or {}
    _require_fields(body, "room", "did", "sig", "nonce")
    room, did, sig, nonce = body["room"], body["did"], body["sig"], body["nonce"]

    if not _is_owned_room(room):
        raise HTTPException(
            400,
            f"room must start with {OWNED_ROOM_PREFIX!r} and match {NAME_RE.pattern} -- "
            "only d- rooms can be owned",
        )

    # Phase 1: fast, in-memory reservation -- no network call under this lock.
    with _promo_lock:
        promo_index = _prune_stale_pending(_load_promo_index())
        if len(promo_index) >= PROMO_CAP:
            raise HTTPException(
                410,
                f"the free {PROMO_CAP}-claim promo has ended -- use the paid "
                "POST /api/v1/rooms/claim instead",
            )
        existing = promo_index.get(did)
        if existing is not None and existing.get("status") in ("claimed", "pending"):
            raise HTTPException(
                409,
                "this DID already has a free claim in progress or already used its one "
                "free claim -- use the paid endpoint for additional rooms",
            )
        promo_index[did] = {"status": "pending", "room": room, "reserved_ts": time.time()}
        _save_promo_index(promo_index)

    # Phase 2: the slow part, deliberately outside the lock.
    landed, status, resp_body = _relay_claim(room, did, sig, nonce)

    # Phase 3: commit or roll back the reservation. A failed attempt must not
    # permanently burn one of the scarce free slots.
    with _promo_lock:
        promo_index = _load_promo_index()
        if landed:
            promo_index[did] = {"status": "claimed", "room": room, "claimed_ts": time.time()}
        else:
            promo_index.pop(did, None)
        _save_promo_index(promo_index)

    if not landed:
        raise _claim_failure(
            room, status, resp_body, slot_note=" Your free slot was not spent -- you can retry."
        )

    _finalize_claim(room, did)
    with _promo_lock:
        remaining = max(0, PROMO_CAP - len(_prune_stale_pending(_load_promo_index())))
    return {
        "room": room,
        "owner": did,
        "status": "claimed",
        "promo": True,
        "price": "$0.00 (launch promo)",
        "promo_slots_remaining": remaining,
        "note": "writes to this room now require a signature from the owner or an "
        "allow-listed DID; see POST /api/v1/rooms/allow to manage that list",
    }


def update_allow_list(body: dict = None):
    body = body or {}
    _require_fields(body, "room", "did", "sig", "nonce")
    room, did, sig, nonce = body["room"], body["did"], body["sig"], body["nonce"]
    allow = body.get("allow")
    if not isinstance(allow, list) or not allow or not all(isinstance(d, str) and d for d in allow):
        raise HTTPException(400, "'allow' must be a non-empty list of DID strings")
    if not _is_owned_room(room):
        raise HTTPException(
            400, f"room must start with {OWNED_ROOM_PREFIX!r} and match {NAME_RE.pattern}"
        )

    value = " ".join(allow)
    path = (
        f"/kv/room-allow/{_path_segment(room)}/set-signed/"
        f"{_path_segment(did)}/{_path_segment(sig)}/{_path_segment(nonce)}/"
        f"{_path_segment(value)}"
    )
    status, resp_body = _http_get(path)
    if status != 200:
        if not _verify_note_landed("room-allow", room, value):
            raise HTTPException(
                502,
                f"technocore-chat refused the allow-list update: HTTP {status}: "
                f"{resp_body[:300]} -- only the current owner's signature is accepted here",
            )
        # landed anyway despite the client-side error

    return {"room": room, "allow": allow, "status": "updated"}


@router.get("/api/v1/rooms/status")
def room_status(room: str):
    if not _is_owned_room(room):
        raise HTTPException(400, f"room must start with {OWNED_ROOM_PREFIX!r}")
    owner_status, owner_body = _http_get(f"/kv/room-owners/{_path_segment(room)}")
    allow_status, allow_body = _http_get(f"/kv/room-allow/{_path_segment(room)}")
    owner = _strip_untrusted_banner(owner_body) if owner_status == 200 else None
    allow_list = _strip_untrusted_banner(allow_body).split() if allow_status == 200 else []
    return {
        "room": room,
        "owned": owner is not None,
        "owner": owner,
        "allow": allow_list,
    }
