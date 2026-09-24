#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""Cross-room DID activity index for technocore-archive, stored in SQLite.

Build (incremental -- only reads bytes appended since the last run):
    uv run did_activity.py build --archive-dir archives --db did_activity.db

archive_api.py imports this module for the query side only. The build always
runs as its own detached process, never inside the API process: a first build
is a full pass over every archive file (~25GB), same reason the tclk index
rebuild is kept out-of-process.

Only signed senders (did:key:...) are indexed -- unsigned nicks are
self-asserted and prove nothing, so counting them as identities would be
meaningless.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sqlite3
import sys
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path

FP_CAP = 16              # distinct text fingerprints tracked per DID
SAMPLE_CHARS = 120       # example text stored per repeated fingerprint
CHECKPOINT_EVERY = 5000  # lines between seq->byte-offset checkpoints
FLUSH_EVERY = 200_000    # lines between commits (crash-safe resume points)
# In-memory LRU sizes, kept across commits: the busiest senders are touched in
# nearly every chunk, and re-reading their state from a multi-GB database on
# every commit is what made the first version of this builder disk-bound.
ROW_CACHE_MAX = 200_000
FP_CACHE_MAX = 100_000
ID_CACHE_MAX = 1_000_000
MAX_SKIP_LINES = 100_000 # read_range() gives up past this many lines without reaching `since`

# v2 storage layout. v1 repeated each ~56-byte DID string (and each room name)
# in every row of every table, stored a text sample for every fingerprint, and
# used rowid tables that kept composite keys twice -- it reached 6GB+ for the
# first 7.6GB of archive on the live box and went disk-bound. v2 interns DIDs
# and rooms as small integers, uses integer fingerprints, keeps samples only
# for fingerprints that actually repeat, and stores the hour histogram
# sparsely. A v1 file is dropped and rebuilt on first open (see _migrate).
SCHEMA_VERSION = "2"

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS room_progress(room TEXT PRIMARY KEY, offset INTEGER NOT NULL, last_seq INTEGER);
CREATE TABLE IF NOT EXISTS rooms(id INTEGER PRIMARY KEY, room TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS dids(id INTEGER PRIMARY KEY, did TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS did_room(
    did_id INTEGER NOT NULL, room_id INTEGER NOT NULL,
    msg_count INTEGER NOT NULL, first_ts TEXT, last_ts TEXT,
    first_seq INTEGER, last_seq INTEGER, text_chars INTEGER NOT NULL,
    gap_n INTEGER NOT NULL, gap_mean REAL NOT NULL, gap_m2 REAL NOT NULL,
    last_epoch REAL, hours TEXT NOT NULL,
    PRIMARY KEY(did_id, room_id)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS did_room_room_first ON did_room(room_id, first_ts);
CREATE TABLE IF NOT EXISTS did_first(did_id INTEGER PRIMARY KEY, first_ts TEXT NOT NULL, first_room_id INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS did_first_ts ON did_first(first_ts);
CREATE TABLE IF NOT EXISTS did_fp(did_id INTEGER NOT NULL, fp INTEGER NOT NULL, count INTEGER NOT NULL, sample TEXT,
    PRIMARY KEY(did_id, fp)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS did_extra(did_id INTEGER PRIMARY KEY, untracked_msgs INTEGER NOT NULL, protocol_msgs INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS seq_offsets(room_id INTEGER NOT NULL, seq INTEGER NOT NULL, offset INTEGER NOT NULL,
    PRIMARY KEY(room_id, seq)) WITHOUT ROWID;
"""
_DATA_TABLES = ("room_progress", "rooms", "dids", "did_room", "did_first", "did_fp", "did_extra", "seq_offsets")

_DIGIT_TOKEN_RE = re.compile(r"\b\w*\d\w*\b")
_HEXISH_RE = re.compile(r"\b[0-9a-f]{6,}\b")
_NON_WORD_RE = re.compile(r"[^\w#]+")
# Structured protocol traffic (tclk frames and sonnet ballots are JSON; kibble
# is "JOB v1 | ..." / "ATTEST v1 | ..."). It's templated by design, so it's
# counted separately and kept out of the filler-repetition signal -- otherwise
# every legitimate trading/voting agent would read as a filler bot.
_PROTOCOL_RE = re.compile(r"^\s*(?:[\[{]|(?:JOB|DELIVER|RESULT|ATTEST)\s+v\d|tclk/)", re.IGNORECASE)


def fingerprint(text: str) -> int:
    """Template fingerprint: case-folded, any token containing a digit or
    looking like a 6+ char hex id collapsed to '#', punctuation dropped -- so
    "still around network running smooth! [1a41f4]" and "... [edccef]" (the
    filler-bot pattern seen live) collide. Returns a 63-bit integer."""
    t = _DIGIT_TOKEN_RE.sub("#", _HEXISH_RE.sub("#", (text or "").lower()))
    t = _NON_WORD_RE.sub(" ", t).strip()[:200]
    return int.from_bytes(hashlib.sha1(t.encode("utf-8")).digest()[:8], "big") >> 1


def parse_ts(ts):
    """Returns (epoch_seconds, 'YYYY-MM-DDTHH:MM:SSZ', utc_hour) or (None, None, None).
    The normalized string sorts lexicographically == chronologically, which the
    identity-rate bucketing relies on."""
    if not isinstance(ts, str) or not ts:
        return None, None, None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None, None, None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.timestamp(), dt.strftime("%Y-%m-%dT%H:%M:%SZ"), dt.hour


def _hours_encode(hours: list[int]) -> str:
    return ",".join(f"{h}:{c}" for h, c in enumerate(hours) if c)


def _hours_decode(s: str) -> list[int]:
    hours = [0] * 24
    for part in (s or "").split(","):
        if part:
            h, c = part.split(":")
            hours[int(h)] = int(c)
    return hours


def connect_writer(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-262144")            # 256MB page cache (default is 2MB)
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA journal_size_limit=268435456")  # truncate the WAL back to <=256MB after checkpoints
    _migrate(conn)
    conn.executescript(SCHEMA)
    conn.execute("INSERT OR REPLACE INTO meta VALUES ('schema_version', ?)", (SCHEMA_VERSION,))
    conn.commit()
    return conn


def _migrate(conn: sqlite3.Connection) -> None:
    has_meta = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").fetchone()
    if not has_meta:
        return
    row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    if row and row[0] == SCHEMA_VERSION:
        return
    for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
        conn.execute(f'DROP TABLE IF EXISTS "{name}"')
    conn.commit()


def connect_reader(db_path, timeout: float = 10) -> sqlite3.Connection:
    # Deliberately not mode=ro: a read-only connection can't create the WAL
    # -shm file, and the builder's own close may have removed it.
    conn = sqlite3.connect(str(db_path), timeout=timeout)
    conn.row_factory = sqlite3.Row
    return conn


# ------------------------------------------------------------------ build --

class _NeedsFullRebuild(Exception):
    pass


class _Builder:
    """Keeps recently-touched state in LRU caches that survive across commits,
    and only writes entries that actually changed. Row/fingerprint entries are
    only evicted right after a commit, when every cached entry is clean, so
    eviction never loses an unwritten change. (DID/room ids are immutable once
    assigned, so their cache can evict at any time.)"""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.did_ids: OrderedDict[str, int] = OrderedDict()
        self.room_ids: dict[str, int] = {r["room"]: r["id"] for r in conn.execute("SELECT id, room FROM rooms")}
        self.rows: OrderedDict[tuple[int, int], dict] = OrderedDict()
        self.fps: OrderedDict[int, dict] = OrderedDict()
        self.dirty_rows: set[tuple[int, int]] = set()
        self.dirty_fps: set[int] = set()
        self.first: dict[int, tuple[str, int]] = {}
        self.offsets: list[tuple[int, int, int]] = []

    def room_id(self, room: str) -> int:
        i = self.room_ids.get(room)
        if i is None:
            i = self.conn.execute("INSERT INTO rooms(room) VALUES (?)", (room,)).lastrowid
            self.room_ids[room] = i
        return i

    def _did_id(self, did: str) -> int:
        i = self.did_ids.get(did)
        if i is not None:
            self.did_ids.move_to_end(did)
            return i
        row = self.conn.execute("SELECT id FROM dids WHERE did=?", (did,)).fetchone()
        i = row[0] if row else self.conn.execute("INSERT INTO dids(did) VALUES (?)", (did,)).lastrowid
        self.did_ids[did] = i
        if len(self.did_ids) > ID_CACHE_MAX:
            self.did_ids.popitem(last=False)
        return i

    def _row(self, did_id: int, room_id: int) -> dict:
        key = (did_id, room_id)
        r = self.rows.get(key)
        if r is None:
            got = self.conn.execute("SELECT * FROM did_room WHERE did_id=? AND room_id=?", key).fetchone()
            if got:
                r = dict(got)
                r["hours"] = _hours_decode(r["hours"])
            else:
                r = {"did_id": did_id, "room_id": room_id, "msg_count": 0, "first_ts": None, "last_ts": None,
                     "first_seq": None, "last_seq": None, "text_chars": 0, "gap_n": 0,
                     "gap_mean": 0.0, "gap_m2": 0.0, "last_epoch": None, "hours": [0] * 24}
            self.rows[key] = r
        else:
            self.rows.move_to_end(key)
        self.dirty_rows.add(key)
        return r

    def _fp(self, did_id: int) -> dict:
        f = self.fps.get(did_id)
        if f is None:
            fp = {row["fp"]: [row["count"], row["sample"]]
                  for row in self.conn.execute("SELECT fp, count, sample FROM did_fp WHERE did_id=?", (did_id,))}
            ex = self.conn.execute("SELECT untracked_msgs, protocol_msgs FROM did_extra WHERE did_id=?",
                                   (did_id,)).fetchone()
            f = {"fp": fp, "overflow": ex[0] if ex else 0, "protocol": ex[1] if ex else 0}
            self.fps[did_id] = f
        else:
            self.fps.move_to_end(did_id)
        self.dirty_fps.add(did_id)
        return f

    def add(self, room_id: int, msg: dict) -> None:
        frm = msg.get("from") or ""
        if not frm.startswith("did:key:"):
            return
        did_id = self._did_id(frm)
        epoch, ts_norm, hour = parse_ts(msg.get("ts"))
        text = msg.get("text") or ""
        seq = msg.get("seq")

        r = self._row(did_id, room_id)
        r["msg_count"] += 1
        r["text_chars"] += len(text)
        if ts_norm is not None:
            if r["first_ts"] is None or ts_norm < r["first_ts"]:
                r["first_ts"] = ts_norm
            if r["last_ts"] is None or ts_norm > r["last_ts"]:
                r["last_ts"] = ts_norm
            r["hours"][hour] += 1
            cur = self.first.get(did_id)
            if cur is None or ts_norm < cur[0]:
                self.first[did_id] = (ts_norm, room_id)
        if isinstance(seq, int):
            if r["first_seq"] is None:
                r["first_seq"] = seq
            r["last_seq"] = seq
        if epoch is not None:
            if r["last_epoch"] is not None and epoch >= r["last_epoch"]:
                gap = epoch - r["last_epoch"]
                r["gap_n"] += 1
                d = gap - r["gap_mean"]
                r["gap_mean"] += d / r["gap_n"]
                r["gap_m2"] += d * (gap - r["gap_mean"])
            r["last_epoch"] = epoch

        f = self._fp(did_id)
        if _PROTOCOL_RE.match(text):
            f["protocol"] += 1
            return
        h = fingerprint(text)
        e = f["fp"].get(h)
        if e is not None:
            e[0] += 1
            if e[1] is None:  # sample isn't persisted until a fingerprint repeats
                e[1] = text[:SAMPLE_CHARS]
        elif len(f["fp"]) < FP_CAP:
            f["fp"][h] = [1, text[:SAMPLE_CHARS]]
        else:
            f["overflow"] += 1

    def flush(self, room: str, offset: int, last_seq) -> None:
        c = self.conn
        c.executemany(
            "INSERT OR REPLACE INTO did_room VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(r["did_id"], r["room_id"], r["msg_count"], r["first_ts"], r["last_ts"], r["first_seq"],
              r["last_seq"], r["text_chars"], r["gap_n"], r["gap_mean"], r["gap_m2"],
              r["last_epoch"], _hours_encode(r["hours"]))
             for r in (self.rows[k] for k in self.dirty_rows)],
        )
        fp_rows, ex_rows = [], []
        for did_id in self.dirty_fps:
            f = self.fps[did_id]
            fp_rows.extend((did_id, h, v[0], v[1] if v[0] > 1 else None) for h, v in f["fp"].items())
            if f["overflow"] or f["protocol"]:
                ex_rows.append((did_id, f["overflow"], f["protocol"]))
        c.executemany("INSERT OR REPLACE INTO did_fp VALUES (?,?,?,?)", fp_rows)
        c.executemany("INSERT OR REPLACE INTO did_extra VALUES (?,?,?)", ex_rows)
        c.executemany(
            "INSERT INTO did_first(did_id, first_ts, first_room_id) VALUES (?,?,?) "
            "ON CONFLICT(did_id) DO UPDATE SET first_ts=excluded.first_ts, first_room_id=excluded.first_room_id "
            "WHERE excluded.first_ts < did_first.first_ts",
            [(did_id, ts, rid) for did_id, (ts, rid) in self.first.items()],
        )
        c.executemany("INSERT OR IGNORE INTO seq_offsets VALUES (?,?,?)", self.offsets)
        # Progress is committed in the same transaction as the data it covers,
        # so a crash mid-build resumes exactly where the last commit left off
        # without double-counting anything.
        c.execute(
            "INSERT INTO room_progress(room, offset, last_seq) VALUES (?,?,?) "
            "ON CONFLICT(room) DO UPDATE SET offset=excluded.offset, "
            "last_seq=COALESCE(excluded.last_seq, room_progress.last_seq)",
            (room, offset, last_seq),
        )
        c.commit()
        self.dirty_rows.clear()
        self.dirty_fps.clear()
        self.first.clear()
        self.offsets.clear()
        while len(self.rows) > ROW_CACHE_MAX:  # everything is clean right after a commit
            self.rows.popitem(last=False)
        while len(self.fps) > FP_CACHE_MAX:
            self.fps.popitem(last=False)


def _process_room(b: _Builder, room: str, path: Path, start: int) -> int:
    size = path.stat().st_size
    if size < start:
        raise _NeedsFullRebuild(room)
    if size == start:
        return 0
    room_id = b.room_id(room)
    added = pending = 0
    offset = start
    since_ckpt = CHECKPOINT_EVERY  # checkpoint the first line of every run
    last_seq = None
    with open(path, "rb") as f:
        f.seek(start)
        for raw in f:
            if not raw.endswith(b"\n"):
                break  # the live watcher's partial trailing write -- next run picks it up
            line_start = offset
            offset += len(raw)
            s = raw.strip()
            if not s:
                continue
            try:
                msg = json.loads(s)
            except ValueError:
                continue
            seq = msg.get("seq")
            if isinstance(seq, int):
                last_seq = seq
                if since_ckpt >= CHECKPOINT_EVERY:
                    b.offsets.append((room_id, seq, line_start))
                    since_ckpt = 0
                since_ckpt += 1
            b.add(room_id, msg)
            added += 1
            pending += 1
            if pending >= FLUSH_EVERY:
                b.flush(room, offset, last_seq)
                pending = 0
    b.flush(room, offset, last_seq)
    return added


def _reset(conn: sqlite3.Connection) -> None:
    for t in _DATA_TABLES:
        conn.execute(f"DELETE FROM {t}")
    conn.execute("INSERT OR REPLACE INTO meta VALUES ('ready', '0')")
    conn.commit()


def _acquire_lock(db_path: Path):
    try:
        import fcntl
    except ImportError:  # non-Linux dev machine -- no concurrent builds there anyway
        return object()
    fh = open(str(db_path) + ".lock", "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        return None
    return fh


def build(archive_dir: Path, db_path: Path, full: bool = False) -> dict:
    lock = _acquire_lock(db_path)
    if lock is None:
        return {"skipped": "another build is already running"}
    conn = connect_writer(db_path)
    try:
        if full:
            _reset(conn)
        for _attempt in range(2):
            progress = {r["room"]: r["offset"] for r in conn.execute("SELECT room, offset FROM room_progress")}
            b = _Builder(conn)
            added = 0
            try:
                for path in sorted(archive_dir.glob("*.jsonl")):
                    added += _process_room(b, path.stem, path, progress.get(path.stem, 0))
                break
            except _NeedsFullRebuild as e:
                # An archive file got shorter than what's already indexed
                # (restored/replaced) -- per-room deletes can't undo its share of
                # the DID-level aggregates, so start over from scratch.
                print(f"archive {e} shrank below its indexed offset -- full rebuild", flush=True)
                _reset(conn)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        conn.executemany("INSERT OR REPLACE INTO meta VALUES (?,?)",
                         [("generated_at", now), ("ready", "1")])
        conn.commit()
        return {"messages_indexed": added, "generated_at": now}
    finally:
        conn.close()


# ------------------------------------------------------------------ query --

def is_ready(db_path, timeout: float = 10) -> bool | None:
    """True/False, or None when the database couldn't be read right now (busy
    or mid-write) -- callers should keep their last known answer rather than
    treat a transient read failure as "not built"."""
    if not Path(db_path).exists():
        return False
    try:
        conn = connect_reader(db_path, timeout)
        try:
            ver = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
            if not ver or ver[0] != SCHEMA_VERSION:
                return False  # an old-layout file the queries below can't read
            row = conn.execute("SELECT value FROM meta WHERE key='ready'").fetchone()
            return bool(row) and row[0] == "1"
        finally:
            conn.close()
    except sqlite3.Error:
        return None


def generated_at(conn) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key='generated_at'").fetchone()
    return row[0] if row else None


def _did_id_of(conn, did: str) -> int | None:
    row = conn.execute("SELECT id FROM dids WHERE did=?", (did,)).fetchone()
    return row[0] if row else None


def _room_id_of(conn, room: str) -> int | None:
    row = conn.execute("SELECT id FROM rooms WHERE room=?", (room,)).fetchone()
    return row[0] if row else None


def _merge_gaps(rows) -> tuple[int, float, float]:
    n, mean, m2 = 0, 0.0, 0.0
    for r in rows:
        nb, mb, m2b = r["gap_n"], r["gap_mean"], r["gap_m2"]
        if not nb:
            continue
        tot = n + nb
        d = mb - mean
        mean += d * nb / tot
        m2 += m2b + d * d * n * nb / tot
        n = tot
    return n, mean, m2


def _epoch(ts_norm: str) -> float:
    return datetime.strptime(ts_norm, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()


def profile(conn, did: str) -> dict | None:
    did_id = _did_id_of(conn, did)
    if did_id is None:
        return None
    rows = conn.execute(
        "SELECT d.*, r.room AS room FROM did_room d JOIN rooms r ON r.id = d.room_id "
        "WHERE d.did_id=? ORDER BY r.room", (did_id,)).fetchall()
    if not rows:
        return None
    total = sum(r["msg_count"] for r in rows)
    chars = sum(r["text_chars"] for r in rows)
    hours = [0] * 24
    for r in rows:
        for i, c in enumerate(_hours_decode(r["hours"])):
            hours[i] += c
    n, mean, m2 = _merge_gaps(rows)
    stdev = math.sqrt(m2 / n) if n > 1 else None
    firsts = [r["first_ts"] for r in rows if r["first_ts"]]
    lasts = [r["last_ts"] for r in rows if r["last_ts"]]
    first_seen = min(firsts) if firsts else None
    last_seen = max(lasts) if lasts else None
    span_days = (_epoch(last_seen) - _epoch(first_seen)) / 86400 if first_seen and last_seen else None
    rooms = sorted(
        ({"room": r["room"], "messages": r["msg_count"], "first_ts": r["first_ts"],
          "last_ts": r["last_ts"], "first_seq": r["first_seq"], "last_seq": r["last_seq"]}
         for r in rows),
        key=lambda x: (-x["messages"], x["room"]),
    )
    return {
        "messages": total,
        "rooms_active": len(rows),
        "first_seen": first_seen,
        "last_seen": last_seen,
        "active_span_days": round(span_days, 2) if span_days is not None else None,
        "messages_per_day": round(total / max(span_days, 1.0), 1) if span_days is not None else None,
        "avg_text_length": round(chars / total, 1) if total else 0,
        "posting_hours_utc": hours,
        "cadence": {
            "gaps_measured": n,
            "mean_gap_seconds": round(mean, 1) if n else None,
            "stdev_gap_seconds": round(stdev, 1) if stdev is not None else None,
            "coefficient_of_variation": round(stdev / mean, 3) if stdev is not None and mean > 0 else None,
        },
        "rooms": rooms,
    }


def bot_check(conn, did: str) -> dict | None:
    """Heuristic templated-filler signal. technocore-chat is an agent network --
    nearly every participant is automated -- so the question this answers isn't
    'is this a bot', it's 'does this identity post templated filler': the same
    few lines over and over, on a fixed timer, around the clock."""
    p = profile(conn, did)
    if p is None:
        return None
    did_id = _did_id_of(conn, did)
    fps = conn.execute("SELECT count, sample FROM did_fp WHERE did_id=? ORDER BY count DESC, fp",
                       (did_id,)).fetchall()
    ex = conn.execute("SELECT untracked_msgs, protocol_msgs FROM did_extra WHERE did_id=?", (did_id,)).fetchone()
    untracked = ex[0] if ex else 0
    protocol_msgs = ex[1] if ex else 0
    tracked = sum(r["count"] for r in fps)
    total = tracked + untracked  # natural-language messages only
    repeat_ratio = (tracked - len(fps)) / total if total else 0.0
    top = [{"count": r["count"], "share": round(r["count"] / total, 3), "sample": (r["sample"] or "")[:160]}
           for r in fps[:5]] if total else []

    cad = p["cadence"]
    cv = cad["coefficient_of_variation"]
    hours = p["posting_hours_utc"]
    hsum = sum(hours)
    h_norm = (-sum((h / hsum) * math.log(h / hsum) for h in hours if h) / math.log(24)) if hsum else 0.0

    rep_score = min(1.0, repeat_ratio / 0.8)
    cadence_score = (max(0.0, min(1.0, (1.0 - cv) / 0.7))
                     if cv is not None and cad["gaps_measured"] >= 20 else 0.0)
    clock_score = max(0.0, min(1.0, (h_norm - 0.85) / 0.15)) if total >= 50 else 0.0
    score = round(100 * (0.5 * rep_score + 0.25 * cadence_score + 0.25 * clock_score))

    # Repetition is required, not just contributing: a metronomic 24/7 agent
    # with genuinely varied content is a normal agent on this network, not
    # filler. Cadence and round-the-clock posting only strengthen a real
    # templating signal.
    if total < 10:
        verdict = "insufficient_data"
    elif score >= 60 and repeat_ratio >= 0.5:
        verdict = "likely_templated_filler"
    elif score >= 35 and repeat_ratio >= 0.3:
        verdict = "possibly_templated_filler"
    else:
        verdict = "no_strong_filler_signal"

    return {
        "verdict": verdict,
        "filler_score": score,
        "messages": p["messages"],
        "chat_messages_scored": total,
        "protocol_messages_excluded": protocol_msgs,
        "signals": {
            "template_repetition": {
                "repeat_ratio": round(repeat_ratio, 3),
                "distinct_templates_tracked": len(fps),
                "tracking_cap": FP_CAP,
                "messages_beyond_cap": untracked,
                "top_templates": top,
            },
            "cadence": {**cad, "regularity_score": round(cadence_score, 3)},
            "round_the_clock": {
                "active_hours_utc": sum(1 for h in hours if h),
                "hour_entropy_normalized": round(h_norm, 3),
                "score": round(clock_score, 3),
            },
        },
        "rooms_active": p["rooms_active"],
        "first_seen": p["first_seen"],
        "last_seen": p["last_seen"],
    }


def identity_rate(conn, since: str, until: str, bucket: str = "day", room: str | None = None) -> dict:
    """First appearance of each signed DID, bucketed. since/until are
    normalized 'YYYY-MM-DDTHH:MM:SSZ' strings, until exclusive."""
    width = 13 if bucket == "hour" else 10
    if room:
        room_id = _room_id_of(conn, room)
        rows = conn.execute(
            f"SELECT substr(first_ts,1,{width}) AS b, COUNT(*) AS n FROM did_room "
            "WHERE room_id=? AND first_ts>=? AND first_ts<? GROUP BY b", (room_id, since, until)).fetchall()
        prior = conn.execute("SELECT COUNT(*) FROM did_room WHERE room_id=? AND first_ts<?",
                             (room_id, since)).fetchone()[0]
        coverage = conn.execute("SELECT MIN(first_ts) FROM did_room WHERE room_id=?", (room_id,)).fetchone()[0]
    else:
        rows = conn.execute(
            f"SELECT substr(first_ts,1,{width}) AS b, COUNT(*) AS n FROM did_first "
            "WHERE first_ts>=? AND first_ts<? GROUP BY b", (since, until)).fetchall()
        prior = conn.execute("SELECT COUNT(*) FROM did_first WHERE first_ts<?", (since,)).fetchone()[0]
        coverage = conn.execute("SELECT MIN(first_ts) FROM did_first").fetchone()[0]

    counts = {r["b"]: r["n"] for r in rows}
    step = timedelta(hours=1) if bucket == "hour" else timedelta(days=1)
    fmt = "%Y-%m-%dT%H" if bucket == "hour" else "%Y-%m-%d"
    start = datetime.strptime(since[:width], fmt).replace(tzinfo=timezone.utc)
    end = datetime.strptime(until, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    buckets, t = [], start
    while t < end:
        k = t.strftime(fmt)
        buckets.append({"bucket": k, "new_dids": counts.get(k, 0)})
        t += step
    total_new = sum(b["new_dids"] for b in buckets)

    notes = [
        "Counts signed did:key identities only -- unsigned nicks are self-asserted and not identities.",
        "'New' means first seen in THIS archive, not first ever on technocore-chat.",
    ]
    if coverage and since <= (_ts_shift(coverage, days=1)):
        notes.append(
            f"Range starts within a day of this archive's coverage start ({coverage}) -- the earliest "
            "bucket(s) are inflated by identities that already existed before archiving began."
        )
    return {
        "room": room,
        "bucket": bucket,
        "since": since,
        "until": until,
        "archive_coverage_start": coverage,
        "distinct_dids_before_range": prior,
        "new_dids_in_range": total_new,
        "distinct_dids_at_range_end": prior + total_new,
        "buckets": buckets,
        "notes": notes,
    }


def _ts_shift(ts_norm: str, days: int) -> str:
    dt = datetime.strptime(ts_norm, "%Y-%m-%dT%H:%M:%SZ") + timedelta(days=days)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def find_offset(conn, room: str, seq: int) -> int:
    room_id = _room_id_of(conn, room)
    if room_id is None:
        return 0
    row = conn.execute(
        "SELECT offset FROM seq_offsets WHERE room_id=? AND seq<=? ORDER BY seq DESC LIMIT 1",
        (room_id, seq)).fetchone()
    return row[0] if row else 0


def read_range(path: Path, start_offset: int, since_seq: int, limit: int) -> list[dict]:
    """Messages with seq > since_seq, reading forward from a checkpoint offset
    instead of from the top of a multi-GB file."""
    out: list[dict] = []
    skipped = 0
    with open(path, "rb") as f:
        f.seek(start_offset)
        for raw in f:
            if not raw.endswith(b"\n"):
                break
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            seq = msg.get("seq")
            if isinstance(seq, int) and seq > since_seq:
                out.append(msg)
                if len(out) >= limit:
                    break
            else:
                skipped += 1
                if skipped > MAX_SKIP_LINES:
                    break
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("build", help="incrementally (re)build the index")
    sp.add_argument("--archive-dir", required=True, type=Path)
    sp.add_argument("--db", required=True, type=Path)
    sp.add_argument("--full", action="store_true", help="drop everything and rebuild from scratch")
    args = ap.parse_args(argv)
    result = build(args.archive_dir, args.db, full=args.full)
    print(json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
