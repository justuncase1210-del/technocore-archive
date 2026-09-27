#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""Per-key index for FLOP Labs' Close Call contest (close-1), stored in SQLite.

Build (incremental -- only reads bytes appended since the last run):
    uv run close1_index.py build --archive-dir archives --db close1_index.db

Sources are this service's own durable archives of the contest rooms:
`close1` (registrations, offers and signed trades) and the referee's five
`d-close1-*` rooms. Referee posts count only if signed by the referee DID.

What it answers, per owner key, that the live rooms can't: its registration,
its offers and trades in close1, and what the referee's flow posts say became
of each trade id -- after technocore.chat has evicted close1 (it keeps ~12
minutes of history) and the referee rooms (~200 posts). Limits, stated in
every response: only close1 is archived (the contest has 200+ registered
trading rooms), and the referee's flow posts omit part of every busy sweep.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

REFEREE = "did:key:z6MkowHQwsx9xr84WbWN3YCnKutyBnBXkT1ChKY4uEAAMzte"
SEASON = "close-1"
TRADE_ROOM = "close1"
REFEREE_ROOMS = ("d-close1-flow", "d-close1-state", "d-close1-pnl", "d-close1-positions")
SOURCES = (TRADE_ROOM,) + REFEREE_ROOMS
FLUSH_EVERY = 50_000
SCHEMA_VERSION = "1"

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS progress(room TEXT PRIMARY KEY, offset INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS coverage(room TEXT PRIMARY KEY, first_seq INTEGER, first_ts TEXT, last_seq INTEGER, last_ts TEXT);
CREATE TABLE IF NOT EXISTS regs(did TEXT PRIMARY KEY, seq INTEGER NOT NULL, ts TEXT) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS owner_rooms(did TEXT NOT NULL, room TEXT NOT NULL, seq INTEGER NOT NULL, PRIMARY KEY(did, room)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS posts(seq INTEGER PRIMARY KEY, kind TEXT NOT NULL, trade_id TEXT NOT NULL,
    maker TEXT NOT NULL, taker TEXT, poster TEXT NOT NULL, ts TEXT, side TEXT, qty TEXT, px TEXT, until INTEGER);
CREATE INDEX IF NOT EXISTS posts_maker ON posts(maker);
CREATE INDEX IF NOT EXISTS posts_taker ON posts(taker);
CREATE INDEX IF NOT EXISTS posts_trade ON posts(trade_id);
CREATE TABLE IF NOT EXISTS outcomes(trade_id TEXT NOT NULL, n INTEGER NOT NULL, outcome TEXT NOT NULL, reason TEXT,
    PRIMARY KEY(trade_id, n, outcome)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS mints(did TEXT PRIMARY KEY, n INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS sweeps(n INTEGER PRIMARY KEY, ts TEXT, omitted TEXT, missed TEXT, owners INTEGER);
CREATE TABLE IF NOT EXISTS board(kind TEXT NOT NULL, did TEXT NOT NULL, n INTEGER NOT NULL, rank INTEGER NOT NULL,
    value TEXT, PRIMARY KEY(kind, did, n)) WITHOUT ROWID;
"""
_DATA_TABLES = ("progress", "coverage", "regs", "owner_rooms", "posts", "outcomes", "mints", "sweeps", "board")


def connect_writer(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-65536")
    conn.execute("PRAGMA journal_size_limit=134217728")
    has_meta = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").fetchone()
    if has_meta:
        row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        if not row or row[0] != SCHEMA_VERSION:
            for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
                conn.execute(f'DROP TABLE IF EXISTS "{name}"')
            conn.commit()
    conn.executescript(SCHEMA)
    conn.execute("INSERT OR REPLACE INTO meta VALUES ('schema_version', ?)", (SCHEMA_VERSION,))
    conn.commit()
    return conn


def connect_reader(db_path, timeout: float = 10) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=timeout)
    conn.row_factory = sqlite3.Row
    return conn


# ------------------------------------------------------------------ build --

class _NeedsFullRebuild(Exception):
    pass


def _text_json(msg: dict):
    try:
        j = json.loads(msg.get("text") or "")
    except ValueError:
        return None
    return j if isinstance(j, dict) else None


def _note_coverage(c, room: str, seq, ts) -> None:
    c.execute(
        "INSERT INTO coverage(room, first_seq, first_ts, last_seq, last_ts) VALUES (?,?,?,?,?) "
        "ON CONFLICT(room) DO UPDATE SET last_seq=excluded.last_seq, last_ts=excluded.last_ts",
        (room, seq, ts, seq, ts))


def _apply_trade_room(c, msg: dict) -> None:
    j = _text_json(msg)
    seq, ts, frm = msg.get("seq"), msg.get("ts"), msg.get("from") or ""
    if j is None or j.get("season") != SEASON or not isinstance(seq, int) or not frm.startswith("did:key:"):
        return
    kind = j.get("t")
    if kind == "owner":
        if j.get("key") == frm:  # an owner registration counts only signed by that key
            c.execute("INSERT OR IGNORE INTO regs VALUES (?,?,?)", (frm, seq, ts))
    elif kind == "room":
        room = j.get("room")
        if isinstance(room, str) and room:
            c.execute("INSERT OR IGNORE INTO owner_rooms VALUES (?,?,?)", (frm, room[:64], seq))
    elif kind in ("trade", "offer"):
        terms = j.get("terms")
        if not isinstance(terms, dict) or not isinstance(terms.get("id"), str) or not isinstance(terms.get("maker"), str):
            return
        if kind == "trade":
            taker = j.get("taker") if isinstance(j.get("taker"), str) else None
        else:
            taker = terms.get("taker") if str(terms.get("taker", "")).startswith("did:key:") else None
        until = terms.get("until") if type(terms.get("until")) is int else None
        c.execute("INSERT OR IGNORE INTO posts VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                  (seq, kind, terms["id"][:64], terms["maker"], taker, frm, ts,
                   str(terms.get("side"))[:4], str(terms.get("qty"))[:16], str(terms.get("px"))[:16], until))


def _apply_referee_room(c, room: str, msg: dict) -> None:
    if msg.get("from") != REFEREE:
        return  # d- rooms only accept their owner, but never trust that blindly
    j = _text_json(msg)
    if j is None or type(j.get("n")) is not int:
        return
    n, ts = j["n"], msg.get("ts")
    if room == "d-close1-flow" and j.get("t") == "flow":
        c.executemany("INSERT OR IGNORE INTO outcomes VALUES (?,?,'settled',NULL)",
                      [(i[:64], n) for i in j.get("settled", []) if isinstance(i, str)])
        c.executemany("INSERT OR IGNORE INTO outcomes VALUES (?,?,'void',?)",
                      [(v[0][:64], n, str(v[1])[:24]) for v in j.get("void", [])
                       if isinstance(v, list) and len(v) >= 2 and isinstance(v[0], str)])
        c.executemany("INSERT OR IGNORE INTO mints VALUES (?,?)",
                      [(d, n) for d in j.get("mints", []) if isinstance(d, str)])
        c.execute("INSERT INTO sweeps(n, ts, omitted, missed) VALUES (?,?,?,?) "
                  "ON CONFLICT(n) DO UPDATE SET ts=excluded.ts, omitted=excluded.omitted, missed=excluded.missed",
                  (n, ts, json.dumps(j.get("omitted") or {}), json.dumps(j.get("missed") or [])))
    elif room == "d-close1-state" and j.get("t") == "state" and isinstance(j.get("owners"), int):
        c.execute("INSERT INTO sweeps(n, owners) VALUES (?,?) ON CONFLICT(n) DO UPDATE SET owners=excluded.owners",
                  (n, j["owners"]))
    elif room in ("d-close1-pnl", "d-close1-positions") and isinstance(j.get("top"), list):
        kind = "pnl" if room == "d-close1-pnl" else "positions"
        c.executemany("INSERT OR IGNORE INTO board VALUES (?,?,?,?,?)",
                      [(kind, row[0], n, rank, str(row[1])) for rank, row in enumerate(j["top"], 1)
                       if isinstance(row, list) and len(row) >= 2 and isinstance(row[0], str)])


def _process(c, room: str, path: Path, start: int) -> int:
    size = path.stat().st_size
    if size < start:
        raise _NeedsFullRebuild(room)
    if size == start:
        return 0
    offset, pending, added = start, 0, 0
    last = None
    with open(path, "rb") as f:
        f.seek(start)
        for raw in f:
            if not raw.endswith(b"\n"):
                break  # the live watcher's partial trailing write
            offset += len(raw)
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            if last is None:
                c.execute("INSERT OR IGNORE INTO coverage(room, first_seq, first_ts) VALUES (?,?,?)",
                          (room, msg.get("seq"), msg.get("ts")))
            last = msg
            if room == TRADE_ROOM:
                _apply_trade_room(c, msg)
            else:
                _apply_referee_room(c, room, msg)
            added += 1
            pending += 1
            if pending >= FLUSH_EVERY:
                _note_coverage(c, room, last.get("seq"), last.get("ts"))
                c.execute("INSERT OR REPLACE INTO progress VALUES (?,?)", (room, offset))
                c.commit()
                pending = 0
    if last is not None:
        _note_coverage(c, room, last.get("seq"), last.get("ts"))
    c.execute("INSERT OR REPLACE INTO progress VALUES (?,?)", (room, offset))
    c.commit()
    return added


def _reset(c) -> None:
    for t in _DATA_TABLES:
        c.execute(f"DELETE FROM {t}")
    c.execute("INSERT OR REPLACE INTO meta VALUES ('ready', '0')")
    c.commit()


def _acquire_lock(db_path: Path):
    try:
        import fcntl
    except ImportError:
        return object()
    fh = open(str(db_path) + ".lock", "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        return None
    return fh


def build(archive_dir: Path, db_path: Path) -> dict:
    if not (archive_dir / f"{TRADE_ROOM}.jsonl").exists():
        return {"skipped": "close1 is not archived on this instance"}
    lock = _acquire_lock(db_path)
    if lock is None:
        return {"skipped": "another build is already running"}
    conn = connect_writer(db_path)
    try:
        for _attempt in range(2):
            progress = {r["room"]: r["offset"] for r in conn.execute("SELECT room, offset FROM progress")}
            added = 0
            try:
                for room in SOURCES:
                    path = archive_dir / f"{room}.jsonl"
                    if path.exists():
                        added += _process(conn, room, path, progress.get(room, 0))
                break
            except _NeedsFullRebuild as e:
                print(f"archive {e} shrank below its indexed offset -- full rebuild", flush=True)
                _reset(conn)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        conn.executemany("INSERT OR REPLACE INTO meta VALUES (?,?)", [("generated_at", now), ("ready", "1")])
        conn.commit()
        return {"messages_indexed": added, "generated_at": now}
    finally:
        conn.close()


# ------------------------------------------------------------------ query --

def is_ready(db_path, timeout: float = 10) -> bool | None:
    if not Path(db_path).exists():
        return False
    try:
        conn = connect_reader(db_path, timeout)
        try:
            ver = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
            if not ver or ver[0] != SCHEMA_VERSION:
                return False
            row = conn.execute("SELECT value FROM meta WHERE key='ready'").fetchone()
            return bool(row) and row[0] == "1"
        finally:
            conn.close()
    except sqlite3.Error:
        return None


MAX_TRADES = 100


def account(conn, did: str) -> dict:
    meta = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM meta")}
    cov = {r["room"]: dict(r) for r in conn.execute("SELECT * FROM coverage")}
    last_sweep = conn.execute("SELECT MAX(n) FROM sweeps").fetchone()[0]
    reg = conn.execute("SELECT seq, ts FROM regs WHERE did=?", (did,)).fetchone()
    mint = conn.execute("SELECT n FROM mints WHERE did=?", (did,)).fetchone()
    rooms = [dict(r) for r in conn.execute("SELECT room, seq FROM owner_rooms WHERE did=? ORDER BY seq", (did,))]

    rows = conn.execute(
        "SELECT * FROM posts WHERE maker=? UNION SELECT * FROM posts WHERE taker=? ORDER BY seq DESC",
        (did, did)).fetchall()
    trades, settled_any = [], False
    for p in rows[:MAX_TRADES]:
        outs = [dict(o) for o in conn.execute(
            "SELECT n, outcome, reason FROM outcomes WHERE trade_id=? ORDER BY n", (p["trade_id"],))]
        settled_any = settled_any or any(o["outcome"] == "settled" for o in outs)
        role = "maker" if p["maker"] == did else "taker"
        side = p["side"] if role == "maker" else {"buy": "sell", "sell": "buy"}.get(p["side"], p["side"])
        if outs:
            status = "settled" if any(o["outcome"] == "settled" for o in outs) else "void"
        elif p["kind"] == "offer":
            status = "offer"
        elif last_sweep is not None and p["until"] is not None and last_sweep < p["until"]:
            status = "not_listed_yet"
        else:
            status = "not_listed"
        trades.append({"id": p["trade_id"], "kind": p["kind"], "role": role, "side": side, "qty": p["qty"],
                       "px": p["px"], "until": p["until"], "close1_seq": p["seq"], "ts": p["ts"],
                       "status": status, "referee_outcomes": outs})
    settled_any = settled_any or bool(conn.execute(
        "SELECT 1 FROM posts p JOIN outcomes o ON o.trade_id = p.trade_id AND o.outcome='settled' "
        "WHERE p.maker=? OR p.taker=? LIMIT 1", (did, did)).fetchone())

    def board(kind):
        r = conn.execute("SELECT n, rank, value FROM board WHERE kind=? AND did=? ORDER BY n DESC LIMIT 1",
                         (kind, did)).fetchone()
        return dict(r) if r else None

    pnl, positions = board("pnl"), board("positions")
    if mint:
        mint_info = {"status": "listed", "sweep": mint["n"]}
    elif settled_any:
        mint_info = {"status": "inferred", "reason": "a trade this key is party to settled, which requires a mint"}
    elif pnl or positions:
        mint_info = {"status": "inferred", "reason": "the key appears on the referee's board, which requires an account"}
    elif reg or rows:
        mint_info = {"status": "not_listed", "reason": "the referee's flow posts omit most mints on busy sweeps"}
    else:
        mint_info = {"status": "no_record", "reason": "no registration, trade or board entry for this key in the archive"}

    c1 = cov.get(TRADE_ROOM, {})
    notes = [
        f"close1 archive starts at seq {c1.get('first_seq')} ({c1.get('first_ts')}); technocore.chat keeps only "
        "~12 minutes of close1, so earlier registrations and trades were already gone when archiving began.",
        "Only close1 is archived: trades posted in other registered trading rooms aren't here.",
        "not_listed means no referee flow post names the trade id -- flow posts omit part of every busy "
        "sweep (see each sweep's 'omitted' counts), so it is not proof the trade was void.",
    ]
    return {
        "did": did,
        "generated_at": meta.get("generated_at"),
        "latest_sweep": last_sweep,
        "registration": {"close1_seq": reg["seq"], "ts": reg["ts"]} if reg else None,
        "mint": mint_info,
        "rooms_registered": rooms,
        "board": {"pnl": pnl, "positions": positions},
        "trades_total": len(rows),
        "trades_truncated": len(rows) > MAX_TRADES,
        "trades": trades,
        "coverage": {r: {k: v for k, v in d.items() if k != "room"} for r, d in cov.items()},
        "notes": notes,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("build")
    sp.add_argument("--archive-dir", required=True, type=Path)
    sp.add_argument("--db", required=True, type=Path)
    args = ap.parse_args(argv)
    print(json.dumps(build(args.archive_dir, args.db)), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
