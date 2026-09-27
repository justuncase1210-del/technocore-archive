#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""Segmented, compressed storage for a room's archive.

Layout, per room:
    archives/<room>.jsonl                          live file -- the watcher appends here; always exists
    archives/segments/<room>/manifest.json
    archives/segments/<room>/<start>-<end>.jsonl     sealed, not yet compressed
    archives/segments/<room>/<start>-<end>.jsonl.gz  sealed and compressed

A room's history is one logical stream: its segments in order, then the live
file. Offsets are byte positions in that uncompressed stream, and neither
rotation nor compression ever changes them -- so every byte offset the
indexes and the /stats counters have already recorded stays valid.

Only the room's watcher thread rotates (the one writer; it closes its own
handle first). Compaction runs as a separate process (`compact` below) and
only ever touches sealed segments. Manifest read-modify-writes take a
per-room file lock, and readers pair the manifest with the live file by
inode plus a manifest re-read, so a reader can't combine one rotation's
manifest with another rotation's live file.

    uv run archive_store.py compact --archive-dir archives
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

ROTATE_BYTES = 64 * 1024 * 1024   # live file size that triggers a rotation
CHUNK_BYTES = 64 * 1024 * 1024    # max uncompressed bytes per compressed chunk (bounds a seek's decompress)
GZIP_LEVEL = 6
MANIFEST = "manifest.json"
_READ_BLOCK = 1 << 20

try:
    import fcntl
except ImportError:  # Windows dev machine: single process, a thread lock is enough there
    fcntl = None
_thread_locks: dict[str, threading.Lock] = {}


def seg_dir(archive_dir: Path, room: str) -> Path:
    return Path(archive_dir) / "segments" / room


def live_path(archive_dir: Path, room: str) -> Path:
    return Path(archive_dir) / f"{room}.jsonl"


def load_manifest(archive_dir: Path, room: str) -> dict:
    try:
        with open(seg_dir(archive_dir, room) / MANIFEST, encoding="utf-8") as f:
            m = json.load(f)
    except (FileNotFoundError, ValueError):
        m = {}
    m.setdefault("live_base", 0)
    m.setdefault("live_inode", None)
    m.setdefault("last_seq", None)
    m.setdefault("segments", [])
    return m


def _write_manifest(archive_dir: Path, room: str, m: dict) -> None:
    d = seg_dir(archive_dir, room)
    d.mkdir(parents=True, exist_ok=True)
    tmp = d / (MANIFEST + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(m, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, d / MANIFEST)


@contextmanager
def _room_lock(archive_dir: Path, room: str):
    d = seg_dir(archive_dir, room)
    d.mkdir(parents=True, exist_ok=True)
    if fcntl is None:
        lock = _thread_locks.setdefault(str(d), threading.Lock())
        with lock:
            yield
        return
    with open(d / ".lock", "w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def _seq_of(raw: bytes):
    try:
        s = json.loads(raw).get("seq")
    except (ValueError, AttributeError):
        return None
    return s if isinstance(s, int) else None


def _head_seq(path: Path):
    """First seq in a plain file -- reads only as far as the first valid line
    (a first rotation can be a 10GB file; never scan it whole)."""
    with open(path, "rb") as f:
        for raw in f:
            s = _seq_of(raw)
            if s is not None:
                return s
    return None


def _tail_last_seq(path: Path):
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 65536))
            lines = f.read().split(b"\n")
    except FileNotFoundError:
        return None
    for raw in reversed(lines):
        s = _seq_of(raw)
        if s is not None:
            return s
    return None


def last_seq(archive_dir: Path, room: str):
    """Highest archived seq: the live file's last line, else (right after a
    rotation, when the live file is empty) the manifest's."""
    s = _tail_last_seq(live_path(archive_dir, room))
    return s if s is not None else load_manifest(archive_dir, room)["last_seq"]


# ------------------------------------------------------------------ writer --

def rotate_if_large(archive_dir: Path, room: str, threshold: int | None = None) -> bool:
    """Seal the live file into a segment once it reaches `threshold` (default
    ROTATE_BYTES, read at call time). Call only from the room's single
    writer, with its own handle closed."""
    threshold = ROTATE_BYTES if threshold is None else threshold
    live = live_path(archive_dir, room)
    try:
        size = live.stat().st_size
    except FileNotFoundError:
        return False
    if size == 0 or size < threshold:
        return False
    with _room_lock(archive_dir, room):
        m = load_manifest(archive_dir, room)
        ino = os.stat(live).st_ino
        if m["live_inode"] != ino:
            # Adopt the current live file first. A reader that loaded a
            # manifest without an inode re-reads it after opening the live
            # file, and this write is what makes it notice the rotation.
            m["live_inode"] = ino
            _write_manifest(archive_dir, room, m)
        base = m["live_base"]
        first, last = _head_seq(live), _tail_last_seq(live)
        name = f"{base}-{base + size}.jsonl"
        d = seg_dir(archive_dir, room)
        os.replace(live, d / name)
        open(live, "a").close()
        m["segments"].append({"file": name, "start": base, "end": base + size, "gz": False,
                              "first_seq": first, "last_seq": last, "stored": size})
        m["live_base"] = base + size
        m["live_inode"] = os.stat(live).st_ino
        if last is not None:
            m["last_seq"] = last
        _write_manifest(archive_dir, room, m)
    return True


# ------------------------------------------------------------------ reader --

class Snapshot:
    """A consistent view of one room: its manifest plus an open handle on the
    live file that manifest describes. Use as a context manager."""

    def __init__(self, archive_dir: Path, room: str):
        self.archive_dir, self.room = Path(archive_dir), room
        self.live = live_path(archive_dir, room)
        for _ in range(20):
            m1 = load_manifest(archive_dir, room)
            fh = open(self.live, "rb")  # FileNotFoundError: the room isn't archived
            ino = os.fstat(fh.fileno()).st_ino
            m2 = load_manifest(archive_dir, room)
            if m1 == m2 and (m1["live_inode"] is None or m1["live_inode"] == ino):
                self.manifest, self.live_fh = m1, fh
                self.live_base = m1["live_base"]
                self.live_size = os.fstat(fh.fileno()).st_size
                return
            fh.close()
            time.sleep(0.02)
        raise RuntimeError(f"archive_store: couldn't get a stable view of {room} (rotating continuously?)")

    def close(self) -> None:
        self.live_fh.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    @property
    def logical_size(self) -> int:
        return self.live_base + self.live_size

    def _open_segment(self, seg: dict):
        path = seg_dir(self.archive_dir, self.room) / seg["file"]
        return (gzip.open if seg["gz"] else open)(path, "rb")

    def _refresh_segments(self) -> None:
        """A compactor swapped a segment for compressed chunks after this
        snapshot was taken. Chunks cover exactly the same logical range, so
        reload the segment list; the live side is unaffected."""
        self.manifest = {**self.manifest, "segments": load_manifest(self.archive_dir, self.room)["segments"]}

    def iter_lines(self, start: int = 0):
        """(logical offset of the line's start, raw line bytes), in order, from
        `start` (a line boundary). The live file's last line may lack its
        trailing newline -- the watcher mid-write; callers decide."""
        pos = start
        while True:
            seg = next((s for s in self.manifest["segments"] if s["start"] <= pos < s["end"]), None)
            if seg is None:
                break
            try:
                fh = self._open_segment(seg)
            except FileNotFoundError:
                self._refresh_segments()
                continue
            with fh:
                skip = pos - seg["start"]
                if seg["gz"]:
                    while skip:  # gzip isn't seekable: decompress up to the start (<= CHUNK_BYTES)
                        got = fh.read(min(skip, _READ_BLOCK))
                        if not got:
                            break
                        skip -= len(got)
                elif skip:
                    fh.seek(skip)
                for raw in fh:
                    yield pos, raw
                    pos += len(raw)
            if pos < seg["end"]:  # truncated segment file -- don't loop forever on it
                pos = seg["end"]
        if pos < self.live_base:
            pos = self.live_base
        self.live_fh.seek(pos - self.live_base)
        for raw in self.live_fh:
            yield pos, raw
            pos += len(raw)

    def iter_lines_reverse(self):
        """Raw lines from the newest backwards: the live file, then segments."""
        fh = self.live_fh
        fh.seek(0, os.SEEK_END)
        yield from _reverse_lines_of(fh, fh.tell())
        upto = self.live_base  # logical end of what's still to be read
        while upto > 0:
            seg = next((s for s in self.manifest["segments"] if s["start"] < upto <= s["end"]), None)
            if seg is None:
                break
            try:
                sfh = self._open_segment(seg)
            except FileNotFoundError:
                self._refresh_segments()  # compacted since the snapshot; same range, new files
                continue
            with sfh:
                if seg["gz"]:
                    for line in reversed(sfh.read().split(b"\n")):  # one chunk, <= CHUNK_BYTES
                        if line:
                            yield line
                else:
                    sfh.seek(0, os.SEEK_END)
                    yield from _reverse_lines_of(sfh, sfh.tell())
            upto = seg["start"]


def _reverse_lines_of(fh, size: int):
    pos, tail = size, b""
    while pos > 0:
        step = min(_READ_BLOCK, pos)
        pos -= step
        fh.seek(pos)
        parts = (fh.read(step) + tail).split(b"\n")
        tail = parts[0]
        for line in reversed(parts[1:]):
            if line:
                yield line
    if tail:
        yield tail


def _parsed(lines):
    for item in lines:
        raw = item[1] if isinstance(item, tuple) else item
        try:
            msg = json.loads(raw)
        except ValueError:
            continue
        if isinstance(msg, dict):
            yield msg


def iter_messages(archive_dir: Path, room: str, start: int = 0):
    """Every archived message of the room, oldest first, from logical offset `start`."""
    with Snapshot(archive_dir, room) as snap:
        yield from _parsed(snap.iter_lines(start))


def iter_lines_reverse(archive_dir: Path, room: str):
    with Snapshot(archive_dir, room) as snap:
        yield from snap.iter_lines_reverse()


def logical_size(archive_dir: Path, room: str) -> int:
    with Snapshot(archive_dir, room) as snap:
        return snap.logical_size


def stored_bytes(archive_dir: Path, room: str) -> int:
    m = load_manifest(archive_dir, room)
    try:
        live = live_path(archive_dir, room).stat().st_size
    except FileNotFoundError:
        live = 0
    return live + sum(s.get("stored", 0) for s in m["segments"])


# --------------------------------------------------------------- compactor --

def _compact_segment(archive_dir: Path, room: str, seg: dict) -> list[dict]:
    """Rewrite one sealed, uncompressed segment as gzip chunks of at most
    CHUNK_BYTES uncompressed each, split on line boundaries. Returns their
    manifest entries; the caller swaps them in."""
    d = seg_dir(archive_dir, room)
    chunks, pos = [], seg["start"]
    out = tmp = None
    start = first = last = None

    def seal():
        out.close()
        name = f"{start}-{pos}.jsonl.gz"
        os.replace(tmp, d / name)
        chunks.append({"file": name, "start": start, "end": pos, "gz": True, "first_seq": first,
                       "last_seq": last, "stored": (d / name).stat().st_size})

    with open(d / seg["file"], "rb") as f:
        for raw in f:
            if out is None:
                start, first, last = pos, None, None
                tmp = d / f"{start}.jsonl.gz.tmp"
                out = gzip.open(tmp, "wb", compresslevel=GZIP_LEVEL)
            out.write(raw)
            pos += len(raw)
            s = _seq_of(raw)
            if s is not None:
                first = s if first is None else first
                last = s
            if pos - start >= CHUNK_BYTES:
                seal()
                out = None
    if out is not None:
        seal()
    if pos != seg["end"]:
        raise RuntimeError(f"{room}/{seg['file']}: read {pos - seg['start']} bytes, manifest says "
                           f"{seg['end'] - seg['start']} -- leaving it uncompressed")
    return chunks


def compact(archive_dir: Path) -> dict:
    """Compress every sealed, uncompressed segment. One segment at a time,
    smallest first, so the extra disk needed at any moment is one segment's
    compressed copy."""
    archive_dir = Path(archive_dir)
    root = archive_dir / "segments"
    if not root.exists():
        return {"compacted": 0}
    lock_fh = None
    if fcntl is not None:
        lock_fh = open(root / ".compact.lock", "w")
        try:
            fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return {"skipped": "another compaction is running"}
    done, saved = 0, 0
    todo = []
    for d in root.iterdir():
        if d.is_dir():
            for seg in load_manifest(archive_dir, d.name)["segments"]:
                if not seg["gz"]:
                    todo.append((seg["end"] - seg["start"], d.name, seg))
    for _size, room, seg in sorted(todo, key=lambda t: t[0]):
        try:
            chunks = _compact_segment(archive_dir, room, seg)
        except (OSError, RuntimeError) as e:
            print(f"compact {room}/{seg['file']}: {e}", file=sys.stderr, flush=True)
            continue
        with _room_lock(archive_dir, room):
            m = load_manifest(archive_dir, room)
            i = next((k for k, s in enumerate(m["segments"]) if s["file"] == seg["file"]), None)
            if i is None:
                continue
            m["segments"][i:i + 1] = chunks
            _write_manifest(archive_dir, room, m)
        try:
            os.remove(seg_dir(archive_dir, room) / seg["file"])
        except FileNotFoundError:
            pass
        done += 1
        saved += (seg["end"] - seg["start"]) - sum(c["stored"] for c in chunks)
        print(f"compacted {room}/{seg['file']} -> {len(chunks)} chunk(s), saved {saved / 1e9:.2f} GB so far",
              flush=True)
    return {"compacted": done, "bytes_saved": saved}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="archive segment storage")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("compact", help="compress sealed segments")
    sp.add_argument("--archive-dir", required=True, type=Path)
    args = ap.parse_args(argv)
    print(json.dumps(compact(args.archive_dir)), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
