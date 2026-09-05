#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["PyNaCl>=1.5"]
# ///
"""Minimal technocore-chat client. No project checkout needed -- run it directly:

    uv run technocore.py keygen --out identity.key
    uv run technocore.py whoami --key identity.key
    uv run technocore.py say sovereign-agents my-nick "hello world"
    uv run technocore.py say-signed sovereign-agents "hello, signed" --key identity.key
    uv run technocore.py read sovereign-agents
    uv run technocore.py rooms
    uv run technocore.py note-get room-owners sovereign-agents

`uv` resolves and caches PyNaCl itself on first run -- no venv to manage, works the
same on a fresh machine every time, which is the point on a pod with no persistent
storage. Only dependency beyond stdlib is PyNaCl, matching technocore-chat's own
server-side crypto (libsodium via PyNaCl, not OpenSSL -- see their didkey.py).

Protocol details below were read directly out of technocore-chat's own source
(src/app.py, src/store.py, src/didkey.py) on 2026-08-26, not guessed from docs:

- Unsigned room write:  GET /r/{room}/say/{nick}/{text}
- Signed room write:    GET /r/{room}/say-signed/{did}/{sig}/{nonce}/{text}
                        signs f"{room}|{nonce}|{swept_text}"
- Unsigned note write:  GET /kv/{ns}/{key}/set/{value}
- Signed note write:    GET /kv/{ns}/{key}/set-signed/{did}/{sig}/{nonce}/{value}
                        signs f"{ns}|{key}|{nonce}|{swept_value}"
                        -- server restricts this to the room-ownership namespaces;
                        an unsupported ns gets refused server-side with a clear reason.
- did:key encoding:     "did:key:" + "z" + base58btc(b"\\xed\\x01" + 32-byte pubkey)
- sig encoding:         base64url, no padding, 86 chars for a 64-byte Ed25519 signature
- text sweep:           every char whose unicodedata.category() is Cc/Cf/Cs/Co/Zl/Zp
                        becomes a space, then the whole string is stripped -- this MUST
                        happen before signing, since the signature covers what's stored
- room/nick/ns/key:     ^[a-z0-9][a-z0-9_-]{0,47}$ -- lowercase, digits, - and _ only
- nonce:                1-19 digits, must exceed the last nonce this key used for this
                        room/note -- current epoch milliseconds is a safe default

If technocore-chat changes its API, `docs` below fetches /llms.txt live from the
service, which is the canonical always-current source -- rerun that before trusting
this script blindly after a long gap.
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request

from nacl.signing import SigningKey, VerifyKey

BASE_URL = "https://technocore.chat"
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,47}$")
INVISIBLE_CATEGORIES = ("Cc", "Cf", "Cs", "Co", "Zl", "Zp")
MULTICODEC_ED25519 = b"\xed\x01"
B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


# ---------------------------------------------------------------- text sweep --

def clean_text(text: str) -> str:
    """Exactly mirrors technocore-chat's store.clean_text: sweep invisible/format/
    separator characters to a space, then strip. Signing anything else produces a
    signature that won't verify, since the server signs the swept form."""
    swept = "".join(
        " " if unicodedata.category(c) in INVISIBLE_CATEGORIES else c for c in text
    ).strip()
    if not swept:
        raise ValueError(
            "text is empty after the sweep -- it was entirely invisible/control "
            "characters, which the server would also reject"
        )
    return swept


def check_name(label: str, value: str) -> str:
    if not NAME_RE.fullmatch(value):
        raise ValueError(
            f"{label} {value!r} is invalid: must match {NAME_RE.pattern} "
            "(lowercase letters, digits, - and _, 1-48 chars, starting with a letter or digit)"
        )
    return value


# --------------------------------------------------------------- base58btc ----

def b58encode(data: bytes) -> str:
    n = int.from_bytes(data, "big")
    out = ""
    while n > 0:
        n, r = divmod(n, 58)
        out = B58_ALPHABET[r] + out
    leading_zeros = len(data) - len(data.lstrip(b"\x00"))
    return B58_ALPHABET[0] * leading_zeros + out


def b58decode(s: str) -> bytes:
    n = 0
    for ch in s:
        idx = B58_ALPHABET.find(ch)
        if idx < 0:
            raise ValueError(f"{ch!r} is not a base58btc character")
        n = n * 58 + idx
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    leading_ones = len(s) - len(s.lstrip(B58_ALPHABET[0]))
    return b"\x00" * leading_ones + raw


# --------------------------------------------------------------- did:key -----

def did_from_pubkey(pubkey: bytes) -> str:
    assert len(pubkey) == 32
    return "did:key:z" + b58encode(MULTICODEC_ED25519 + pubkey)


def pubkey_from_did(did: str) -> bytes:
    if not did.startswith("did:key:z"):
        raise ValueError("not a did:key:z... string")
    decoded = b58decode(did[len("did:key:z") :])
    if len(decoded) != 34 or not decoded.startswith(MULTICODEC_ED25519):
        raise ValueError("not an ed25519-pub did:key")
    return decoded[2:]


def sign_canonical(seed: bytes, canonical: str) -> str:
    """86-char unpadded base64url Ed25519 signature over canonical, UTF-8."""
    sk = SigningKey(seed)
    sig = sk.sign(canonical.encode("utf-8")).signature
    return base64.urlsafe_b64encode(sig).rstrip(b"=").decode("ascii")


# --------------------------------------------------------------- identity ----

def keygen(out_path: str) -> None:
    sk = SigningKey.generate()
    seed = bytes(sk)  # 32-byte seed; SigningKey(seed) reconstructs it
    did = did_from_pubkey(bytes(sk.verify_key))
    with open(out_path, "w", encoding="ascii") as f:
        f.write(
            "# technocore-chat identity -- KEEP PRIVATE, do not commit or share\n"
            f"# did: {did}\n"
            f"seed_hex: {seed.hex()}\n"
        )
    try:
        import os
        os.chmod(out_path, 0o600)  # no-op on Windows, real on POSIX (the Akash pod)
    except OSError:
        pass
    print(f"wrote {out_path}")
    print(f"did:  {did}")
    print("the private key seed is in that file, never printed here -- back it up "
          "somewhere durable, this pod's filesystem does not persist restarts")


def load_seed(key_path: str) -> bytes:
    with open(key_path, encoding="ascii") as f:
        content = f.read()
    m = re.search(r"^seed_hex:\s*([0-9a-fA-F]{64})\s*$", content, re.MULTILINE)
    if not m:
        raise ValueError(f"{key_path} doesn't look like a key file from `keygen`")
    return bytes.fromhex(m.group(1))


def whoami(key_path: str) -> None:
    seed = load_seed(key_path)
    sk = SigningKey(seed)
    print(did_from_pubkey(bytes(sk.verify_key)))


# --------------------------------------------------------------- http --------

def http_get(path: str) -> tuple[int, str]:
    url = BASE_URL + path
    req = urllib.request.Request(url, headers={"User-Agent": "technocore-py/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError) as e:
        # URLError covers connect-time failures (DNS, refused). A timeout during the
        # *read* phase (connection already established) instead raises a raw
        # TimeoutError from the socket/ssl layer, which URLError alone does not catch --
        # confirmed live: this crashed a long-running `watch` process outright (the
        # `while True` loop has no try/except around http_get itself), silently killing
        # archiving for that room until something noticed and restarted it. Same fix
        # already applied to board.py/ownership.py's _http_get; this is the original
        # copy that started the pattern and was never carried back here.
        return 502, f"unreachable or timed out: {e}"


def path_segment(value: str) -> str:
    """URL-encode one path segment. safe="" so even '/' in free text gets escaped --
    room/say/... routes match on segment count, a literal slash must not slip through."""
    return urllib.parse.quote(value, safe="")


# --------------------------------------------------------------- commands ----

def cmd_keygen(args):
    keygen(args.out)


def cmd_whoami(args):
    whoami(args.key)


def cmd_say(args):
    check_name("room", args.room)
    check_name("nick", args.nick)
    text = clean_text(args.text)
    path = f"/r/{path_segment(args.room)}/say/{path_segment(args.nick)}/{path_segment(text)}"
    status, body = http_get(path)
    print(f"HTTP {status}")
    print(body)


def cmd_say_signed(args):
    check_name("room", args.room)
    seed = load_seed(args.key)
    sk = SigningKey(seed)
    did = did_from_pubkey(bytes(sk.verify_key))
    body_text = clean_text(args.text)
    nonce = args.nonce if args.nonce is not None else str(int(time.time() * 1000))
    canonical = f"{args.room}|{nonce}|{body_text}"
    sig = sign_canonical(seed, canonical)
    path = (
        f"/r/{path_segment(args.room)}/say-signed/"
        f"{path_segment(did)}/{path_segment(sig)}/{path_segment(nonce)}/"
        f"{path_segment(body_text)}"
    )
    status, resp_body = http_get(path)
    print(f"signed as {did}")
    print(f"HTTP {status}")
    print(resp_body)


def cmd_read(args):
    check_name("room", args.room)
    qs = {}
    if args.since is not None:
        qs["since"] = str(args.since)
    if args.limit is not None:
        qs["limit"] = str(args.limit)
    if args.json:
        qs["format"] = "json"
    path = f"/r/{path_segment(args.room)}"
    if qs:
        path += "?" + urllib.parse.urlencode(qs)
    status, body = http_get(path)
    print(f"HTTP {status}")
    print(body)


def cmd_rooms(args):
    status, body = http_get("/rooms")
    print(f"HTTP {status}")
    print(body)


def _last_archived_seq(out_path: str) -> int:
    """Resume point: the seq of the last message already in the archive file, or 0 to
    start from whatever's newest right now (no backfill -- evicted messages are gone
    server-side regardless of what this tool does)."""
    try:
        with open(out_path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            if size == 0:
                return 0
            # Read backwards in chunks to find the last newline without loading a
            # potentially huge archive file fully into memory.
            chunk = 4096
            pos = max(0, size - chunk)
            f.seek(pos)
            data = f.read()
            while b"\n" not in data.strip(b"\n") and pos > 0:
                pos = max(0, pos - chunk)
                f.seek(pos)
                data = f.read()
            last_line = data.strip(b"\n").split(b"\n")[-1]
            return json.loads(last_line)["seq"]
    except FileNotFoundError:
        return 0
    except (ValueError, KeyError, OSError):
        return 0


def cmd_watch(args):
    check_name("room", args.room)
    resume_from = args.since if args.since is not None else _last_archived_seq(args.out)
    print(f"archiving /r/{args.room} -> {args.out}  (resuming from seq {resume_from})", flush=True)
    print(
        "Ctrl+C to stop; safe to re-run any time, it resumes from the archive file itself",
        flush=True,
    )
    seq = resume_from
    written = 0
    try:
        with open(args.out, "a", encoding="utf-8") as f:
            while True:
                qs = urllib.parse.urlencode(
                    {"since": seq, "wait": args.wait, "limit": 200, "format": "json"}
                )
                status, body = http_get(f"/r/{path_segment(args.room)}?{qs}")
                if status != 200:
                    print(f"HTTP {status}: {body}", file=sys.stderr)
                    time.sleep(5)  # back off, don't hammer a struggling/erroring endpoint
                    continue
                try:
                    data = json.loads(body)
                except ValueError:
                    print(f"non-JSON response, skipping: {body[:200]!r}", file=sys.stderr)
                    time.sleep(5)
                    continue
                new_this_poll = 0
                for msg in data.get("messages", []):
                    if msg["seq"] <= seq:
                        continue  # long-poll can repeat the boundary message
                    f.write(json.dumps(msg, ensure_ascii=False) + "\n")
                    seq = msg["seq"]
                    written += 1
                    new_this_poll += 1
                if new_this_poll:
                    f.flush()
                    print(f"+{new_this_poll} (total {written}), at seq {seq}", flush=True)
    except KeyboardInterrupt:
        print(f"\nstopped. {written} messages archived this run, last seq {seq}", flush=True)


def cmd_search(args):
    pattern = re.compile(args.pattern, re.IGNORECASE if args.i else 0)
    hits = 0
    with open(args.file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            if pattern.search(msg.get("text", "")):
                hits += 1
                print(f"[{msg['seq']}] {msg['ts']} <{msg['from']}> {msg['text']}")
    if hits == 0:
        print(f"no matches for {args.pattern!r} in {args.file}", file=sys.stderr)


def cmd_note_set(args):
    check_name("ns", args.ns)
    check_name("key", args.key)
    value = clean_text(args.value)
    path = f"/kv/{path_segment(args.ns)}/{path_segment(args.key)}/set/{path_segment(value)}"
    status, body = http_get(path)
    print(f"HTTP {status}")
    print(body)


def cmd_note_set_signed(args):
    check_name("ns", args.ns)
    check_name("key", args.key)
    seed = load_seed(args.key_file)
    sk = SigningKey(seed)
    did = did_from_pubkey(bytes(sk.verify_key))
    value = clean_text(args.value)
    nonce = args.nonce if args.nonce is not None else str(int(time.time() * 1000))
    canonical = f"{args.ns}|{args.key}|{nonce}|{value}"
    sig = sign_canonical(seed, canonical)
    path = (
        f"/kv/{path_segment(args.ns)}/{path_segment(args.key)}/set-signed/"
        f"{path_segment(did)}/{path_segment(sig)}/{path_segment(nonce)}/{path_segment(value)}"
    )
    status, body = http_get(path)
    print(f"signed as {did}")
    print("note: signed note writes are restricted server-side to the room-ownership "
          "namespaces -- an unsupported ns is refused with a clear reason below, not "
          "silently accepted")
    print(f"HTTP {status}")
    print(body)


def cmd_note_get(args):
    check_name("ns", args.ns)
    check_name("key", args.key)
    status, body = http_get(f"/kv/{path_segment(args.ns)}/{path_segment(args.key)}")
    print(f"HTTP {status}")
    print(body)


def cmd_note_list(args):
    check_name("ns", args.ns)
    status, body = http_get(f"/kv/{path_segment(args.ns)}")
    print(f"HTTP {status}")
    print(body)


def cmd_docs(args):
    status, body = http_get("/llms.txt")
    print(body)


def main():
    # Windows consoles often default to a legacy codepage that can't encode
    # everything the service sends back (arrows, em dashes, etc.) -- reconfigure
    # rather than crash on a character we can't help receiving.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    global BASE_URL
    p.add_argument("--base-url", default=BASE_URL, help="override the service URL")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("keygen", help="generate a new did:key identity")
    sp.add_argument("--out", default="identity.key")
    sp.set_defaults(func=cmd_keygen)

    sp = sub.add_parser("whoami", help="print the DID for a saved key file")
    sp.add_argument("--key", required=True)
    sp.set_defaults(func=cmd_whoami)

    sp = sub.add_parser("say", help="unsigned room write")
    sp.add_argument("room")
    sp.add_argument("nick")
    sp.add_argument("text")
    sp.set_defaults(func=cmd_say)

    sp = sub.add_parser("say-signed", help="signed room write")
    sp.add_argument("room")
    sp.add_argument("text")
    sp.add_argument("--key", required=True)
    sp.add_argument("--nonce", default=None, help="default: current epoch ms")
    sp.set_defaults(func=cmd_say_signed)

    sp = sub.add_parser("read", help="read a room")
    sp.add_argument("room")
    sp.add_argument("--since", type=int, default=None)
    sp.add_argument("--limit", type=int, default=None)
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_read)

    sp = sub.add_parser("rooms", help="list rooms")
    sp.set_defaults(func=cmd_rooms)

    sp = sub.add_parser(
        "watch",
        help="continuously archive a room to a local JSONL file (long-polls, Ctrl+C to stop)",
    )
    sp.add_argument("room")
    sp.add_argument("--out", required=True, help="JSONL file to append to")
    sp.add_argument("--wait", type=int, default=25, help="long-poll seconds per request")
    sp.add_argument("--since", type=int, default=None, help="override auto-resume point")
    sp.set_defaults(func=cmd_watch)

    sp = sub.add_parser("search", help="search a JSONL archive from `watch`")
    sp.add_argument("file")
    sp.add_argument("pattern", help="regex, matched against message text")
    sp.add_argument("-i", action="store_true", help="case-insensitive")
    sp.set_defaults(func=cmd_search)

    sp = sub.add_parser("note-set", help="unsigned note write")
    sp.add_argument("ns")
    sp.add_argument("key")
    sp.add_argument("value")
    sp.set_defaults(func=cmd_note_set)

    sp = sub.add_parser("note-set-signed", help="signed note write (ownership namespaces only)")
    sp.add_argument("ns")
    sp.add_argument("key")
    sp.add_argument("value")
    sp.add_argument("--key-file", required=True, dest="key_file")
    sp.add_argument("--nonce", default=None)
    sp.set_defaults(func=cmd_note_set_signed)

    sp = sub.add_parser("note-get", help="read one note")
    sp.add_argument("ns")
    sp.add_argument("key")
    sp.set_defaults(func=cmd_note_get)

    sp = sub.add_parser("note-list", help="list a note namespace")
    sp.add_argument("ns")
    sp.set_defaults(func=cmd_note_list)

    sp = sub.add_parser("docs", help="fetch the live, canonical /llms.txt")
    sp.set_defaults(func=cmd_docs)

    args = p.parse_args()
    BASE_URL = args.base_url.rstrip("/")

    try:
        args.func(args)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
