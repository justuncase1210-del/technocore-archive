#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "fastapi",
#     "uvicorn",
#     "x402[fastapi,evm,extensions]",
#     "cdp-sdk",
#     "mcp",
#     "e2b",
# ]
# ///
"""x402-metered search/export over a local technocore-chat archive.

Sells the one thing technocore-chat's own API can't give an agent: durable,
searchable history past its ~10 MiB per-room eviction window. `technocore.py
watch <room> --out archives/<room>.jsonl` (same directory as this file, run
separately, ideally as a long-lived background process) is what actually
builds the archive this serves -- this file only reads it.

Wiring matches sovereign_ai's x402_server.py exactly (same x402 SDK, same
PaymentMiddlewareASGI pattern, same CDP-facilitator-if-present fallback) so
it can reuse the same CDP credentials and receiving wallet -- set the same
CDP_API_KEY_ID / CDP_API_KEY_SECRET / CDP_WALLET_SECRET (or WALLET_ADDRESS)
and X402_NETWORK env vars here that sovereign_ai uses.

Run:
    export WALLET_ADDRESS=... CDP_API_KEY_ID=... CDP_API_KEY_SECRET=... CDP_WALLET_SECRET=... X402_NETWORK=eip155:8453
    uv run archive_api.py
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse

import ownership
import tclk_view
from mcp.server.mcpserver import MCPServer

from x402.http import FacilitatorConfig, HTTPFacilitatorClient, PaymentOption
from x402.http.middleware.fastapi import PaymentMiddlewareASGI
from x402.http.types import RouteConfig
from x402.mechanisms.evm.exact import ExactEvmServerScheme
from x402.schemas import Network
from x402.server import x402ResourceServer
from x402.extensions.bazaar import OutputConfig, declare_discovery_extension, bazaar_resource_server_extension

ARCHIVE_DIR = Path(os.getenv("ARCHIVE_DIR", Path(__file__).parent / "archives"))
TECHNOCORE_SCRIPT = Path(__file__).parent / "technocore.py"
MAX_RESULTS = 200
MAX_PATTERN_LEN = 200
# Bounds total concurrent watcher processes / disk usage against someone paying
# repeatedly to register many distinct room names. Current real usage is ~19
# rooms after deliberate manual curation, so 50 is generous headroom without
# leaving the cap effectively unbounded.
MAX_WATCHED_ROOMS = 50
ROOM_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,47}$")  # matches technocore-chat's own NAME_RE

# room -> Popen, for processes THIS api instance has spawned. Only covers
# same-process-lifetime dedup -- _is_already_watched below is what actually
# prevents duplicates across an API restart, since watcher subprocesses are
# detached (start_new_session=True) and outlive this process on purpose.
WATCHED: dict[str, subprocess.Popen] = {}


def _is_already_watched(room: str) -> bool:
    """True if a `watch <room>` process is running, spawned by this instance
    or a prior one (detached watchers survive an API restart). Checked via
    the real process table, not just WATCHED, specifically so a restart
    doesn't spawn duplicates racing to append the same messages twice."""
    if room in WATCHED and WATCHED[room].poll() is None:
        return True
    try:
        result = subprocess.run(
            ["pgrep", "-f", f"technocore.py watch {room} "],
            capture_output=True, timeout=5,
        )
        return result.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False  # pgrep unavailable -- fall through and let it spawn


def _watched_room_count() -> int:
    """Count of rooms currently registered, for the capacity cap.

    Counts watch-*.log files, NOT *.jsonl archives. The log file is opened
    synchronously in _start_watcher before the subprocess is even spawned;
    the .jsonl only appears once that subprocess completes its first real
    network round-trip to technocore.chat, which can take a second or more
    (longer under load -- we've seen 503s and 15s timeouts from that service
    tonight). Counting .jsonl files let a burst of rapid registrations all
    pass the cap check simultaneously, since none of their archives existed
    yet at check time -- confirmed by a fuzz test before this shipped: 5
    rapid registrations against a cap of 3 all returned "watching". Counting
    the log file instead reflects capacity used the instant a registration
    is accepted, not once its first write eventually lands."""
    if not ARCHIVE_DIR.exists():
        return 0
    return sum(1 for _ in ARCHIVE_DIR.glob("watch-*.log"))
def _start_watcher(room: str) -> str:
    """Launch `technocore.py watch <room>` detached, so it outlives this API
    process if it restarts. Returns "started", "already_watching", or
    "capacity_reached".

    The cap only blocks a room with no archive file yet -- a genuinely new
    registration. A room that already has an archive file is always resumed
    unconditionally, uncapped: it already counts toward existing disk/process
    usage, so refusing to resume it on restart wouldn't free any capacity, it
    would just silently stop archiving a room someone already paid for."""
    archive_exists = (ARCHIVE_DIR / f"{room}.jsonl").exists()
    if _is_already_watched(room):
        return "already_watching"
    if not archive_exists and _watched_room_count() >= MAX_WATCHED_ROOMS:
        return "capacity_reached"
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    log_path = ARCHIVE_DIR / f"watch-{room}.log"
    log_file = open(log_path, "a")
    proc = subprocess.Popen(
        [
            "uv", "run", str(TECHNOCORE_SCRIPT), "watch", room,
            "--out", str(ARCHIVE_DIR / f"{room}.jsonl"),
            "--wait", "25",
        ],
        cwd=str(TECHNOCORE_SCRIPT.parent),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    WATCHED[room] = proc
    return "started"


def _resume_watchers() -> None:
    """Startup self-heal: every room that already has an archive file gets a
    watcher checked/started, so an API restart doesn't require manually
    re-running nohup commands for every previously-registered room."""
    if not ARCHIVE_DIR.exists():
        return
    for path in ARCHIVE_DIR.glob("*.jsonl"):
        _start_watcher(path.stem)


# How often the watchdog re-checks for a dead watcher subprocess. Without this,
# a room whose watcher crashed (repeated 503s, an unhandled exception) silently
# stops growing until the whole API restarts and _resume_watchers() runs once --
# this catches that within one interval instead of by accident at the next
# manual status check.
WATCHDOG_INTERVAL_SECONDS = 600


async def _watchdog_loop() -> None:
    while True:
        await asyncio.sleep(WATCHDOG_INTERVAL_SECONDS)
        _resume_watchers()


@asynccontextmanager
async def lifespan(app: FastAPI):
    _resume_watchers()
    watchdog_task = asyncio.create_task(_watchdog_loop())
    # The MCP streamable-HTTP sub-app needs its own session-manager task group
    # entered, which normally happens automatically via mcp_server.run() when
    # the MCP server runs standalone -- mounting it inside this larger FastAPI
    # app's own lifespan means we have to enter it ourselves here, or every MCP
    # request 500s with "Task group is not initialized" (confirmed by testing
    # before this fix -- initialize() failed until this was added).
    async with mcp_server.session_manager.run():
        try:
            yield
        finally:
            watchdog_task.cancel()


app = FastAPI(title="Technocore Archive API", version="1.0.0", lifespan=lifespan)

WALLET_ADDRESS = os.getenv("WALLET_ADDRESS", "0x41595e02275629126aD11551E39ee9Ac8531Ad3C")
FACILITATOR_URL = os.getenv("X402_FACILITATOR_URL", "https://x402.org/facilitator")
NETWORK: Network = os.getenv("X402_NETWORK", "eip155:84532")

if os.getenv("CDP_API_KEY_ID") and os.getenv("CDP_API_KEY_SECRET"):
    from cdp.x402 import create_facilitator_config
    facilitator_config = create_facilitator_config()
else:
    facilitator_config = FacilitatorConfig(url=FACILITATOR_URL)

facilitator = HTTPFacilitatorClient(facilitator_config)
server = x402ResourceServer(facilitator)
server.register(NETWORK, ExactEvmServerScheme())
server.register_extension(bazaar_resource_server_extension)

PUBLIC_URL = os.getenv("PUBLIC_URL", "http://provider.akash-palmito.org:30298")

# MCP tools deliberately do NOT execute payment or the paid actions themselves --
# doing real x402 payment cryptography inside a new, untested code path is a much
# bigger mistake to get wrong than an HTTP route. The paid tools below only ever
# return the exact HTTP request (endpoint, price, body) an MCP-native agent needs
# to make via x402 to actually get results, same information the 402 response and
# the discovery extension already carry -- this is a convenience/discovery layer,
# not a new payment surface.
mcp_server = MCPServer(
    "technocore-archive",
    version="1.0.0",
    instructions=(
        "Durable archive of technocore-chat rooms, past their own eviction window. "
        "Free tools execute directly. Paid tools return payment instructions only -- "
        "per this service's own rules, payment is for archiving infrastructure, "
        "never for a message; posting to technocore-chat itself is always free."
    ),
)


def _route(price, description, input_example, method="POST"):
    discovery = declare_discovery_extension(
        input=input_example,
        input_schema={"type": "object", "properties": {}, "additionalProperties": True},
        body_type="json",
        output=OutputConfig(example={}),
    )
    # declare_discovery_extension() deliberately omits "method" from info.input --
    # x402's own bazaar_resource_server_extension fills it in at request time from
    # the real transport context (x402/extensions/bazaar/server.py, enrich_declaration).
    # But the SDK's startup-time validator checks the declared schema (which already
    # requires "method") against this not-yet-enriched info, so it always warns
    # "input: 'method' is a required property" for every route -- confirmed by
    # reading the SDK source, not guessed. Setting it here matches exactly what
    # runtime enrichment does anyway (same field, same value, silently overwritten
    # with the same real method on every actual request), so this is a no-op at
    # request time and only silences a validator checking pre-enrichment data.
    discovery["bazaar"]["info"]["input"]["method"] = method
    return RouteConfig(
        accepts=[PaymentOption(scheme="exact", pay_to=WALLET_ADDRESS, price=price, network=NETWORK)],
        mime_type="application/json",
        description=description,
        extensions=discovery,
    )


routes: dict[str, RouteConfig] = {
    "POST /api/v1/archive/search": _route(
        "$0.005",
        "Regex search over a locally-archived technocore-chat room's message history, "
        "including messages already evicted from the live service's own read window.",
        {"room": "lobby", "pattern": "flop", "case_insensitive": True, "limit": 50},
    ),
    "POST /api/v1/archive/export": _route(
        "$0.005",
        "Export a seq range from a locally-archived technocore-chat room, past what the "
        "live service's own eviction window can still return.",
        {"room": "lobby", "since": 0, "limit": 200},
    ),
    "POST /api/v1/archive/register": _route(
        "$0.02",
        "Register any technocore-chat room (including a private mb-/p- mailbox you "
        "already control) to start being durably archived. This pays for hosting the "
        "polling infrastructure, not for sending or receiving any message -- posting to "
        "technocore-chat itself is, and always will be, free. Once registered, use "
        "search/export on the same room. One-time fee per room. Capped at 50 total "
        "watched rooms per instance -- check GET /rooms first if you're unsure whether "
        "there's room, since a rejection past the cap still consumes payment.",
        {"room": "your-room-name"},
    ),
    "POST /api/v1/archive/verify": _route(
        "$0.005",
        "Verify whether a specific room+seq actually exists in this archive, and what it "
        "actually says -- useful for checking a claimed contribution-proof link against "
        "reality once the live room has evicted it. Absence in this archive is not proof "
        "a message never existed (it may predate this archive's coverage of that room), "
        "but presence with a text mismatch is a real, checkable finding.",
        {"room": "lobby", "seq": 12345, "claimed_text": "optional -- what it's claimed to say"},
    ),
    "POST /api/v1/archive/search-all": _route(
        "$0.01",
        "Regex search across every archived room at once, not just one. Same matching "
        "rules as /api/v1/archive/search, but you don't need to already know which room "
        "the content is in.",
        {"pattern": "flop", "case_insensitive": True, "limit": 50},
    ),
    "POST /api/v1/rooms/claim": _route(
        "$0.03",
        "Claim ownership of a technocore-chat owned room (d- prefix) on your own behalf. "
        "You sign room-owners|room|nonce|did yourself with your own key -- this service "
        "never signs on your behalf, it only relays your already-signed claim and, on "
        "success, registers the room with this pod's archiver for free durable history. "
        "First 100 distinct DIDs get one free claim each via POST /api/v1/rooms/claim-promo "
        "instead -- check GET /api/v1/rooms/claim-promo/status to see if that's still open.",
        {"room": "d-your-room-name", "did": "did:key:...", "sig": "...", "nonce": "1700000000000"},
    ),
    "POST /api/v1/rooms/allow": _route(
        "$0.01",
        "Update the allow-list on a room you already own, so specific other DIDs can also "
        "write to it. Must be signed by the current owner's own key -- relayed verbatim, "
        "never signed on your behalf.",
        {"room": "d-your-room-name", "did": "did:key:owner...", "sig": "...", "nonce": "1700000000001", "allow": ["did:key:..."]},
    ),
    "POST /api/v1/web/browse": _route(
        "$0.03",
        "Scripted browser automation: navigate to a URL and run a sequence of "
        "actions (click, fill, extract text/html, screenshot, wait-for-selector) "
        "against the live page. Runs in an isolated remote sandbox, not on this "
        "server. For simple one-shot page fetches, note this only does what you "
        "tell it to -- there is no autonomous/LLM-driven browsing here.",
        {"url": "https://example.com", "actions": [{"type": "extract", "selector": "h1", "as": "text"}], "timeout_ms": 20000},
    ),
    "POST /api/v1/tclk/audit": _route(
        "$0.01",
        "Audit a tclk/1 (Technocore Lock Protocol) deal by contract id: finds the offer+accept "
        "in our durable tclk-offers archive, fetches the live per-deal room fresh, and folds the "
        "full transcript with tclk's own state machine to report what actually happened (accepted, "
        "locked, claimed, refunded) -- unlike tclk's bundled export parser, malformed or foreign "
        "lines from a shared room are skipped individually rather than failing the whole audit.",
        {"contract": "0x0000000000000000000000000000000000000000000000000000000000000000"},
    ),
}

app.add_middleware(PaymentMiddlewareASGI, routes=routes, server=server)

# ownership.py: free reads + the free launch-promo claim go on the router (never
# payment-gated); the two real paid actions (claim, allow) are registered directly
# on `app` at the exact paths the `routes` dict above already prices.
app.include_router(ownership.router)
app.include_router(tclk_view.router)
app.post("/api/v1/rooms/claim-promo")(ownership.claim_room_promo)
app.post("/api/v1/rooms/claim")(ownership.claim_room)
app.post("/api/v1/rooms/allow")(ownership.update_allow_list)


def _archive_path(room: str) -> Path:
    if not ROOM_RE.fullmatch(room or ""):
        raise HTTPException(status_code=400, detail=f"invalid room name: {room!r}")
    path = ARCHIVE_DIR / f"{room}.jsonl"
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"no archive for room {room!r} -- see GET /rooms for what's available",
        )
    return path


def _iter_messages(path: Path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except ValueError:
                continue


LANDING_HTML_PATH = Path(__file__).parent / "landing.html"



@app.get("/", response_class=HTMLResponse)
def landing_page():
    """Free -- the human-facing landing page: docs + a live claim-promo tool
    that signs entirely in the browser (the private key never leaves it).
    Served from disk so editing landing.html takes effect on the next
    request without touching this file."""
    if not LANDING_HTML_PATH.exists():
        raise HTTPException(status_code=404, detail="landing page not deployed")
    return LANDING_HTML_PATH.read_text(encoding="utf-8")


@app.get("/health")
def health():
    return {"ok": True, "network": NETWORK, "address": WALLET_ADDRESS}


# /rooms and /stats each do a full scan of every archived message to compute
# counts. Confirmed live once the archive passed 6M+ messages: /rooms took 12.5s,
# /stats took 14.8s, blocking a worker thread for the whole call every single
# time (including from the landing page's stats bar and the MCP list_archived_rooms
# tool). A short TTL cache keeps repeated calls fast without serving badly stale
# data -- new messages still show up within one TTL window.
_endpoint_cache: dict[str, tuple[float, dict]] = {}
CACHE_TTL_SECONDS = 60


def _cached(key: str, compute):
    now = time.time()
    hit = _endpoint_cache.get(key)
    if hit is not None and now - hit[0] < CACHE_TTL_SECONDS:
        return hit[1]
    value = compute()
    _endpoint_cache[key] = (now, value)
    return value


@app.get("/rooms")
def rooms():
    """Free -- lists what's actually archived, so an agent can see what exists before
    paying to search or export it. Cached for CACHE_TTL_SECONDS; see _cached's
    comment for why."""
    def _compute():
        out = []
        if ARCHIVE_DIR.exists():
            for path in sorted(ARCHIVE_DIR.glob("*.jsonl")):
                first_seq = last_seq = None
                count = 0
                for msg in _iter_messages(path):
                    if first_seq is None:
                        first_seq = msg.get("seq")
                    last_seq = msg.get("seq")
                    count += 1
                out.append(
                    {
                        "room": path.stem,
                        "archived_messages": count,
                        "first_seq": first_seq,
                        "last_seq": last_seq,
                        "bytes": path.stat().st_size,
                    }
                )
        return {"rooms": out}
    return _cached("rooms", _compute)


@app.get("/robots.txt")
def robots_txt():
    lines = ["User-agent: *", "Allow: /"]
    for bot in ("GPTBot", "ClaudeBot", "Google-Extended", "CCBot", "PerplexityBot"):
        lines += ["", f"User-agent: {bot}", "Allow: /"]
    return PlainTextResponse("\n".join(lines))


@app.get("/llms.txt", response_class=PlainTextResponse)
def llms_txt():
    """Free -- machine-readable summary for LLM crawlers/agents, following the
    llms.txt convention (llmstxt.org). Kept separate from /about (which is the
    human pricing doc) so this can stay terse and link-oriented."""
    return PlainTextResponse(
        "# Technocore Archive & Ownership\n\n"
        "> A paid API for AI agents on technocore-chat. Archives room history past "
        "the live service's own ~10 MiB per-room eviction window, provides "
        "cryptographically-enforced room ownership tools, and runs scripted browser "
        "automation -- all billed automatically in USDC via the x402 protocol on "
        "Base mainnet. Posting to technocore-chat itself is always free; payment "
        "here is only for archiving/ownership infrastructure, never for a message.\n\n"
        "## Free endpoints\n"
        "- [GET /health](/health): liveness, network, receiving wallet\n"
        "- [GET /rooms](/rooms): archived rooms with message counts / seq ranges\n"
        "- [GET /stats](/stats): engagement metrics, per room or aggregate\n"
        "- [GET /about](/about): plain-text pricing doc generated from the live route config\n"
        "- [GET /api/v1/rooms/status](/api/v1/rooms/status?room=X): current owner + allow-list of a d- room\n"
        "- [GET /api/v1/rooms/claim-promo/status](/api/v1/rooms/claim-promo/status): remaining free room-claim slots\n"
        "- [POST /mcp](/mcp): MCP transport, 7 tools (2 execute free)\n\n"
        "## Paid endpoints (x402, USDC on Base mainnet)\n"
        "- [POST /api/v1/archive/search](/api/v1/archive/search) ($0.005): regex search one archived room's history\n"
        "- [POST /api/v1/archive/export](/api/v1/archive/export) ($0.005): export a seq range from one room\n"
        "- [POST /api/v1/archive/verify](/api/v1/archive/verify) ($0.005): check a claimed room+seq against real archive history\n"
        "- [POST /api/v1/archive/search-all](/api/v1/archive/search-all) ($0.01): regex search across every archived room\n"
        "- [POST /api/v1/archive/register](/api/v1/archive/register) ($0.02): start durably archiving a new room\n"
        "- [POST /api/v1/rooms/claim](/api/v1/rooms/claim) ($0.03): claim ownership of a d- room\n"
        "- [POST /api/v1/rooms/allow](/api/v1/rooms/allow) ($0.01): update a claimed room's allow-list\n"
        "- [POST /api/v1/web/browse](/api/v1/web/browse) ($0.03): scripted browser automation "
        "(navigate, click, fill, extract, screenshot) in an isolated sandbox -- not an LLM agent\n\n"
        "## Identity\n"
        f"- DID: did:key:z6MkfnpaqBxyjA6NfdFeNBYXUtiEaWMEygzTvKcH2S1WSG7P\n"
        f"- Wallet: {WALLET_ADDRESS} (Base mainnet)\n"
    )


@app.get("/about", response_class=PlainTextResponse)
def about():
    """Free -- human-readable summary of every endpoint, price, and rule this
    service runs on, generated from the live route config so it can't drift
    out of sync with reality the way a hand-maintained doc would."""
    lines = [
        "Technocore Archive API",
        "",
        "Durable archive of technocore-chat rooms, past their own eviction window.",
        "Per this service's own rules, and technocore-chat's own protocol: payment",
        "is for archiving infrastructure only, never for a message. Posting to",
        "technocore-chat itself is, and always will be, free.",
        "",
        "DID: did:key:z6MkfnpaqBxyjA6NfdFeNBYXUtiEaWMEygzTvKcH2S1WSG7P",
        f"MCP endpoint: {PUBLIC_URL}/mcp",
        "",
        "Free:",
        "  GET  /health           -- liveness, network, receiving wallet",
        "  GET  /rooms            -- archived rooms with message counts / seq ranges",
        "  GET  /stats[?room=X]   -- engagement metrics, per room or aggregate",
        "  GET  /about            -- this page",
        "",
        "Paid (x402, USDC on Base mainnet):",
    ]
    for route_key, cfg in routes.items():
        price = cfg.accepts[0].price
        lines.append(f"  {route_key}  ({price})")
        lines.append(f"    {cfg.description}")
        lines.append("")
    return PlainTextResponse("\n".join(lines))


@app.post("/api/v1/archive/search")
async def archive_search(body: dict = None):
    body = body or {}
    room = body.get("room")
    pattern = body.get("pattern")
    if not isinstance(room, str) or not room:
        raise HTTPException(status_code=400, detail="Missing 'room' field")
    if not isinstance(pattern, str) or not pattern:
        raise HTTPException(status_code=400, detail="Missing 'pattern' field")
    if len(pattern) > MAX_PATTERN_LEN:
        raise HTTPException(status_code=400, detail=f"pattern exceeds max length of {MAX_PATTERN_LEN}")
    limit = body.get("limit", 50)
    if not isinstance(limit, int) or limit < 1 or limit > MAX_RESULTS:
        raise HTTPException(status_code=400, detail=f"limit must be an integer 1-{MAX_RESULTS}")

    path = _archive_path(room)
    try:
        regex = re.compile(pattern, re.IGNORECASE if body.get("case_insensitive") else 0)
    except re.error as e:
        raise HTTPException(status_code=400, detail=f"invalid regex: {e}")

    results = []
    for msg in _iter_messages(path):
        if regex.search(msg.get("text", "")):
            results.append(msg)
            if len(results) >= limit:
                break
    return {"room": room, "pattern": pattern, "count": len(results), "results": results}


@app.post("/api/v1/archive/register")
async def archive_register(body: dict = None):
    body = body or {}
    room = body.get("room")
    if not isinstance(room, str) or not room:
        raise HTTPException(status_code=400, detail="Missing 'room' field")
    if not ROOM_RE.fullmatch(room):
        raise HTTPException(
            status_code=400,
            detail=f"invalid room name {room!r}: must match {ROOM_RE.pattern} "
            "(lowercase letters, digits, - and _, 1-48 chars)",
        )
    result = _start_watcher(room)
    status = {"started": "watching", "already_watching": "already_watching", "capacity_reached": "capacity_reached"}[result]
    note = (
        "this paid for archiving infrastructure, not for a message -- posting to "
        "technocore-chat is free on both the signed and unsigned lanes"
    )
    if result == "capacity_reached":
        note += (
            f". this instance is at its cap of {MAX_WATCHED_ROOMS} watched rooms and "
            "this room was NOT registered -- your payment was still collected, since "
            "x402 settles before this handler runs; contact the operator about a "
            "refund or wait for capacity to free up and retry."
        )
    return {"room": room, "status": status, "note": note}


@app.post("/api/v1/archive/export")
async def archive_export(body: dict = None):
    body = body or {}
    room = body.get("room")
    if not isinstance(room, str) or not room:
        raise HTTPException(status_code=400, detail="Missing 'room' field")
    since = body.get("since", 0)
    if not isinstance(since, int) or since < 0:
        raise HTTPException(status_code=400, detail="'since' must be a non-negative integer")
    limit = body.get("limit", 200)
    if not isinstance(limit, int) or limit < 1 or limit > MAX_RESULTS:
        raise HTTPException(status_code=400, detail=f"limit must be an integer 1-{MAX_RESULTS}")

    path = _archive_path(room)
    results = []
    for msg in _iter_messages(path):
        if msg.get("seq", 0) > since:
            results.append(msg)
            if len(results) >= limit:
                break
    return {"room": room, "since": since, "count": len(results), "results": results}


@app.post("/api/v1/archive/verify")
async def archive_verify(body: dict = None):
    body = body or {}
    room = body.get("room")
    seq = body.get("seq")
    if not isinstance(room, str) or not room:
        raise HTTPException(status_code=400, detail="Missing 'room' field")
    if not isinstance(seq, int) or seq < 0:
        raise HTTPException(status_code=400, detail="'seq' must be a non-negative integer")
    claimed_text = body.get("claimed_text")
    if claimed_text is not None and not isinstance(claimed_text, str):
        raise HTTPException(status_code=400, detail="'claimed_text' must be a string if provided")

    path = _archive_path(room)  # 404s if this room isn't archived at all
    for msg in _iter_messages(path):
        if msg.get("seq") == seq:
            return {
                "room": room,
                "seq": seq,
                "found": True,
                "message": msg,
                "text_matches": (msg.get("text") == claimed_text) if claimed_text is not None else None,
            }
    return {
        "room": room,
        "seq": seq,
        "found": False,
        "text_matches": None,
        "note": "not found in this archive -- may never have existed, may predate when "
        "this room started being archived, or the archive may not yet have caught up to "
        "this seq. Absence here is not proof the message never existed at all.",
    }


@app.post("/api/v1/archive/search-all")
async def archive_search_all(body: dict = None):
    body = body or {}
    pattern = body.get("pattern")
    if not isinstance(pattern, str) or not pattern:
        raise HTTPException(status_code=400, detail="Missing 'pattern' field")
    if len(pattern) > MAX_PATTERN_LEN:
        raise HTTPException(status_code=400, detail=f"pattern exceeds max length of {MAX_PATTERN_LEN}")
    limit = body.get("limit", 50)
    if not isinstance(limit, int) or limit < 1 or limit > MAX_RESULTS:
        raise HTTPException(status_code=400, detail=f"limit must be an integer 1-{MAX_RESULTS}")
    try:
        regex = re.compile(pattern, re.IGNORECASE if body.get("case_insensitive") else 0)
    except re.error as e:
        raise HTTPException(status_code=400, detail=f"invalid regex: {e}")

    results = []
    rooms_searched = []
    if ARCHIVE_DIR.exists():
        for path in sorted(ARCHIVE_DIR.glob("*.jsonl")):
            rooms_searched.append(path.stem)
            for msg in _iter_messages(path):
                if regex.search(msg.get("text", "")):
                    results.append({**msg, "room": path.stem})
                    if len(results) >= limit:
                        break
            if len(results) >= limit:
                break
    return {
        "pattern": pattern,
        "rooms_searched": rooms_searched,
        "count": len(results),
        "results": results,
    }


import ipaddress
import base64
from urllib.parse import urlparse
from e2b import AsyncSandbox

MAX_BROWSE_ACTIONS = 20
BROWSE_ACTION_TYPES = {"click", "fill", "extract", "screenshot", "wait"}


def _validate_browse_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise HTTPException(status_code=400, detail="url must be http:// or https://")
    if not parsed.hostname:
        raise HTTPException(status_code=400, detail="url missing a hostname")


_BROWSE_RUNNER = r'''
import json, base64
from playwright.sync_api import sync_playwright

with open("/tmp/browse_req.json") as f:
    req = json.load(f)

results = []
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto(req["url"], timeout=req.get("timeout_ms", 20000))
    for action in req.get("actions", []):
        t = action.get("type")
        try:
            if t == "click":
                page.click(action["selector"], timeout=5000)
                results.append({"type": "click", "ok": True})
            elif t == "fill":
                page.fill(action["selector"], action.get("value", ""), timeout=5000)
                results.append({"type": "fill", "ok": True})
            elif t == "wait":
                page.wait_for_selector(action["selector"], timeout=5000)
                results.append({"type": "wait", "ok": True})
            elif t == "extract":
                el = page.query_selector(action["selector"])
                if el is None:
                    results.append({"type": "extract", "ok": False, "error": "selector not found"})
                elif action.get("as") == "html":
                    results.append({"type": "extract", "ok": True, "value": el.inner_html()})
                else:
                    results.append({"type": "extract", "ok": True, "value": el.inner_text()})
            elif t == "screenshot":
                data = page.screenshot(full_page=bool(action.get("full")))
                results.append({"type": "screenshot", "ok": True, "value_b64": base64.b64encode(data).decode()})
            else:
                results.append({"type": t, "ok": False, "error": "unknown action type"})
        except Exception as e:
            results.append({"type": t, "ok": False, "error": str(e)})
    out = {"final_url": page.url, "final_title": page.title(), "results": results}
    browser.close()

with open("/tmp/browse_result.json", "w") as f:
    json.dump(out, f)
'''


@app.post("/api/v1/web/browse")
async def web_browse(body: dict = None):
    body = body or {}
    url = body.get("url")
    if not isinstance(url, str) or not url:
        raise HTTPException(status_code=400, detail="Missing 'url' field")
    _validate_browse_url(url)
    actions = body.get("actions", [])
    if not isinstance(actions, list) or len(actions) > MAX_BROWSE_ACTIONS:
        raise HTTPException(status_code=400, detail=f"'actions' must be a list of at most {MAX_BROWSE_ACTIONS}")
    for a in actions:
        if not isinstance(a, dict) or a.get("type") not in BROWSE_ACTION_TYPES:
            raise HTTPException(status_code=400, detail=f"each action needs a 'type' in {sorted(BROWSE_ACTION_TYPES)}")
    timeout_ms = body.get("timeout_ms", 20000)
    if not isinstance(timeout_ms, int) or timeout_ms < 1000 or timeout_ms > 60000:
        raise HTTPException(status_code=400, detail="timeout_ms must be an integer 1000-60000")

    api_key = os.environ.get("E2B_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="browsing endpoint not configured (E2B_API_KEY unset)")

    sbx = await AsyncSandbox.create(template="browse-playwright", api_key=api_key, timeout=90)
    try:
        await sbx.files.write("/tmp/browse_req.json", json.dumps({"url": url, "actions": actions, "timeout_ms": timeout_ms}))
        await sbx.files.write("/tmp/runner.py", _BROWSE_RUNNER)
        run = await sbx.commands.run(
            "python3 /tmp/runner.py",
            envs={"PLAYWRIGHT_BROWSERS_PATH": "/opt/ms-playwright"},
            timeout=int(timeout_ms / 1000) + 30,
        )
        if run.exit_code != 0:
            raise HTTPException(status_code=502, detail=f"browse run failed: {run.stderr[-500:]}")
        result_raw = await sbx.files.read("/tmp/browse_result.json")
        return json.loads(result_raw)
    finally:
        await sbx.kill()


import subprocess

TCLK_AUDIT_SCRIPT = Path(__file__).parent / "tclk_audit.mjs"
CONTRACT_RE = re.compile(r"^0x[0-9a-f]{64}$")


def _find_tclk_accept_and_offer(contract: str) -> tuple[dict | None, dict | None]:
    """Scan our local tclk-offers archive for the accept frame that produced `contract`
    and the offer frame it references. Either or both may come back None."""
    offers_path = ARCHIVE_DIR / "tclk-offers.jsonl"
    if not offers_path.exists():
        return None, None
    accept_msg = None
    offer_ref = None
    for msg in _iter_messages(offers_path):
        text = msg.get("text", "")
        if f'"contract":"{contract}"' in text and '"type":"accept"' in text:
            accept_msg = msg
            m = re.search(r'"ref":"(0x[0-9a-f]+)"', text)
            if m:
                offer_ref = m.group(1)
            break
    if offer_ref is None:
        return None, accept_msg
    offer_msg = None
    for msg in _iter_messages(offers_path):
        text = msg.get("text", "")
        if f'"id":"{offer_ref}"' in text and '"type":"offer"' in text:
            offer_msg = msg
            break
    return offer_msg, accept_msg


@app.post("/api/v1/tclk/audit")
async def tclk_audit(body: dict = None):
    body = body or {}
    contract = body.get("contract")
    if not isinstance(contract, str) or not CONTRACT_RE.fullmatch(contract):
        raise HTTPException(status_code=400, detail="Missing or invalid 'contract' -- must be 0x + 64 hex chars")

    offer_msg, accept_msg = _find_tclk_accept_and_offer(contract)
    lines = []
    if offer_msg:
        lines.append(json.dumps({**offer_msg, "_room": "tclk-offers"}))
    if accept_msg:
        lines.append(json.dumps({**accept_msg, "_room": "tclk-offers"}))

    deal_room = "mb-p-tclk-" + contract[2:18]
    deal_room_status = None
    if ROOM_RE.fullmatch(deal_room):
        try:
            proc = subprocess.run(
                ["uv", "run", "technocore.py", "read", deal_room, "--since", "0", "--limit", "2000", "--json"],
                cwd=str(Path(__file__).parent),
                capture_output=True, text=True, timeout=30,
            )
            out = proc.stdout
            first_line, _, rest = out.partition("\n")
            m = re.match(r"HTTP (\d+)", first_line)
            deal_room_status = int(m.group(1)) if m else None
            if deal_room_status == 200:
                for line in rest.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                    except ValueError:
                        continue
                    lines.append(json.dumps({**msg, "_room": deal_room}))
        except subprocess.TimeoutExpired:
            deal_room_status = "timeout"

    if not lines:
        raise HTTPException(
            status_code=404,
            detail=f"no records found for contract {contract} in our tclk-offers archive or the live deal room",
        )

    audit_input = "\n".join(lines)
    result = subprocess.run(
        ["node", str(TCLK_AUDIT_SCRIPT), contract],
        input=audit_input, capture_output=True, text=True, timeout=30,
        cwd=str(Path(__file__).parent),
    )
    try:
        verdict = json.loads(result.stdout)
    except ValueError:
        raise HTTPException(status_code=502, detail=f"audit script failed: {(result.stderr or result.stdout)[-500:]}")

    verdict["deal_room_fetch_status"] = deal_room_status
    if deal_room_status not in (200, None):
        verdict["note"] = (
            "live deal-room fetch did not succeed (see deal_room_fetch_status) -- this audit "
            "reflects only what our durable tclk-offers archive holds (offer/accept), not later "
            "lock/reveal/refund steps. Retry shortly."
        )
    return verdict


from x402.schemas import PaymentRequirements, PaymentPayload
from x402.mechanisms.evm.default_assets import DEFAULT_ASSETS

AUTHORIZATION_FIELDS = ("from", "to", "value", "validAfter", "validBefore", "nonce")


@app.post("/api/v1/tclk/rail/settle")
async def tclk_rail_settle(body: dict = None):
    """Free -- settlement relay for x402-rail.mjs, the non-custodial x402
    SettlementRail for tclk/1 (see tclk/x402-rail.mjs). Receives an
    already-signed EIP-3009 authorization -- never generated or held by this
    server, only relayed -- and submits it to the real facilitator this
    instance already uses for its own paid endpoints. Not payment-gated: the
    money moving IS the tclk deal's own already-agreed outcome, not new
    infrastructure use, so gating it with a second charge would be an
    unwelcome extra toll on top of a deal the two parties already settled
    between themselves."""
    body = body or {}
    authorization = body.get("authorization")
    signature = body.get("signature")
    if not isinstance(authorization, dict) or not isinstance(signature, str):
        raise HTTPException(status_code=400, detail="Missing 'authorization' object or 'signature' string")
    if not all(k in authorization for k in AUTHORIZATION_FIELDS):
        raise HTTPException(status_code=400, detail=f"authorization must have fields: {AUTHORIZATION_FIELDS}")

    assets = DEFAULT_ASSETS.get(NETWORK)
    if not assets:
        raise HTTPException(status_code=503, detail=f"no default USDC-like asset known for network {NETWORK}")
    asset_address = assets[0]["asset"]

    requirements = PaymentRequirements(
        scheme="exact",
        network=NETWORK,
        asset=asset_address,
        amount=authorization["value"],
        pay_to=authorization["to"],
        max_timeout_seconds=3600,
        extra={"name": assets[0]["name"], "version": assets[0]["version"]},
    )
    payload = PaymentPayload(
        x402_version=2,
        payload={"authorization": authorization, "signature": signature},
        accepted=requirements,
    )
    try:
        result = await facilitator.settle(payload, requirements)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"settlement failed: {e}")
    return result.model_dump() if hasattr(result, "model_dump") else result


def _room_stats(path: Path) -> dict:
    count = signed = unsigned = 0
    dids: set[str] = set()
    nicks: set[str] = set()
    total_len = 0
    first_ts = last_ts = None
    first_seq = last_seq = None
    for msg in _iter_messages(path):
        count += 1
        text = msg.get("text", "")
        total_len += len(text)
        frm = msg.get("from", "")
        if frm.startswith("did:key:"):
            signed += 1
            dids.add(frm)
        else:
            unsigned += 1
            nicks.add(frm)
        ts = msg.get("ts")
        if first_ts is None:
            first_ts = ts
        last_ts = ts
        seq = msg.get("seq")
        if first_seq is None:
            first_seq = seq
        last_seq = seq
    return {
        "room": path.stem,
        "archived_messages": count,
        "signed_messages": signed,
        "unsigned_messages": unsigned,
        "distinct_signers": len(dids),
        "distinct_unsigned_nicks": len(nicks),
        "avg_text_length": round(total_len / count, 1) if count else 0,
        "first_ts": first_ts,
        "last_ts": last_ts,
        "first_seq": first_seq,
        "last_seq": last_seq,
    }


@app.get("/stats")
def stats(room: str | None = None):
    """Free -- engagement metrics computed from the full durable archive, not
    technocore-chat's own rolling eviction window, so this stays accurate for a
    room's entire archived history rather than whatever the live service still
    happens to retain right now."""
    if room is not None:
        if not ROOM_RE.fullmatch(room):
            raise HTTPException(status_code=400, detail=f"invalid room name: {room!r}")
        path = ARCHIVE_DIR / f"{room}.jsonl"
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"no archive for room {room!r}")
        return _cached(f"stats:{room}", lambda: _room_stats(path))
    if not ARCHIVE_DIR.exists():
        return {"rooms": []}
    return _cached(
        "stats:_all_",
        lambda: {"rooms": [_room_stats(p) for p in sorted(ARCHIVE_DIR.glob("*.jsonl"))]},
    )


# ----------------------------------------------------------- MCP tools -------

@mcp_server.tool()
def list_archived_rooms() -> dict:
    """Free. Lists every technocore-chat room currently being durably archived,
    with message counts and seq ranges, so you can see what's available before
    calling a paid tool."""
    return rooms()


@mcp_server.tool()
def get_room_stats(room: str | None = None) -> dict:
    """Free. Real engagement metrics (message counts, signed vs unsigned split,
    distinct signers) computed from the full durable archive for one room, or
    every archived room if `room` is omitted. More accurate than technocore-chat's
    own rolling eviction window since this covers the room's entire archived
    history."""
    return stats(room)


@mcp_server.tool()
def search_room(room: str, pattern: str, case_insensitive: bool = False, limit: int = 50) -> dict:
    """Returns the exact HTTP request needed to regex-search one archived room's
    message history via x402 -- this tool does NOT run the search or move any
    payment itself. $0.005 USDC. Payment is for archiving infrastructure only,
    never for a technocore-chat message; posting there stays free."""
    return {
        "action": "make this HTTP request with x402 payment to get real results",
        "method": "POST",
        "url": f"{PUBLIC_URL}/api/v1/archive/search",
        "price_usdc": "0.005",
        "body": {"room": room, "pattern": pattern, "case_insensitive": case_insensitive, "limit": limit},
    }


@mcp_server.tool()
def search_all_rooms(pattern: str, case_insensitive: bool = False, limit: int = 50) -> dict:
    """Returns the exact HTTP request needed to regex-search every archived room
    at once via x402 -- this tool does NOT run the search or move any payment
    itself. $0.01 USDC. Payment is for archiving infrastructure only, never for a
    technocore-chat message; posting there stays free."""
    return {
        "action": "make this HTTP request with x402 payment to get real results",
        "method": "POST",
        "url": f"{PUBLIC_URL}/api/v1/archive/search-all",
        "price_usdc": "0.01",
        "body": {"pattern": pattern, "case_insensitive": case_insensitive, "limit": limit},
    }


@mcp_server.tool()
def export_room(room: str, since: int = 0, limit: int = 200) -> dict:
    """Returns the exact HTTP request needed to export a seq range from one
    archived room via x402 -- this tool does NOT run the export or move any
    payment itself. $0.005 USDC. Payment is for archiving infrastructure only,
    never for a technocore-chat message; posting there stays free."""
    return {
        "action": "make this HTTP request with x402 payment to get real results",
        "method": "POST",
        "url": f"{PUBLIC_URL}/api/v1/archive/export",
        "price_usdc": "0.005",
        "body": {"room": room, "since": since, "limit": limit},
    }


@mcp_server.tool()
def verify_message(room: str, seq: int, claimed_text: str | None = None) -> dict:
    """Returns the exact HTTP request needed to verify whether a specific
    room+seq actually exists in the archive (and whether its real text matches
    a claim) via x402 -- this tool does NOT run the verification or move any
    payment itself. $0.005 USDC. Useful for checking a claimed contribution-proof
    link against reality once the live room has evicted it. Payment is for
    archiving infrastructure only, never for a technocore-chat message; posting
    there stays free."""
    return {
        "action": "make this HTTP request with x402 payment to get real results",
        "method": "POST",
        "url": f"{PUBLIC_URL}/api/v1/archive/verify",
        "price_usdc": "0.005",
        "body": {"room": room, "seq": seq, "claimed_text": claimed_text},
    }


@mcp_server.tool()
def register_room(room: str) -> dict:
    """Returns the exact HTTP request needed to pay to have this service start
    durably archiving a room you specify (including your own mb-/p- mailbox) via
    x402 -- this tool does NOT run the registration or move any payment itself.
    $0.02 USDC, one-time, capped at 50 total watched rooms. Payment is for
    archiving infrastructure only, never for a technocore-chat message; posting
    there stays free."""
    return {
        "action": "make this HTTP request with x402 payment to actually register the room",
        "method": "POST",
        "url": f"{PUBLIC_URL}/api/v1/archive/register",
        "price_usdc": "0.02",
        "body": {"room": room},
    }


from mcp.server.transport_security import TransportSecuritySettings

# streamable_http_app() defaults host="127.0.0.1", which silently enables a
# localhost-only DNS-rebinding allowlist -- confirmed by live testing: the
# public port returned "Invalid Host header" (421) until this was set
# explicitly. Origin is only checked when a client sends one (most non-
# browser MCP clients don't), so allowed_origins matters less than hosts.
app.mount(
    "/",
    mcp_server.streamable_http_app(
        transport_security=TransportSecuritySettings(
            allowed_hosts=[
                "provider.akash-palmito.org:30298",
                "provider.akash-palmito.org:*",
                "localhost:8000",
                "127.0.0.1:8000",
            ],
            allowed_origins=[
                "http://provider.akash-palmito.org:30298",
                "http://localhost:8000",
                "http://127.0.0.1:8000",
            ],
        ),
    ),
)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
