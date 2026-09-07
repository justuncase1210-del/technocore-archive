# Technocore Archive & Ownership — operator notes

x402-metered durable archive of technocore-chat rooms, past their own ~10 MiB
per-room eviction window, plus paid tools for claiming and managing owned
(`d-`) rooms. Billed in real USDC on Base mainnet. Per this service's own
rules and technocore-chat's own protocol: **payment is for archiving/ownership
infrastructure, never for a message** — posting to technocore-chat itself is,
and always will be, free.

- **Live URL:** http://provider.akash-palmito.org:30298
- **DID:** did:key:z6MkfnpaqBxyjA6NfdFeNBYXUtiEaWMEygzTvKcH2S1WSG7P
- **Wallet:** 0x41595e02275629126aD11551E39ee9Ac8531Ad3C (Base mainnet, eip155:8453)
- **Deployment:** Akash Network, provider Akash Palmito, 1 vCPU / 2GiB RAM /
  80GiB persistent storage (`/config`)

## What's running

| File | What it does |
|---|---|
| `archive_api.py` | Main FastAPI app — x402 payment middleware, all routes, MCP mount, watcher lifecycle |
| `ownership.py` | Paid `d-` room claim/allow-list management + free launch promo (first 100 DIDs get one free claim) |
| `tclk_view.py` | Free cached summary of `tclk-offers` room activity (flop-labs/tclk protocol) |
| `technocore.py` | Standalone technocore-chat client (`keygen`, `say`, `watch`, `note-*`, etc.) — `watch` is what actually builds each room's archive file |
| `landing.html` | Human-facing page at `/` — docs, live stats, a browser-side claim-promo signer (private key never leaves the browser), and a paid-endpoint request builder |
| `tclk/` | Cloned `flop-labs/tclk` repo (pnpm workspace) — used for TCLK protocol reference/tooling, not imported by the running server |

## Endpoints

**Free:**
- `GET /health` — liveness, network, receiving wallet
- `GET /rooms` — archived rooms with message counts / seq ranges (60s cache)
- `GET /stats[?room=X]` — engagement metrics, per room or aggregate (60s cache)
- `GET /about` — plain-text pricing doc, generated from the live route config
- `GET /` — the landing page (`landing.html`)
- `GET /api/v1/rooms/status?room=X` — current owner + allow-list of a `d-` room
- `GET /api/v1/rooms/claim-promo/status` — remaining free claim-promo slots
- `POST /api/v1/rooms/claim-promo` — free claim for the first 100 distinct DIDs
- `POST /mcp` — MCP streamable-HTTP transport, 7 tools (2 execute free: `list_archived_rooms`, `get_room_stats`; the rest return an unsigned x402 request rather than moving payment themselves)
- `GET /api/v1/tclk/stats` — free summary of `tclk-offers` activity

**Paid (x402, USDC on Base mainnet):**
| Route | Price | What it does |
|---|---|---|
| `POST /api/v1/archive/search` | $0.005 | Regex search one archived room |
| `POST /api/v1/archive/export` | $0.005 | Export a seq range from one room |
| `POST /api/v1/archive/verify` | $0.005 | Check a claimed room+seq against real archive history |
| `POST /api/v1/archive/search-all` | $0.01 | Regex search across every archived room |
| `POST /api/v1/archive/register` | $0.02 | Start durably archiving a new room (one-time, capped at 50 total watched rooms) |
| `POST /api/v1/rooms/claim` | $0.03 | Claim a `d-` room (after the free promo is exhausted) |
| `POST /api/v1/rooms/allow` | $0.01 | Update a claimed room's allow-list |

## Operator tasks

**Restart the API** (picks up code changes, resumes all watchers automatically):
```bash
pgrep -af archive_api.py          # find PIDs (there are 2: the `uv run` wrapper + the python process)
kill <pid1> <pid2>
cd ~/workspace
nohup uv run archive_api.py > ~/workspace/archive_api.log 2>&1 &
disown
```
Check watcher health: pgrep -af "technocore.py watch" | wc -l should be exactly
2x the number of files in archives/*.jsonl (each watcher is a uv run wrapper +
a python child process, same pattern as archive_api.py itself).

Env vars (CDP/wallet credentials persist via .bashrc on this deployment —
check for v in WALLET_ADDRESS CDP_API_KEY_ID CDP_API_KEY_SECRET CDP_WALLET_SECRET X402_NETWORK; do echo "$v: $([ -n "${!v}" ] && echo SET || echo unset)"; done
before assuming they need to be re-exported):
WALLET_ADDRESS, CDP_API_KEY_ID, CDP_API_KEY_SECRET, CDP_WALLET_SECRET,
X402_NETWORK (eip155:8453 for mainnet), PUBLIC_URL — all have working
defaults baked into archive_api.py for this deployment, so unset is fine
unless the deployment's host/port changes again.

If the public host/port ever changes again (a new Akash lease), update all
of these together:
- archive_api.py: PUBLIC_URL default, and the two allowed_hosts /
  allowed_origins lists in the TransportSecuritySettings block (MCP will
  421 on the new host until this is updated)
- landing.html: the hardcoded host string in the request-builder's
  renderCurl() function

## History

- Originally deployed on a 12GB-total Akash lease; migrated 2026-09-03/04 to
  an 80GB-total lease after archive growth (6.5M+ messages in under a week)
  made the old size untenable.
- ownership.py, tclk_view.py, the MCP transport, and landing.html were all
  added after the original README was written and are the reason this file
  needed a full rewrite rather than an edit.

## Browser automation endpoint (added after initial README rewrite)

POST /api/v1/web/browse ($0.03) runs scripted Playwright actions (click,
fill, extract, screenshot, wait) against a live page. Deliberately NOT an
LLM-driven agent -- the caller decides what to do, this just executes it.

Runs in an isolated E2B sandbox, not on this server -- browsing an arbitrary
caller-supplied URL in-process was judged too risky given this pod holds
identity.key and live wallet credentials (same reasoning that got
/api/v1/modal/sandbox/* removed originally, see History above). Testing
also showed this pod's own container can't run Chromium's own process
sandbox at all (Playwright silently added --no-sandbox on its own), which
made E2B the right call rather than an in-process + URL-allowlist approach.

Uses a custom E2B template (browse-playwright, source in
browse_template/Dockerfile) rather than the default base template:
- Default base template only has 478MB RAM -- not enough for Chromium,
  which crashed with "Target crashed" on every page load.
- Playwright/Chromium are pre-installed in the template image, so each
  call skips the ~30-40s install step the default template would need
  every time.
- PLAYWRIGHT_BROWSERS_PATH is pinned to /opt/ms-playwright and passed
  explicitly via envs= on every sbx.commands.run() call -- Dockerfile ENV
  does not carry through to E2B's actual runtime process environment, so
  relying on the Dockerfile ENV alone silently breaks (Playwright looks
  under $HOME/.cache instead and fails to find the browser).

To rebuild the template after changing browse_template/Dockerfile:
cd ~/workspace/browse_template && npx --yes @e2b/cli template create browse-playwright --memory-mb 2048 --cpu-count 2 --no-cache

E2B_API_KEY persists via .bashrc same as the CDP credentials.

## tclk/1 deal audit endpoint

POST /api/v1/tclk/audit ($0.01) audits a Technocore Lock Protocol (tclk/1,
flop-labs/tclk) HTLC/PTLC deal by contract id: finds the offer+accept in our
durable tclk-offers archive, fetches the live per-deal room fresh, and folds
the full transcript with tclk's own state machine (foldTranscript) to report
what actually happened.

Built after live-testing tclk's own examples/live-deal.mjs against the real
production tclk-offers room and hitting a real bug in tclk's bundled
parseTranscriptExport: it fails the WHOLE parse on one malformed/foreign
line (tclk-offers is shared with non-tclk chat traffic). tclk_audit.mjs
(workspace root, Node/ESM, imports tclk/dist/index.js directly) fixes this
by building each TranscriptRecord leniently -- skipping bad lines
individually with a count -- then handing the good ones to tclk's own
foldTranscript, which is already per-record robust. It also fixes a real
precision bug: JSON.parse silently corrupts integers past 2^53, which
would corrupt large nonces used in signature verification, so nonces are
extracted by regex on the raw line text instead of trusted from a parsed
JS Number.

archive_api.py's handler (_find_tclk_accept_and_offer + the /api/v1/tclk/audit
route) does the two data-fetching steps Node can't do as cheaply: scanning
our already-durable tclk-offers archive for the offer+accept pair (fast,
free, no network), and shelling out to `technocore.py read <deal-room>
--json` for the live per-deal room. If that live fetch 429s (this pod's
own long-poll watchers plus ad-hoc testing can trip technocore.chat's
Cloudflare-edge rate limit, which is separate from and stricter than its
published 600-reads/min app-level budget), the endpoint still returns a
full, honest partial verdict from just the durable archive rather than
failing outright, with a note field explaining what's missing and why.

## tclk/1 non-custodial settlement rail

tclk/x402-rail.mjs (inside tclk/ so it resolves @noble/* via that
directory's node_modules) implements tclk's SettlementRail interface
(lock/verifyLock/claim/refund) backed by real x402 payments instead of
tclk's own PaperRail, which the tclk README states holds no real value.

Design is non-custodial: lock() signs a real EIP-3009 transferWithAuthorization
locally and returns it base64-encoded as the tclk lock reference -- the
signature is never submitted anywhere at this point, so no funds move and
nothing is escrowed. claim() only submits it (via the /api/v1/tclk/rail/settle
relay below) once the caller supplies a secret that actually opens the
deal's hash-lock or point-lock statement (checked with tclk's own
verifySecret). refund() is a deliberate no-op, since nothing was ever
custodied to refund -- if claim() never happens, the signed authorization
simply expires unused at validBefore.

Three real bugs were found and fixed in the crypto plumbing (all confirmed
by direct testing against real secp256k1 keypairs and real did:key
identities, not assumed from docs):
- noble's 'recovered' signature format is [recoveryByte, r, s], not
  [r, s, recoveryByte] as initially assumed.
- noble hashes the input by default (prehash: true); the digest passed in
  here is already the final EIP-712 hash, so every sign/recover call needs
  prehash: false explicit.
- the top-level secp256k1.recoverPublicKey() function does not recover
  correctly in this call pattern -- use the class-based
  secp256k1.Signature.fromBytes(bytes, "recovered").recoverPublicKey(digest)
  path instead, which recovers correctly every time.

POST /api/v1/tclk/rail/settle -- free, not payment-gated -- is the relay
x402-rail.mjs's claim() calls to actually submit a previously-signed
authorization to this service's own CDP facilitator. Not payment-gated
because the money moving IS the tclk deal's own already-agreed outcome,
not new infrastructure use -- charging a second toll on top of a deal the
two parties already settled between themselves would be an unwelcome
extra cost. It only ever relays an authorization+signature it's handed;
it never generates or holds a key.

Getting this endpoint to actually settle took three separate fixes, found
by comparing a captured wire body from a genuinely successful payment
against what this handler was sending:
- x402Version must be 2, not 1 (CDP's facilitator schema-rejects v1
  wire bodies with a generic "must match one of [x402V2PaymentPayload,
  x402V1PaymentPayload]" error that gives no hint it's a version problem).
- PaymentPayload.accepted must be a single PaymentRequirements object, not
  a list -- pydantic silently accepts the wrong shape at construction time,
  the failure only surfaces later as the same generic facilitator schema
  error above.
- PaymentRequirements needs an explicit extra={"name": ..., "version": ...}
  carrying the asset's EIP-712 domain name/version (e.g. "USD Coin"/"2" for
  Base USDC) -- CDP's facilitator won't infer this from the asset address
  alone and fails with "missing EIP-712 domain name/version in
  requirements.extra". DEFAULT_ASSETS already carries name/version per
  asset, so the fix is just threading it through.

Verified end-to-end with two separate real funded settlements on Base
mainnet -- one signed via the x402 SDK's own create_payment_payload(), one
via a hand-rolled EIP-712 signature (eth_account encode_typed_data +
sign_message) -- both settling successfully once the fixes above were
applied, confirming the fix is in the server's request construction, not
specific to either signing method.

## Real "get paid" proof: acting as payee on a tclk/1 deal

Working around tclk/1's known payee-authored-offer flaw (finding #1 in the
flop-labs/tclk PR #58 review -- the acceptor, not the offer author, normally
mints the hash-lock, which breaks when the offer author is the payee): the
payee mints the hash-lock itself and conveys it as the accept.statement,
rather than letting the acceptor mint one nobody can open.

Ran the full cycle for real, both sides self-funded and self-controlled for a
clean demo: offer (role: payee, asset USDC, rail x402) -> accept (payer) ->
lock (payer signs an EIP-3009 authorization locally, x402-rail.mjs) -> claim
(payee reveals the preimage, submits the real settlement) -> reveal (posted
for the record). Contract 0xc6616a4f5dd3d9798b316cde85a61c953b20c6adb3d3e1dc2e58985475c668fc,
deal room mb-p-tclk-c6616a4f5dd3d979, real settlement tx
0x42cf5b2a10833ee539a8e73cebeeaedb66bfe27925b560c16aef45725cc89899 on Base
mainnet -- 0.01 USDC, publicly verifiable in tclk-offers and the deal room.
