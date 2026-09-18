#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Self-dealing + timing-structure signals in tclk-offers, for flop-labs/yellowpaper#58.

Streaming rewrite: the archive has grown to 3.2M+ lines since this was first
built, well past what the original two-pass version (full offers dict with
5 fields including a rails tuple, plus a full accepts list held in memory for
a final join pass) could handle -- it pushed the container from ~1GB to the
2GB cgroup ceiling and stayed pinned there. The file is chronologically
appended by watch-all, so an accept's referenced offer always already exists
in the dict by the time the accept is seen -- meaning the join can happen
inline, record by record, with no need to ever hold the full accepts list.
Offer entries are trimmed to the two fields anything downstream actually
reads (from, ts); amount/asset/rails were stored but never used.

Usage:
    uv run python3 tclk_sybil_signals_v2.py archives/tclk-offers.jsonl \
        --out sybil_signals.json
"""
from __future__ import annotations

import argparse
import collections
import json
import statistics
import sys
from datetime import datetime, timezone

try:
    import resource  # Unix-only; the memory limit below is skipped elsewhere
except ImportError:
    resource = None


def iter_jsonl(path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def parse_ts(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000.0
    except Exception:
        return None


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--out", default="sybil_signals.json")
    ap.add_argument("--did-index-out", default=None,
                     help="if set, also write a per-DID lookup index (self-accepts, "
                          "reciprocal partners, reused statements) for a live risk-check API")
    ap.add_argument("--mint-window-seconds", type=float, default=60.0,
                     help="an identity's action counts as 'mint-and-transact' if it "
                          "falls within this many seconds of that identity's first "
                          "ever message in the room")
    ap.add_argument("--max-mem-mb", type=int, default=2048,
                     help="self-imposed RLIMIT_AS ceiling in MB -- exit cleanly with "
                          "MemoryError instead of risking the whole container's OOM "
                          "killer picking a victim among archive_api.py/watch-all too")
    a = ap.parse_args(argv)

    if resource is not None:
        try:
            resource.setrlimit(resource.RLIMIT_AS, (a.max_mem_mb * 1024 * 1024, a.max_mem_mb * 1024 * 1024))
        except (ValueError, OSError) as e:
            print(f"warning: could not set memory limit: {e}", file=sys.stderr)

    offers: dict[str, tuple] = {}            # offer id -> (from, ts) only
    first_seen: dict[str, float] = {}        # did -> earliest ts (ms) seen anywhere
    daily_first_seen = collections.Counter()  # date-string -> count of dids first seen that day
    # statement -> first-seen owner DID (plain string, no set overhead) for the
    # ~99.996% of statements that only ever have one owner (156 of ~4M+ in the
    # real archive). Only promoted into statement_multi_owners, below, the
    # instant a genuinely different second DID is seen -- so the set() cost
    # (real, measured overhead per instance, not just its elements) is paid
    # only for the rare reused statements instead of every single one.
    statement_owners: dict[str, str] = {}
    statement_multi_owners: dict[str, set] = {}
    pair_counts = collections.Counter()       # (payer, accepter) -> n accepted offers
    _intern = sys.intern  # local binding: same did:key string reused across offers,
                          # pair_counts, statement_owners, first_seen etc. now shares
                          # ONE object instead of a fresh allocation at each site

    offers_posted_by_did = collections.Counter()
    accepts_made_by_did = collections.Counter()
    self_accepts_by_did = collections.Counter()

    total_records = 0
    offers_parsed = 0
    accepts_parsed = 0
    total_resolved = 0
    self_accepts = 0
    mint_offers = 0
    mint_accepts = 0
    latencies_new = []
    latencies_established = []

    for r in iter_jsonl(a.path):
        total_records += 1
        did = r.get("from")
        if did:
            did = _intern(did)
        ts = parse_ts(r.get("ts"))
        if did and ts is not None and did not in first_seen:
            first_seen[did] = ts
            day = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
            daily_first_seen[day] += 1

        text = (r.get("text") or "").strip()
        if not text.startswith("tclk1 "):
            continue
        try:
            o = json.loads(text[6:])
        except Exception:
            continue
        t = o.get("type")

        if t == "offer":
            oid = o.get("id")
            if not oid:
                continue
            offers[oid] = (did, ts)
            offers_parsed += 1
            if did:
                offers_posted_by_did[did] += 1
            fs = first_seen.get(did)
            if fs is not None and ts is not None and (ts - fs) <= a.mint_window_seconds * 1000.0:
                mint_offers += 1

        elif t == "accept":
            accepts_parsed += 1
            if did:
                accepts_made_by_did[did] += 1
            stmt = o.get("statement")
            if stmt and did:
                if stmt in statement_multi_owners:
                    statement_multi_owners[stmt].add(did)
                elif stmt in statement_owners:
                    first_owner = statement_owners[stmt]
                    if first_owner != did:
                        statement_multi_owners[stmt] = {first_owner, did}
                        del statement_owners[stmt]
                    # else: same DID re-using their own statement -- no new info
                else:
                    statement_owners[stmt] = did
            fs = first_seen.get(did)
            is_mint = fs is not None and ts is not None and (ts - fs) <= a.mint_window_seconds * 1000.0
            if is_mint:
                mint_accepts += 1

            off = offers.get(o.get("ref"))
            if off is None or off[0] is None or did is None:
                continue
            total_resolved += 1
            payer, accepter = off[0], did
            pair_counts[(payer, accepter)] += 1
            if payer == accepter:
                self_accepts += 1
                self_accepts_by_did[payer] += 1

            off_ts = off[1]
            if off_ts is not None and ts is not None and ts >= off_ts:
                dt = (ts - off_ts) / 1000.0
                (latencies_new if is_mint else latencies_established).append(dt)

    # -- reciprocal wash-pairs: both directions present, excluding self-pairs
    #    (payer == accepter already appears in self_accepts -- without this
    #    exclusion a self-accept trivially satisfies "(c,p) in pair_counts"
    #    against itself and gets double-counted here as a fake second signal).
    #    The actual DID pair is recorded so the top entries can be verified by
    #    hand, not just trusted as bare counts.
    reciprocal = []
    seen = set()
    for (p, c), n in pair_counts.items():
        if p == c:
            continue
        if (c, p) in pair_counts and (p, c) not in seen and (c, p) not in seen:
            seen.add((p, c)); seen.add((c, p))
            reciprocal.append({
                "did_a": p, "did_b": c,
                "pair_total": n + pair_counts[(c, p)],
                "a_to_b": n, "b_to_a": pair_counts[(c, p)],
            })
    reciprocal.sort(key=lambda d: -d["pair_total"])

    # -- statement reuse across distinct DIDs -------------------------------------
    # statement_multi_owners already holds only the genuinely-reused statements
    # (see the two-tier tracking above) -- no filtering pass needed here anymore.
    reused = {s: sorted(owners) for s, owners in statement_multi_owners.items()}

    # -- per-DID index for a live pre-trade risk-check API -------------------------
    # Built from aggregates already computed above (reciprocal, reused, the three
    # per-DID counters) -- no extra pass over the file needed.
    if a.did_index_out:
        did_index: dict[str, dict] = collections.defaultdict(lambda: {
            "offers_posted": 0, "accepts_made": 0, "self_accepts": 0,
            "reciprocal_partners": [], "reused_statements": [],
        })
        for did, n in offers_posted_by_did.items():
            did_index[did]["offers_posted"] = n
        for did, n in accepts_made_by_did.items():
            did_index[did]["accepts_made"] = n
        for did, n in self_accepts_by_did.items():
            did_index[did]["self_accepts"] = n
        for pr in reciprocal:
            did_index[pr["did_a"]]["reciprocal_partners"].append(
                {"with": pr["did_b"], "a_to_b": pr["a_to_b"], "b_to_a": pr["b_to_a"], "total": pr["pair_total"]})
            did_index[pr["did_b"]]["reciprocal_partners"].append(
                {"with": pr["did_a"], "a_to_b": pr["b_to_a"], "b_to_a": pr["a_to_b"], "total": pr["pair_total"]})
        for stmt, owners in reused.items():
            for did in owners:
                did_index[did]["reused_statements"].append(stmt)

        with open(a.did_index_out, "w", encoding="utf-8") as fh:
            json.dump({
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "dids": dict(did_index),
            }, fh)

    # -- daily distinct-signer growth ---------------------------------------------
    cumulative = {}
    running = 0
    for day in sorted(daily_first_seen):
        running += daily_first_seen[day]
        cumulative[day] = running

    def pctiles(xs):
        if not xs:
            return None
        xs = sorted(xs)
        def p(q):
            i = min(len(xs) - 1, int(q * len(xs)))
            return round(xs[i], 3)
        return {"n": len(xs), "p10": p(.10), "p50": p(.50), "p90": p(.90),
               "mean": round(statistics.mean(xs), 3)}

    result = {
        "total_records": total_records,
        "distinct_signers": len(first_seen),
        "offers_parsed": offers_parsed,
        "accepts_parsed": accepts_parsed,
        "accepts_resolved_to_an_offer": total_resolved,
        "self_accepts": self_accepts,
        "self_accept_rate_pct": round(100.0 * self_accepts / total_resolved, 3) if total_resolved else None,
        "reciprocal_wash_pairs": {
            "count_of_pairs": len(reciprocal),
            "total_accepts_in_reciprocal_pairs": sum(p["pair_total"] for p in reciprocal),
            "top_20": reciprocal[:20],
        },
        "cross_did_statement_reuse": {
            "distinct_statements_reused": len(reused),
            "examples": {s: owners for s, owners in list(reused.items())[:20]},
        },
        "mint_and_transact": {
            "mint_window_seconds": a.mint_window_seconds,
            "offers_within_window_of_posters_first_message": mint_offers,
            "offers_within_window_pct": round(100.0 * mint_offers / offers_parsed, 3) if offers_parsed else None,
            "accepts_within_window_of_accepters_first_message": mint_accepts,
            "accepts_within_window_pct": round(100.0 * mint_accepts / accepts_parsed, 3) if accepts_parsed else None,
        },
        "offer_to_accept_latency_seconds": {
            "mint_accepters": pctiles(latencies_new),
            "established_accepters": pctiles(latencies_established),
        },
        "daily_new_signers": dict(daily_first_seen),
        "daily_cumulative_signers": cumulative,
    }

    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=2)

    print("=" * 70)
    print("SELF-ACCEPT")
    print(f"  {self_accepts} / {total_resolved} resolved accepts "
          f"({result['self_accept_rate_pct']}%)")
    print()
    print("RECIPROCAL WASH-PAIRS")
    print(f"  {len(reciprocal)} pairs, {result['reciprocal_wash_pairs']['total_accepts_in_reciprocal_pairs']} "
          f"total accepts inside them")
    for p in reciprocal[:10]:
        print(f"    ...{p['did_a'][-12:]} <-> ...{p['did_b'][-12:]}: "
              f"{p['a_to_b']} + {p['b_to_a']} = {p['pair_total']}")
    print()
    print("CROSS-DID STATEMENT REUSE")
    print(f"  {len(reused)} statements each posted by >1 distinct DID")
    for s, owners in list(reused.items())[:5]:
        print(f"    {s[:18]}...  owners={len(owners)}")
    print()
    print("MINT-AND-TRANSACT")
    print(f"  offers within {a.mint_window_seconds}s of poster's first msg:  "
          f"{mint_offers}/{offers_parsed} ({result['mint_and_transact']['offers_within_window_pct']}%)")
    print(f"  accepts within {a.mint_window_seconds}s of accepter's first msg: "
          f"{mint_accepts}/{accepts_parsed} ({result['mint_and_transact']['accepts_within_window_pct']}%)")
    print()
    print("OFFER->ACCEPT LATENCY (seconds)")
    print(f"  mint accepters:        {result['offer_to_accept_latency_seconds']['mint_accepters']}")
    print(f"  established accepters: {result['offer_to_accept_latency_seconds']['established_accepters']}")
    print()
    print("DAILY NEW / CUMULATIVE DISTINCT SIGNERS")
    for day in sorted(daily_first_seen):
        print(f"  {day}: +{daily_first_seen[day]:>6}  cumulative={cumulative[day]}")
    print()
    print(f"wrote {a.out}")
    if a.did_index_out:
        print(f"wrote {a.did_index_out} ({len(did_index)} distinct DIDs)")


if __name__ == "__main__":
    main(sys.argv[1:])
