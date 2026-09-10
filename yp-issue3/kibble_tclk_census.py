#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""kibble verdict-constancy + tclk dispute cross-reference, over a continuous archive.

Origin: flop-labs/yellowpaper#3 disputes R3.5d's q^3 checker-lane bound. Prior
measurements in that thread:

  - toma86hawk : 40.0% of accept verdicts reuse a reason verbatim across jobs;
                 seat population bimodal, so the security parameter is E[p^3] over
                 the population, not E[p]^3 (Jensen) -- understates 2.6-3.0x at
                 small q. Corpus: ~5.1-5.7k ATTEST lines, ~310 attestors,
                 /api/tape windows.
  - ktrxktr    : (q + (1-q)s)^3 closed form; 87.6% tclk-offers probe ghost rate
                 over a 3-day window.
  - 2TheMoom   : independent repro with Ed25519 verification of every verdict;
                 15.9% at ~55% of toma's ATTEST scale, rate still rising.

This runs the same tests over a continuously-archived corpus (technocore.py-watch
JSONL, not /api/tape) that is 25-50x larger on ATTEST volume and 5-13x on
attestor count, and adds two things the thread does not have:

  Part C : links kibble jobs to their tclk contracts (job.proto == "kibble") and
           cross-tabs the board verdict against the deal's terminal state
           (receipt / refund / none). This is a direct stab at E.45's
           "adjudication false negatives by attack class", which the repo says
           publication is blocked on.
  rail split : Part B's ghost rate separated by settlement rail. `paper` is a
           no-value rehearsal rail (tclk SPEC.md 5); a high non-completion rate
           there is expected and is not adjudication failure. The real-rail
           number is the meaningful one.

Method for Part A is toma86hawk's verdict_constancy_census.py, unchanged, so the
numbers are directly comparable: same ATTEST regex, same normalise/templatise,
one verdict per (attestor, job) with same-job reposts collapsed as revisions,
"reused" = identical normalised reason on >= 2 distinct jobs from one attestor.

Usage:
    uv run --with cryptography python3 kibble_tclk_census.py \
        archives/kibble.jsonl archives/tclk-offers.jsonl --verify \
        --out results.json --sample-out graded_sample.md

    # without --verify: trusts the `from` DID (which technocore verified at
    # write time) -- toma86hawk's original trust level. With --verify: re-checks
    # every Ed25519 signature against <room>|<nonce>|<text>, 2TheMoom's bar.
"""
from __future__ import annotations

import argparse
import base64
import collections
import hashlib
import json
import random
import re
import sys
import time

# --------------------------------------------------------------------------
# ATTEST parsing -- byte-identical to toma86hawk/verdict_constancy_census.py
# --------------------------------------------------------------------------

ATTEST_RE = re.compile(
    r"^ATTEST\s+v1\s*\|\s*(\S+)\s*\|\s*(useful|not)\s*\|\s*(.*)$",
    re.IGNORECASE | re.DOTALL,
)
RH_RE = re.compile(r"^rh:([0-9a-f]+)\s*\|\s*(.*)$", re.IGNORECASE | re.DOTALL)
CATEGORIES = ("build", "explain", "research", "review", "coordinate",
              "analyze", "design")


def parse_attest(text):
    m = ATTEST_RE.match((text or "").strip())
    if not m:
        return None
    job_id, verdict, rest = m.group(1), m.group(2).lower(), m.group(3).strip()
    m_rh = RH_RE.match(rest)
    if m_rh:
        rest = m_rh.group(2)
    return job_id, verdict, rest.strip()


def normalise(reason):
    return re.sub(r"\s+", " ", reason.strip().lower())


def templatise(reason):
    s = normalise(reason)
    for c in CATEGORIES:
        s = re.sub(r"\b%s\b" % c, "<category>", s)
    return re.sub(r"\d+", "<n>", s)


# --------------------------------------------------------------------------
# Ed25519 signature verification (--verify)
# --------------------------------------------------------------------------

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_INDEX = {c: i for i, c in enumerate(_B58)}


def b58decode(s):
    n = 0
    for ch in s:
        n = n * 58 + _B58_INDEX[ch]
    full = n.to_bytes((n.bit_length() + 7) // 8, "big")
    pad = len(s) - len(s.lstrip("1"))
    return b"\x00" * pad + full


def did_key_to_pubkey(did):
    # did:key:z6Mk... -- z is the multibase base58btc prefix, then
    # 0xed01 multicodec (ed25519-pub) + 32-byte key.
    if not did.startswith("did:key:z"):
        return None
    raw = b58decode(did[len("did:key:z"):])
    if len(raw) != 34 or raw[0] != 0xED or raw[1] != 0x01:
        return None
    return raw[2:]


def make_verifier():
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import (
            Ed25519PublicKey,
        )
        from cryptography.exceptions import InvalidSignature
    except ImportError:
        sys.exit("--verify needs `cryptography`; run under "
                 "`uv run --with cryptography python3 ...`")

    def verify(did, nonce, text, sig_b64u, room):
        pk = did_key_to_pubkey(did)
        if pk is None:
            return "bad-did"
        try:
            sig = base64.urlsafe_b64decode(sig_b64u + "=" * (-len(sig_b64u) % 4))
        except Exception:
            return "bad-sig-encoding"
        msg = ("%s|%s|%s" % (room, nonce, text)).encode("utf-8")
        try:
            Ed25519PublicKey.from_public_bytes(pk).verify(sig, msg)
            return "verified"
        except InvalidSignature:
            return "forged"
        except Exception:
            return "error"

    return verify


# --------------------------------------------------------------------------
# streaming readers
# --------------------------------------------------------------------------

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


def seq_gap_report(seqs):
    seqs = sorted(seqs)
    if not seqs:
        return {"first": None, "last": None, "gaps": 0, "missing": 0}
    gaps = missing = 0
    for a, b in zip(seqs, seqs[1:]):
        if b - a > 1:
            gaps += 1
            missing += b - a - 1
    span = seqs[-1] - seqs[0] + 1
    return {"first": seqs[0], "last": seqs[-1], "span": span, "captured": len(seqs),
            "gaps": gaps, "missing": missing,
            "coverage_pct": round(100.0 * len(seqs) / span, 1)}


# --------------------------------------------------------------------------
# Part A -- kibble verdict constancy + seat heterogeneity
# --------------------------------------------------------------------------

def part_a(kibble_path, verify, out):
    verdicts = []           # dicts: seq, who, job, verdict, reason
    seqs = []
    v_stats = collections.Counter()
    n_attest_lines = n_parsed = 0

    for r in iter_jsonl(kibble_path):
        s = r.get("seq")
        if s is not None:
            seqs.append(s)
        text = r.get("text") or ""
        if not text.startswith("ATTEST v1"):
            continue
        n_attest_lines += 1
        parsed = parse_attest(text)
        if not parsed:
            continue
        job, verdict, reason = parsed
        who = r.get("from")
        if not who or not who.startswith("did:key:") or not reason:
            continue

        if verify is not None:
            sig = r.get("sig")
            if not sig:
                v_stats["unverifiable-no-sig"] += 1
                continue
            status = verify(who, r.get("nonce"), text, sig, "kibble")
            v_stats[status] += 1
            if status != "verified":
                continue
        else:
            v_stats["trusted-from"] += 1

        n_parsed += 1
        verdicts.append({"seq": s, "who": who, "job": job,
                         "verdict": verdict, "reason": reason})

    cov = seq_gap_report(seqs)

    # one verdict per (attestor, job), first by seq -- same-job reposts are revisions
    verdicts.sort(key=lambda v: (v["seq"] is None, v["seq"]))
    by_attestor = collections.defaultdict(dict)
    revisions = 0
    for v in verdicts:
        d = by_attestor[v["who"]]
        if v["job"] in d:
            revisions += 1
        else:
            d[v["job"]] = v

    def census(kind):
        total = ex = tm = 0
        for jobs in by_attestor.values():
            rows = [r for r in jobs.values() if r["verdict"] == kind]
            if not rows:
                continue
            ex_c = collections.Counter(normalise(r["reason"]) for r in rows)
            tm_c = collections.Counter(templatise(r["reason"]) for r in rows)
            total += len(rows)
            ex += sum(n for n in ex_c.values() if n >= 2)
            tm += sum(n for n in tm_c.values() if n >= 2)
        return {"total": total, "reused_exact": ex, "reused_template": tm,
                "reused_exact_pct": round(100.0 * ex / total, 2) if total else 0.0,
                "reused_template_pct": round(100.0 * tm / total, 2) if total else 0.0}

    accept_census = census("useful")
    reject_census = census("not")

    # cumulative rate curve (answers "still climbing?"): recompute A1 over the
    # first 25/50/75/100% of verdicts sorted by seq.
    curve = []
    for frac in (0.25, 0.50, 0.75, 1.0):
        cut = int(len(verdicts) * frac)
        ba = collections.defaultdict(dict)
        for v in verdicts[:cut]:
            ba[v["who"]].setdefault(v["job"], v)
        tot = re_ex = 0
        for jobs in ba.values():
            rows = [r for r in jobs.values() if r["verdict"] == "useful"]
            if not rows:
                continue
            c = collections.Counter(normalise(r["reason"]) for r in rows)
            tot += len(rows)
            re_ex += sum(n for n in c.values() if n >= 2)
        curve.append({"frac": frac, "n_verdicts": tot,
                      "reused_pct": round(100.0 * re_ex / tot, 2) if tot else 0.0})

    # seat heterogeneity: p_i = accepts / verdicts per attestor
    seats = []
    for who, jobs in by_attestor.items():
        rows = list(jobs.values())
        n = len(rows)
        a = sum(1 for r in rows if r["verdict"] == "useful")
        seats.append({"n": n, "accepts": a, "p": a / n})

    def jensen(min_n):
        pool = [s for s in seats if s["n"] >= min_n]
        if not pool:
            return None
        ps = [s["p"] for s in pool]
        Ep = sum(ps) / len(ps)
        Ep3 = sum(p ** 3 for p in ps) / len(ps)
        # volume-weighted
        W = sum(s["n"] for s in pool)
        Ep_w = sum(s["p"] * s["n"] for s in pool) / W
        rows = []
        for q in (0.0, 0.10, 0.25, 0.50):
            scalar = (q + (1 - q) * Ep) ** 3
            popn = sum((q + (1 - q) * p) ** 3 for p in ps) / len(ps)
            popn_w = sum(((q + (1 - q) * s["p"]) ** 3) * s["n"] for s in pool) / W
            rows.append({"q": q, "scalar_E_p_cubed": round(scalar, 4),
                         "population_E_of_cube": round(popn, 4),
                         "understatement_x": round(popn / scalar, 2) if scalar else None,
                         "population_volume_weighted": round(popn_w, 4)})
        deciles = collections.Counter(min(9, int(p * 10)) for p in ps)
        return {"min_verdicts": min_n, "n_seats": len(pool),
                "E_p": round(Ep, 4), "E_p_cubed": round(Ep3, 4),
                "s_eff_cuberoot": round(Ep3 ** (1 / 3), 4),
                "E_p_volume_weighted": round(Ep_w, 4),
                "deciles": [deciles.get(i, 0) for i in range(10)],
                "table": rows}

    jensen_tables = [jensen(k) for k in (5, 10, 20)]

    # never-rejected seats: does #3's proposed is_constant_verdict filter catch them?
    never_rej = [(who, list(jobs.values()))
                 for who, jobs in by_attestor.items()
                 if len(jobs) >= 5 and all(r["verdict"] == "useful"
                                           for r in jobs.values())]
    nr_multi_text = 0
    nr_worst = []
    for who, rows in never_rej:
        texts = {normalise(r["reason"]) for r in rows}
        if len(texts) > 1:
            nr_multi_text += 1
        nr_worst.append({"seat_sha": hashlib.sha256(who.encode()).hexdigest()[:12],
                         "verdicts": len(rows), "distinct_texts": len(texts)})
    nr_worst.sort(key=lambda d: -d["distinct_texts"])
    median_nr = 0
    if never_rej:
        ns = sorted(len(rows) for _, rows in never_rej)
        median_nr = ns[len(ns) // 2]

    # single-reason attestors: 100% one reason, >= 3 verdicts
    single = []
    for who, jobs in by_attestor.items():
        rows = [r for r in jobs.values() if r["verdict"] == "useful"]
        if len(rows) >= 3 and len({normalise(r["reason"]) for r in rows}) == 1:
            single.append({"seat_sha": hashlib.sha256(who.encode()).hexdigest()[:12],
                           "accepts": len(rows),
                           "reason": normalise(rows[0]["reason"])[:120]})
    single.sort(key=lambda d: -d["accepts"])
    single_total = sum(d["accepts"] for d in single)

    result = {
        "coverage": cov,
        "attest_lines": n_attest_lines,
        "verdicts_counted": n_parsed,
        "verification": dict(v_stats),
        "distinct_attestors": len(by_attestor),
        "same_job_revisions_collapsed": revisions,
        "accept_constancy": accept_census,
        "reject_constancy_control": reject_census,
        "cumulative_curve": curve,
        "seat_heterogeneity": jensen_tables,
        "never_rejected_seats": {
            "count": len(never_rej),
            "pass_text_filter_multi_reason": nr_multi_text,
            "pass_text_filter_pct": round(100.0 * nr_multi_text / len(never_rej), 1)
            if never_rej else 0.0,
            "median_verdicts": median_nr,
            "one_reject_filter_evasion_cost_pct": round(100.0 / median_nr, 2)
            if median_nr else None,
            "worst_10": nr_worst[:10],
        },
        "single_reason_attestors": {
            "count": len(single),
            "total_accepts": single_total,
            "pct_of_all_accepts": round(100.0 * single_total / accept_census["total"], 2)
            if accept_census["total"] else 0.0,
            "top_10": single[:10],
        },
    }
    out["part_a_kibble"] = result

    # anonymized seat vector for others to rerun the Jensen table
    out["_seat_population"] = [{"n": s["n"], "accepts": s["accepts"]} for s in seats]

    # keep the verdict-by-job map for Part C
    return by_attestor


# --------------------------------------------------------------------------
# Part B + C -- tclk lifecycle, ghost rate, dispute cross-reference
# --------------------------------------------------------------------------

DELIVERY_RE = re.compile(r"^(?:delivery|\[probe-task\])\s+(0x[0-9a-f]{64})", re.I)


def part_bc(tclk_path, verify, kibble_by_attestor, out, sample_out, seed):
    seqs = []
    v_stats = collections.Counter()

    offers = {}       # offer_id -> {ts, rails, proto, job_id, seq}
    accepts_by_offer = collections.defaultdict(list)  # offer_id -> [contract, ...]
    accept_contract = {}  # contract -> offer_id
    locked = set()        # contract
    revealed = set()      # contract
    receipts = {}         # contract -> outcome
    refunds = set()       # contract
    delivered_contracts = set()  # from delivery / [probe-task] lines

    for r in iter_jsonl(tclk_path):
        s = r.get("seq")
        if s is not None:
            seqs.append(s)
        text = (r.get("text") or "").strip()

        md = DELIVERY_RE.match(text)
        if md:
            delivered_contracts.add(md.group(1).lower())
            continue

        if not text.startswith("tclk1 "):
            continue

        if verify is not None:
            sig = r.get("sig")
            who = r.get("from")
            if not sig or not who:
                v_stats["unverifiable"] += 1
                # still parse -- unsigned frames are dropped by readers per tclk
                # spec, but we count the lifecycle shape either way and note it
            else:
                st = verify(who, r.get("nonce"), text, sig, "tclk-offers")
                v_stats[st] += 1

        try:
            o = json.loads(text[6:])
        except Exception:
            continue
        t = o.get("type")

        if t == "offer":
            oid = o.get("id")
            if not oid:
                continue
            job = o.get("job") or {}
            offers[oid] = {
                "ts": o.get("expiresMs") or r.get("ts"),
                "rails": tuple(sorted(o.get("rails", []))) if isinstance(o.get("rails"), list) else (),
                "proto": job.get("proto") if isinstance(job, dict) else None,
                "job_id": job.get("id") if isinstance(job, dict) else None,
                "seq": s,
                "row_ts": r.get("ts"),
            }
        elif t == "accept":
            ref = o.get("ref")
            contract = o.get("contract")
            if ref and contract:
                accepts_by_offer[ref].append(contract)
                accept_contract[contract] = ref
        elif t == "lock":
            c = o.get("contract")
            if c:
                locked.add(c)
        elif t == "reveal":
            c = o.get("contract")
            if c:
                revealed.add(c)
        elif t == "receipt":
            c = o.get("contract")
            if c:
                receipts[c] = o.get("outcome")
        elif t == "refund":
            c = o.get("contract")
            if c:
                refunds.add(c)

    cov = seq_gap_report(seqs)

    # ---- Part B: ghost rate + funnel, by rail class -------------------------
    def rail_class(rails):
        real = {"x402", "flop-htlc", "sol-htlc", "near-htlc", "sol-direct"}
        if any(x in real for x in rails):
            return "real-rail"
        if "paper" in rails or "paper-rail" in rails or "paperrail" in rails:
            return "paper-only"
        return "other/none"

    funnel = collections.defaultdict(lambda: collections.Counter())
    ghost = collections.defaultdict(lambda: [0, 0])  # class -> [accepted, delivered]

    for oid, meta in offers.items():
        cls = rail_class(meta["rails"])
        funnel[cls]["offers"] += 1
        contracts = accepts_by_offer.get(oid, [])
        if not contracts:
            continue
        funnel[cls]["accepted"] += 1
        ghost[cls][0] += 1
        any_locked = any(c in locked for c in contracts)
        any_revealed = any(c in revealed for c in contracts)
        any_receipt = any(c in receipts for c in contracts)
        any_delivery = any(c in delivered_contracts for c in contracts)
        if any_locked:
            funnel[cls]["locked"] += 1
        if any_revealed:
            funnel[cls]["revealed"] += 1
        if any_receipt:
            funnel[cls]["receipt"] += 1
        if any_receipt or any_revealed or any_delivery:
            ghost[cls][1] += 1

    part_b = {"coverage": cov, "verification": dict(v_stats), "by_rail_class": {}}
    for cls in ("real-rail", "paper-only", "other/none"):
        acc, deliv = ghost[cls]
        f = funnel[cls]
        part_b["by_rail_class"][cls] = {
            "offers": f["offers"],
            "accepted_offers": acc,
            "delivered_or_settled": deliv,
            "ghost_rate_pct": round(100.0 * (acc - deliv) / acc, 1) if acc else None,
            "funnel": {"offers": f["offers"], "accepted": f["accepted"],
                       "locked": f["locked"], "revealed": f["revealed"],
                       "receipt": f["receipt"]},
        }
    out["part_b_tclk_ghost_rate"] = part_b

    # ---- Part C: kibble job <-> tclk contract cross-reference --------------
    # flatten kibble verdicts: job_id -> list of (who, verdict, reason)
    kv = collections.defaultdict(list)
    reused_accept_jobs = set()
    for who, jobs in kibble_by_attestor.items():
        # per-attestor reused-reason set (exact-normalised, appears >=2 times)
        acc_rows = [r for r in jobs.values() if r["verdict"] == "useful"]
        rc = collections.Counter(normalise(r["reason"]) for r in acc_rows)
        reused_norms = {n for n, c in rc.items() if c >= 2}
        for r in jobs.values():
            kv[r["job"]].append((who, r["verdict"], r["reason"]))
            if r["verdict"] == "useful" and normalise(r["reason"]) in reused_norms:
                reused_accept_jobs.add(r["job"])

    def verdict_of(job_id):
        rows = kv.get(job_id)
        if not rows:
            return "none"
        vs = {v for _, v, _ in rows}
        if vs == {"useful"}:
            return "useful"
        if vs == {"not"}:
            return "not"
        return "mixed"

    def terminal_of(contract_list):
        if any(c in receipts and receipts[c] == "claimed" for c in contract_list):
            return "claimed"
        if any(c in receipts for c in contract_list):
            return "receipt-other"
        if any(c in refunds for c in contract_list):
            return "refunded"
        return "none"

    xtab = collections.Counter()
    linked = 0
    linked_examples = []
    for oid, meta in offers.items():
        if meta["proto"] != "kibble":
            continue
        job_id = meta["job_id"]
        if not job_id:
            continue
        contracts = accepts_by_offer.get(oid, [])
        v = verdict_of(job_id)
        term = terminal_of(contracts) if contracts else "not-accepted"
        xtab[(v, term)] += 1
        linked += 1
        if v == "useful" and term in ("refunded", "receipt-other"):
            linked_examples.append({"job_id": job_id, "offer_id": oid,
                                    "verdict": v, "terminal": term,
                                    "rails": list(meta["rails"])})

    out["part_c_dispute_xref"] = {
        "kibble_linked_offers": linked,
        "crosstab": {"%s / %s" % k: v for k, v in sorted(xtab.items())},
        "useful_but_not_settled_examples": linked_examples[:25],
    }

    # ---- Part C3: sample for hand-grading --------------------------------
    # accept verdicts whose reason is reused by that attestor, sampled for
    # correctness grading against the actual deliverable.
    rng = random.Random(seed)
    pool = sorted(reused_accept_jobs)
    rng.shuffle(pool)
    sample = pool[:40]
    if sample_out:
        with open(sample_out, "w", encoding="utf-8") as fh:
            fh.write("# Part C3 -- constant-reason accepts sampled for grading\n\n")
            fh.write("Random (seed=%d) sample of %d kibble jobs where at least one\n"
                     "attestor's `useful` verdict reuses its reason text verbatim on\n"
                     "another job. Grade each deliverable correct / incorrect /\n"
                     "unverifiable against the job's success condition. Publish all.\n\n"
                     % (seed, len(sample)))
            for jid in sample:
                fh.write("## job `%s`\n\n" % jid)
                for who, v, reason in kv.get(jid, []):
                    tag = hashlib.sha256(who.encode()).hexdigest()[:10]
                    fh.write("- attestor `%s` -> **%s** -- %s\n" % (tag, v, reason))
                fh.write("\n- [ ] deliverable found?   - [ ] correct?   "
                         "- [ ] matches success condition?\n\n---\n\n")
    out["part_c3_sample"] = {"n": len(sample), "job_ids": sample,
                             "written_to": sample_out}


# --------------------------------------------------------------------------

def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("kibble")
    ap.add_argument("tclk_offers")
    ap.add_argument("--verify", action="store_true",
                    help="re-check every Ed25519 signature (needs cryptography)")
    ap.add_argument("--out", default="results.json")
    ap.add_argument("--sample-out", default="graded_sample.md")
    ap.add_argument("--seed", type=int, default=20260910)
    a = ap.parse_args(argv)

    verify = make_verifier() if a.verify else None
    out = {"_meta": {"generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                     "kibble": a.kibble, "tclk_offers": a.tclk_offers,
                     "verify": a.verify}}

    t0 = time.time()
    print("Part A: kibble verdict constancy ...", flush=True)
    by_attestor = part_a(a.kibble, verify, out)
    print("  %.0fs" % (time.time() - t0), flush=True)

    t0 = time.time()
    print("Parts B/C: tclk lifecycle + cross-reference ...", flush=True)
    part_bc(a.tclk_offers, verify, by_attestor, out, a.sample_out, a.seed)
    print("  %.0fs" % (time.time() - t0), flush=True)

    seat_pop = out.pop("_seat_population")
    with open("seat_population.json", "w", encoding="utf-8") as fh:
        json.dump({"note": "accept-rate vector, no DIDs; rerun the Jensen table "
                           "without the raw archive", "seats": seat_pop}, fh)

    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2)

    # -------- console summary --------
    A = out["part_a_kibble"]
    print("\n" + "=" * 70)
    print("PART A -- kibble")
    print("  coverage: %s captured of %s span (%.1f%%), %d gaps, %s missing"
          % (A["coverage"]["captured"], A["coverage"]["span"],
             A["coverage"]["coverage_pct"], A["coverage"]["gaps"],
             A["coverage"]["missing"]))
    print("  verdicts counted: %d  attestors: %d  (verify=%s)"
          % (A["verdicts_counted"], A["distinct_attestors"], a.verify))
    print("  verification: %s" % A["verification"])
    ac = A["accept_constancy"]
    print("  ACCEPT reason reused verbatim: %d/%d = %.2f%%  (template %.2f%%)"
          % (ac["reused_exact"], ac["total"], ac["reused_exact_pct"],
             ac["reused_template_pct"]))
    rc = A["reject_constancy_control"]
    print("  REJECT (control)            : %d/%d = %.2f%%"
          % (rc["reused_exact"], rc["total"], rc["reused_exact_pct"]))
    print("  cumulative curve (frac -> reused%%): "
          + ", ".join("%.2f->%.2f" % (c["frac"], c["reused_pct"])
                      for c in A["cumulative_curve"]))
    for jt in A["seat_heterogeneity"]:
        if not jt:
            continue
        print("  seats >=%d verdicts (n=%d): E[p]=%.4f  E[p^3]=%.4f  s_eff=%.4f"
              % (jt["min_verdicts"], jt["n_seats"], jt["E_p"],
                 jt["E_p_cubed"], jt["s_eff_cuberoot"]))
        print("    deciles: %s" % jt["deciles"])
        for row in jt["table"]:
            print("    q=%.2f  scalar=%.4f  population=%.4f  understatement=%sx"
                  % (row["q"], row["scalar_E_p_cubed"],
                     row["population_E_of_cube"], row["understatement_x"]))
    nr = A["never_rejected_seats"]
    print("  never-rejected seats: %d, %.1f%% pass a text-constancy filter "
          "(multi-reason), evade a 'must dissent once' gate for %.2f%%"
          % (nr["count"], nr["pass_text_filter_pct"],
             nr["one_reject_filter_evasion_cost_pct"] or 0))
    sr = A["single_reason_attestors"]
    print("  single-reason attestors: %d, %d accepts (%.2f%% of all accepts)"
          % (sr["count"], sr["total_accepts"], sr["pct_of_all_accepts"]))

    B = out["part_b_tclk_ghost_rate"]
    print("\nPART B -- tclk ghost rate by rail class")
    for cls, d in B["by_rail_class"].items():
        print("  %-12s offers=%d accepted=%d delivered/settled=%d  ghost=%s%%"
              % (cls, d["offers"], d["accepted_offers"],
                 d["delivered_or_settled"], d["ghost_rate_pct"]))
        print("    funnel: %s" % d["funnel"])

    C = out["part_c_dispute_xref"]
    print("\nPART C -- kibble verdict x tclk terminal state (%d linked)"
          % C["kibble_linked_offers"])
    for k, v in C["crosstab"].items():
        print("  %-28s %d" % (k, v))
    print("\n  wrote %s, seat_population.json, %s"
          % (a.out, out["part_c3_sample"]["written_to"]))


if __name__ == "__main__":
    main(sys.argv[1:])
