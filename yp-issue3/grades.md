# graded_sample results — 39 of 40 (job 1 truncated in transfer)

Method: each sampled job is one where at least one attestor's `useful` verdict
reused its reason text verbatim on a different job. For each, the deliverable(s)
that carry a `useful` verdict were graded by hand against the job's stated
success condition. Deliverables were read at up to ~1000 characters (see
`grading_enriched.md` for the exact text scored). Grades are the author's
judgement and are published here for dispute.

Grade key — OK: meets the success condition · OK*: a correct deliverable exists
but the constant-reason verdict's target is ambiguous · PARTIAL: some but not all
success elements · FAIL: boilerplate, off-topic, or fabricated content carrying a
`useful` verdict · N/A: JOB/DELIVER not retained in the archive window.

| # | job | grade | note |
|---|-----|-------|------|
| 2 | k9d635433a5 | FAIL | constant-reason `useful` on "formal domain evaluation" boilerplate; a real 3-step answer existed but is not what was accepted |
| 3 | kd718127781 | N/A | JOB + DELIVER not in archive window |
| 4 | kfaf3f1ff51 | FAIL | "system operates through layered abstraction" boilerplate; no benchmark |
| 5 | kdbfe5192f4 | OK* | a correct answer (WMT) exists; 5 other attestors rejected the filler results; constant reason uninformative as to target |
| 6 | kb5b5971769 | FAIL | `useful` reason describes an OBI answer; job asked for a cross-exchange VWAP quorum feed (a good VWAP deliverable exists, unattested by the constant-reason seat) |
| 7 | kfb0586c835 | FAIL | "Explaining X: layered abstraction" boilerplate; no batch verification |
| 8 | ka5393bc967 | FAIL | success condition is the number 8200; no result contains it; one result is about HTTP method idempotency |
| 9 | k19beeeb901 | FAIL | 8x "attention matrix verified, compute hash: 0x..." + FLOP boilerplate |
| 10 | kcbef70d7d7 | FAIL | "interconnected factors" boilerplate; the `not` attestor caught it |
| 11 | k79ff001365 | FAIL | canned "Ed25519 uses Curve25519..." paragraph; job asked about Byzantine fault resilience under 400ms partitions |
| 12 | k82cdceee24 | OK | genuine audit conclusion correctly identifies the false "asynchronous CPI" premise |
| 13 | k5139f5ae74 | OK | names comparison metric (checksum/parity) + reconciliation (sidecar, offline log) |
| 14 | k9d640828a1 | FAIL | "key principles understood through practical examples" boilerplate |
| 15 | k3d36ec2c34 | FAIL | same boilerplate as job 4; "Verified criteria met" |
| 16 | kce149d9aff | OK | correct two-level cuckoo derivation, cites Pagh-Rodler, O(1) amortized argument |
| 17 | kd55f3787ac | FAIL | `useful` reason is about the canned Ed25519 paragraph; job asked for Sybil PageRank (an honest methodology answer exists, unattested) |
| 18 | kd5a8a47362 | FAIL | no commit date / issue count / verdict; success condition unmet |
| 19 | k2830a15c08 | OK | names privilege boundary (isolated backoff scheduler) + runtime validation (both Retry-After formats, clamp) |
| 20 | k1b309aa856 | OK | expert K8s answer; correctly notes Argo Rollouts has no native StatefulSet canary; second attestor quotes the content |
| 21 | k9d255fdcf9 | OK | on-topic Pact + Hypothesis + named invariants; plan as requested |
| 22 | kbb08e6968e | FAIL | `useful` reason is the canned Ed25519 paragraph again; job asked for Sybil PageRank |
| 23 | keccfc6fe9c | FAIL | FLOP-ecosystem boilerplate; "Verified criteria met" |
| 24 | k69a2e3e5c6 | PARTIAL | 2 of 3 success elements (franchise + bootstrap RESULT; missing the passport field name) |
| 25 | k2e26c306e8 | OK | identifies backpressure / credit-based flow control + Little's Law throttling |
| 26 | k92e7d9efcb | OK | all 3 success elements; boolean `franchise` passport field named |
| 27 | k9c76e9b800 | OK | correct STARK AIR matmul transition/boundary constraints (a Docker RESULT also present, not attested) |
| 28 | kba689ffe6c | OK* | good Sybil answer names Worldcoin + Tezos (real deployments); one attestor's reason misdescribes it as replay-protection |
| 29 | k4385180a71 | OK | correct tail-latency answer with overload / noisy-neighbor examples (5 identical constant reasons, 1 rh, but the content is right) |
| 30 | k45dc80af6c | FAIL | `useful` on an EVM gas-cost paragraph (C_mem = 3a + a^2/512) for a vector-embeddings build |
| 31 | k480506d49f | PARTIAL | genuine expert design (PipeMutator, data-aware mutation operators); no prototype, which the success condition required |
| 32 | kbf926f88f4 | FAIL | fabricated "EPM standards" values (two deliverables, different invented numbers); 2 other attestors flagged it |
| 33 | kc1ac9e26c2 | OK | correct AIR/FRI matmul construction over F_p |
| 34 | k6e0e77e983 | OK | correct scope 1/2/3 emissions ordering with definitions |
| 35 | k301d0e8b92 | FAIL | "formal domain evaluation" boilerplate; no failures / successes / PDF link / page numbers |
| 36 | ke0e8b13894 | FAIL | filler + calls CPI "Call Present Instruction" (it is Cross-Program Invocation) |
| 37 | kb55f218e14 | OK | correct OBI equation + quote-queue exhaustion, per the exact success condition |
| 38 | k99c7366df0 | FAIL | "I will follow these steps: 1. Build and Run..." — a plan, no benchmark; a non-constant attestor caught it |
| 39 | kcdd72df188 | FAIL | boilerplate + "this topic relates to the FLOP/Technocore ecosystem" for a Turkish vowel-harmony question |
| 40 | kefcf23f599 | OK | correct resolve -> route -> cache per-hop answer with DNS / service-discovery detail |

**Tally:** OK 14, OK* 2, PARTIAL 2, FAIL 20, N/A 1.

20 of 39 (51%) do not meet the job's success condition. ~16 (41%) are correct or
largely so. The constant-reason accept verdict is close to independent of whether
the deliverable is any good.

Recurring patterns:
- Canned cross-job answers rubber-stamped: "Ed25519 uses Curve25519 in twisted
  Edwards form..." accepted for jobs 11, 17, 22; "Explaining X: the system
  operates through layered abstraction..." for jobs 4, 7, 15, 36; "Technical
  deliverable for [X]: Conducted formal domain evaluation..." for jobs 2, 8, 32, 35.
- Fabrication accepted: job 32 (invented "European Perceptual Measurement"
  standard with specific numbers).
- Topic mismatch accepted: jobs 6, 8, 11, 17, 22, 30 — the `useful` reason
  describes content answering a different question than the job asked.
- The `not` lane was accurate on nearly every checkable case.
