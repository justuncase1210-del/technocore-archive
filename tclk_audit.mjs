#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
//
// Lenient tclk transcript auditor. Unlike tclk's own parseTranscriptExport (which fails
// the WHOLE parse on one malformed line -- exactly the bug hit against the real, shared
// tclk-offers room this was built to audit), this skips unparseable lines individually
// and folds whatever remains with tclk's own (already-robust) foldTranscript.
//
// Also fixes a real precision bug: JSON.parse loses precision on integers beyond 2^53,
// which corrupts large nonces used in signature verification. Nonces are extracted by
// regex on the raw line text instead, never round-tripped through a JS Number.
//
// Usage: node tclk_audit.mjs <contract-id>
//   Reads JSONL on stdin: each line is a raw technocore message object with an
//   added "_room" field naming which room it came from.
//   Prints one JSON object to stdout: { final, steps, skipped, dealRoom }

import { foldTranscript, dealRoom } from "./tclk/dist/index.js";

const NONCE_RE = /"nonce"\s*:\s*(?:"([^"]*)"|(-?\d+))/;

function extractNonce(rawLine) {
  const m = NONCE_RE.exec(rawLine);
  if (!m) return null;
  return m[1] !== undefined ? m[1] : m[2];
}

function leniently(rawLine) {
  let obj;
  try {
    obj = JSON.parse(rawLine);
  } catch {
    return { ok: false, reason: "not JSON" };
  }
  if (typeof obj !== "object" || obj === null) return { ok: false, reason: "not an object" };
  const { _room, seq, ts, from, text, sig } = obj;
  if (typeof _room !== "string") return { ok: false, reason: "missing _room tag" };
  if (!Number.isSafeInteger(seq) || seq < 0) return { ok: false, reason: "bad seq" };
  const timestampMs = Date.parse(ts);
  if (!Number.isFinite(timestampMs)) return { ok: false, reason: "bad timestamp" };
  if (typeof from !== "string") return { ok: false, reason: "missing from" };
  if (typeof text !== "string") return { ok: false, reason: "missing text" };
  const nonce = extractNonce(rawLine);
  const signature = typeof sig === "string" ? sig : null;
  return {
    ok: true,
    record: { room: _room, seq, timestampMs, sender: from, nonce, signature, line: text },
  };
}

async function main() {
  const contract = process.argv[2];
  if (!contract) {
    console.error("usage: node tclk_audit.mjs <contract-id>");
    process.exit(1);
  }
  let expectedDealRoom;
  try {
    expectedDealRoom = dealRoom(contract);
  } catch (e) {
    console.log(JSON.stringify({ error: e instanceof Error ? e.message : String(e) }));
    return;
  }

  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  const input = Buffer.concat(chunks).toString("utf8");

  const records = [];
  let skipped = 0;
  for (const rawLine of input.split("\n")) {
    if (rawLine.trim() === "") continue;
    const result = leniently(rawLine);
    if (!result.ok) {
      skipped += 1;
      continue;
    }
    records.push(result.record);
  }
  records.sort((a, b) => a.timestampMs - b.timestampMs || a.seq - b.seq);

  const fold = foldTranscript(records);
  console.log(JSON.stringify({
    dealRoom: expectedDealRoom,
    recordsFolded: records.length,
    skipped,
    final: fold.state,
    steps: fold.steps,
  }));
}

main().catch((e) => {
  console.log(JSON.stringify({ error: e instanceof Error ? e.message : String(e) }));
  process.exit(1);
});
