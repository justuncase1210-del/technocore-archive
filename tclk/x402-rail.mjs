// SPDX-License-Identifier: Apache-2.0
//
// A non-custodial x402 settlement rail for tclk/1: lock() signs an EIP-3009
// transferWithAuthorization LOCALLY and returns it (JSON+base64url) as `ref` --
// no funds move, no server holds anything. claim() verifies the tclk secret
// against the hash-lock statement, then submits the stored authorization to a
// settlement-relay endpoint (this project's own x402 server, which already has
// a proven, tested CDP facilitator client) for real settlement. refund() is a
// pure no-op: since nothing was ever escrowed, there is nothing to reclaim --
// the unused, now-expired authorization simply becomes inert.
//
// DID <-> EVM address resolution: tclk identities are did:key (Ed25519); x402
// payments need secp256k1 EVM addresses -- there is no valid cryptographic
// mapping between the two curves. This rail requires each party to publish
// "x402-addr:0x..." in their own technocore DID note, the exact same
// publish-a-token-in-your-DID-note convention tclk itself already uses for
// capabilityToken/parseCapabilityToken (see technocore.ts), and resolves it
// with a plain read.
//
// Wire format matches this project's own x402 Python SDK exactly (verified
// against x402.mechanisms.evm.eip712 / .types): EIP-712 domain {name:"USD
// Coin", version:"2", chainId:8453, verifyingContract:<Base USDC>},
// TransferWithAuthorization{from,to,value,validAfter,validBefore,nonce}.
//
// secp256k1 signing note: @noble/curves signs sha256(message) by default --
// our `digest` is already the final EIP-712/EIP-191 hash, so every sign/
// recover call here passes {prehash:false}, and noble's native 0/1 recovery
// bit is converted to Ethereum's v=27/28 at the wire boundary (the universal
// convention every EVM signing library uses on top of a raw ECDSA primitive).

import { keccak_256 } from "@noble/hashes/sha3.js";
import { secp256k1 } from "@noble/curves/secp256k1.js";
import { verifySecret } from "./dist/index.js";

const BASE_MAINNET_USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913";
const BASE_CHAIN_ID = 8453n;

function keccakOfText(s) {
  return keccak_256(new TextEncoder().encode(s));
}

const DOMAIN_TYPEHASH = keccakOfText(
  "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
);
const AUTH_TYPEHASH = keccakOfText(
  "TransferWithAuthorization(address from,address to,uint256 value,uint256 validAfter,uint256 validBefore,bytes32 nonce)",
);

function hexToBytes(hex) {
  const clean = hex.startsWith("0x") ? hex.slice(2) : hex;
  const out = new Uint8Array(clean.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  return out;
}

function bytesToHex(bytes) {
  return "0x" + Array.from(bytes).map((b) => b.toString(16).padStart(2, "0")).join("");
}

function concatBytes(...parts) {
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
}

function encodeAddress(addr) {
  const clean = hexToBytes(addr);
  const out = new Uint8Array(32);
  out.set(clean, 32 - clean.length);
  return out;
}

function encodeUint256(value) {
  const out = new Uint8Array(32);
  let v = BigInt(value);
  for (let i = 31; i >= 0; i--) {
    out[i] = Number(v & 0xffn);
    v >>= 8n;
  }
  return out;
}

function hashDomain() {
  return keccak_256(
    concatBytes(
      DOMAIN_TYPEHASH,
      keccakOfText("USD Coin"),
      keccakOfText("2"),
      encodeUint256(BASE_CHAIN_ID),
      encodeAddress(BASE_MAINNET_USDC),
    ),
  );
}

function hashAuthorization(auth) {
  return keccak_256(
    concatBytes(
      AUTH_TYPEHASH,
      encodeAddress(auth.from),
      encodeAddress(auth.to),
      encodeUint256(auth.value),
      encodeUint256(auth.validAfter),
      encodeUint256(auth.validBefore),
      hexToBytes(auth.nonce),
    ),
  );
}

/** EIP-191 "\x19\x01" prefix + domain separator + struct hash -- the exact 32-byte digest an EIP-712 signer signs. */
function digestToSign(auth) {
  return keccak_256(concatBytes(new Uint8Array([0x19, 0x01]), hashDomain(), hashAuthorization(auth)));
}

/**
 * Sign `digest` (already-hashed) with a raw secp256k1 private key. Returns a
 * 65-byte Ethereum-style (r,s,v=27/28) hex signature.
 *
 * Byte-layout note: noble's own 'recovered' format is [recoveryByte, r, s]
 * (recovery FIRST -- confirmed against node_modules/@noble/curves/abstract/
 * weierstrass.js Signature.toBytes(), not just the .d.ts comment, which was
 * ambiguous about order and initially misread as [r, s, recoveryByte]).
 * Ethereum's wire convention is [r, s, v] with v=27/28, so this function
 * reorders and re-bases the recovery byte at the boundary.
 */
export function signDigestEthereum(digest, privateKeyHex) {
  const priv = hexToBytes(privateKeyHex);
  const sig = secp256k1.sign(digest, priv, { prehash: false, format: "recovered", lowS: true });
  const nobleRecovery = sig[0]; // noble's native 0/1 bit, FIRST byte
  const rs = sig.slice(1, 65);
  const v = nobleRecovery + 27;
  return bytesToHex(concatBytes(rs, new Uint8Array([v])));
}

/**
 * Recover the signer's EVM address from a 65-byte Ethereum-style (r,s,v=27/28)
 * signature over `digest`.
 *
 * Uses the class-based Signature.fromBytes(...).recoverPublicKey(...) path,
 * not the top-level secp256k1.recoverPublicKey() function -- confirmed by
 * direct testing that the top-level function does not recover correctly in
 * this call pattern even with {prehash:false} explicitly set (a real,
 * reproducible discrepancy from its own .d.ts documentation in this
 * installed version), while the class-based path recovers correctly every
 * time. Do not swap this back without re-verifying against a known keypair.
 */
export function recoverAddressEthereum(digest, signatureHex) {
  const sigBytes = hexToBytes(signatureHex);
  const rs = sigBytes.slice(0, 64);
  const v = sigBytes[64];
  const nobleRecovery = v - 27; // back to noble's native 0/1 bit
  const nobleSig = concatBytes(new Uint8Array([nobleRecovery]), rs); // [recoveryByte, r, s]
  const sigParsed = secp256k1.Signature.fromBytes(nobleSig, "recovered");
  const pubKey = sigParsed.recoverPublicKey(digest).toBytes(false); // uncompressed, 65 bytes: 0x04 + x(32) + y(32)
  const addressBytes = keccak_256(pubKey.slice(1)).slice(-20);
  return bytesToHex(addressBytes);
}

function encodeRef(stored) {
  return "x402lock1." + Buffer.from(JSON.stringify(stored), "utf8").toString("base64url");
}

function decodeRef(ref) {
  if (!ref.startsWith("x402lock1.")) throw new Error("x402-rail: not an x402 lock ref");
  return JSON.parse(Buffer.from(ref.slice("x402lock1.".length), "base64url").toString("utf8"));
}

/**
 * Non-custodial x402 settlement rail. See file header for the design.
 * Implements tclk's SettlementRail interface (id, lock, verifyLock, claim, refund).
 */
export class X402Rail {
  id = "x402";

  /**
   * @param {object} opts
   * @param {{privateKeyHex: string, address: string}} opts.signer - this rail
   *   instance's own EVM signer (used only when this side is the payer, in lock()).
   * @param {(did: string) => Promise<string>} opts.resolveEvmAddress - resolve a
   *   tclk did:key to its EVM address (e.g. read "x402-addr:0x..." from that DID's
   *   technocore note).
   * @param {string} opts.settleUrl - this project's settlement-relay endpoint.
   */
  constructor(opts) {
    this.signer = opts.signer;
    this.resolveEvmAddress = opts.resolveEvmAddress;
    this.settleUrl = opts.settleUrl;
  }

  async lock(terms) {
    if (terms.asset !== "USDC") {
      throw new Error(`x402-rail: only USDC is wired up, got asset=${terms.asset}`);
    }
    const payerAddr = await this.resolveEvmAddress(terms.payer);
    const payeeAddr = await this.resolveEvmAddress(terms.payee);
    if (payerAddr.toLowerCase() !== this.signer.address.toLowerCase()) {
      throw new Error("x402-rail: lock() must be called by the payer's own signer");
    }
    const nonceBytes = new Uint8Array(32);
    crypto.getRandomValues(nonceBytes);
    const authorization = {
      from: payerAddr,
      to: payeeAddr,
      value: terms.amount,
      validAfter: "0",
      validBefore: String(Math.floor(terms.claimByMs / 1000)),
      nonce: bytesToHex(nonceBytes),
    };
    const digest = digestToSign(authorization);
    const signature = signDigestEthereum(digest, this.signer.privateKeyHex);
    const stored = {
      authorization,
      signature,
      contract: terms.contract,
      statement: terms.statement,
      claimByMs: terms.claimByMs,
      refundAfterMs: terms.refundAfterMs,
    };
    return encodeRef(stored);
  }

  async verifyLock(terms, ref) {
    let stored;
    try {
      stored = decodeRef(ref);
    } catch {
      return false;
    }
    if (stored.contract !== terms.contract) return false;
    if (stored.authorization.value !== terms.amount) return false;
    if (stored.claimByMs !== terms.claimByMs) return false;
    if (stored.refundAfterMs !== terms.refundAfterMs) return false;
    const payerAddr = await this.resolveEvmAddress(terms.payer);
    const payeeAddr = await this.resolveEvmAddress(terms.payee);
    if (stored.authorization.from.toLowerCase() !== payerAddr.toLowerCase()) return false;
    if (stored.authorization.to.toLowerCase() !== payeeAddr.toLowerCase()) return false;
    const digest = digestToSign(stored.authorization);
    const recovered = recoverAddressEthereum(digest, stored.signature);
    return recovered.toLowerCase() === payerAddr.toLowerCase();
  }

  async claim(ref, secret) {
    const stored = decodeRef(ref);
    if (Date.now() >= stored.claimByMs) {
      throw new Error("x402-rail: claim after claimByMs");
    }
    // Defense in depth -- tclk's state machine already verified this before
    // invoking claim() on any rail, matching MemoryRail's own behavior.
    const opensHash = verifySecret("hash", stored.statement, secret);
    const opensPoint = !opensHash && verifySecret("point", stored.statement, secret);
    if (!opensHash && !opensPoint) {
      throw new Error("x402-rail: secret does not open the statement");
    }
    const res = await fetch(this.settleUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ authorization: stored.authorization, signature: stored.signature }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`x402-rail: settlement relay refused: ${res.status} ${body}`);
    }
  }

  async refund(_ref) {
    // Non-custodial: the authorization was never submitted, so there is
    // nothing to reclaim. No-op by design -- see file header.
  }
}
