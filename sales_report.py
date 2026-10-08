#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""Summarize archives/sales.jsonl (written by archive_api.py's _SalesLog):
sales and revenue per endpoint, per day, and per payer.

    uv run sales_report.py                 # everything
    uv run sales_report.py --days 7        # last 7 days
"""
from __future__ import annotations

import argparse
import collections
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--file", type=Path, default=Path(__file__).parent / "archives" / "sales.jsonl")
    ap.add_argument("--days", type=int, help="only the last N days")
    a = ap.parse_args()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=a.days)).strftime("%Y-%m-%dT%H:%M:%SZ") if a.days else ""
    rows = []
    try:
        with open(a.file, encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if r.get("ts", "") >= cutoff:
                    rows.append(r)
    except FileNotFoundError:
        print(f"no sales logged yet ({a.file} doesn't exist)")
        return
    if not rows:
        print("no sales in that range")
        return

    def usd(r):
        if r.get("amount_usdc") is not None:
            return r["amount_usdc"]
        try:
            return float(str(r.get("price", "0")).lstrip("$"))
        except ValueError:
            return 0.0

    total = sum(map(usd, rows))
    print(f"{len(rows)} sales, ${total:.4f} USDC  ({rows[0]['ts']} .. {rows[-1]['ts']})\n")
    for title, key in (("by endpoint", lambda r: r.get("endpoint")),
                       ("by day", lambda r: r.get("ts", "")[:10]),
                       ("by payer", lambda r: r.get("payer"))):
        agg = collections.defaultdict(lambda: [0, 0.0])
        for r in rows:
            agg[key(r)][0] += 1
            agg[key(r)][1] += usd(r)
        print(title)
        order = sorted(agg.items()) if title == "by day" else sorted(agg.items(), key=lambda kv: -kv[1][1])
        for k, (n, v) in order:
            print(f"  {n:5d}  ${v:8.4f}  {k}")
        print()


if __name__ == "__main__":
    main()
