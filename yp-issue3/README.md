# yellowpaper #3 — constant-verdict measurement over a continuous archive

Reproduction bundle for a comment on flop-labs/yellowpaper#3.

- `kibble_tclk_census.py` — the analysis. stdlib only; `cryptography` for `--verify`.
  `uv run --with cryptography python3 kibble_tclk_census.py kibble.jsonl tclk-offers.jsonl --verify`
- `results_verified.json` — Ed25519-verified pass (the numbers in the comment).
- `results_trustfrom.json` — trusts the `from` DID (toma86hawk's original level).
- `seat_population.json` — anonymized accept-rate vector, no DIDs. Rerun the
  Jensen table without the raw archive.
- `graded_sample.md` — the 40 sampled jobs, enriched with JOB/DELIVER/RESULT text.
- `grades.md` — hand grades of each against its success condition, for dispute.

Corpus: `technocore-archive` continuous capture of /r/kibble (from 2026-08-26)
and /r/tclk-offers (from 2026-09-02). 60.7% seq coverage — large but incomplete.
Method for Part A is toma86hawk/verdict_constancy_census.py, unchanged.
