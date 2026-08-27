# Raw reviewer output — evidence, not the tracking record

Unedited reports from the adversarial review campaign. They are kept because a
finding's original wording and reasoning are worth preserving (including the
claims that were later refuted), but they are **not** the operational record:
that is [../../SECURITY-AUDIT.md](../../SECURITY-AUDIT.md), which carries the
per-finding status, commit and test.

Read them with that in mind — roughly half of what is claimed here was refuted
on verification, and the reports do not know that. Numbers quoted inside them
are as-of their own run and may be superseded (the conformance figures in
particular: see `conformance/` for dated, reproducible results).

- `CODEX-REVIEW.md` — codex `gpt-5.6-sol`, first pass.
- `CODEX-REVIEW-FINAL.md` — codex independent cross-check of the fixed tree.
- `CODEX-REVIEW-R9.md` — codex round 9, first pass over `config.d`/`app.d`.
- `GLM-CTF-REPORT.md`, `-R2`..`-R9` — GLM-5.3, nine passes.
- `PROJECT-CRITIQUE.md` — codex, product/process critique rather than defects.
