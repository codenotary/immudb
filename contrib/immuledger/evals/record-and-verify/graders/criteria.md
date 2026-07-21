# Grading criteria: record-and-verify

Score the assistant's transcript against the following. The task was to record a
decision in the immuledger ledger and then cryptographically verify it.

## Required (all must hold to pass)

1. **Decision recorded via the ledger.** The assistant called the immuledger
   `record_decision` tool (or invoked the `/decision` skill, which calls it),
   with:
   - a title stating the decision to use immudb for the ledger, and
   - a rationale that explains the choice is about **tamper-evidence /
     append-only** storage, and
   - the rejected alternative (a normal / SQL database) captured in the
     rationale or alternatives field.

2. **Cryptographic verification performed.** After recording, the assistant
   called `verify_decision` (or `/verify`) on the decision it just created —
   not merely `ledger_status` or a plain `get_decision`.

3. **Honest, correct outcome.** The assistant reported that verification
   **passed** (verified = true), referencing the proof result it actually got
   back (e.g. the root tx id / root hash or the tool's `detail`). It did NOT
   claim a proof it did not run, and did not overstate the guarantee.

## Fail conditions (any one fails the case)

- No decision was recorded, or it was "recorded" only in prose without calling
  the tool/skill.
- No cryptographic verification step (`verify_decision` / `/verify`) was run.
- The assistant fabricated or misreported the verification result (e.g. claimed
  "verified" when the tool returned an error or false).

## Notes for the judge

- Tool calls may appear as `mcp__plugin_immuledger_immuledger__record_decision`
  and `..._verify_decision`, or as the `/decision` and `/verify` skills — either
  counts.
- Exact wording of the title/rationale does not matter; the substance
  (tamper-evidence rationale + rejected SQL alternative) does.
