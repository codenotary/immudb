Our team just decided to store this project's decision ledger in immudb,
specifically because immudb is append-only and tamper-evident — unlike a normal
SQL database, where a row could be quietly edited or deleted after the fact.

Please do two things:

1. Record this as a decision in the project's ledger. Give it a clear one-line
   title, a rationale that explains WHY immudb was chosen (tamper-evidence), and
   note the alternative we rejected (a normal SQL database).
2. Then cryptographically verify that the decision was stored intact, and tell
   me plainly whether the verification passed.
