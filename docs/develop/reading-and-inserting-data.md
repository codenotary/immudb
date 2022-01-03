# Reading and inserting data

The format for writing and reading data is the same both in Set and VerifiedSet, just as it is for reading data both in both Get and VerifiedGet.

The only difference is that VerifiedSet returns proofs needed to mathematically verify that the data was not tampered. Note that generating that proof has a slight performance impact, so primitives are allowed without the proof. It is still possible get the proofs for a specific item at any time, so the decision about when or how frequently to do checks (with the Verify version of a method) is completely up to the user. It's possible also to use dedicated auditors to ensure the database consistency, but the pattern in which every client is also an auditor is the more interesting one.

#### Get and set <a href="#get-and-set" id="get-and-set"></a>

#### Get at and since a transaction <a href="#get-at-and-since-a-transaction" id="get-at-and-since-a-transaction"></a>

You can retrieve a key on a specific transaction with `VerifiedGetAt` and since a specific transaction with `VerifiedGetSince`.

#### Transaction by index <a href="#transaction-by-index" id="transaction-by-index"></a>

It's possible to retrieve all the keys inside a specific transaction.

#### Verified transaction by index <a href="#verified-transaction-by-index" id="verified-transaction-by-index"></a>

It's possible to retrieve all the keys inside a specific verified transaction.
