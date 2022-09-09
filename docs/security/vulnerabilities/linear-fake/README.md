# Linear fake vulnerability

## Issue description

immudb uses Merkle Tree enhanced with additional linear part to perform
consistency proofs between two transactions. The linear part is built from
the last leaf node of the Merkle Tree compensating for transactions
that were not yet *consumed* by the Merkle Tree calculation.

The Merkle Tree part is then used to perform proofs for things that are
in transaction range covered by the Merkle Tree where the linear part
is used to check those that are not yet in the Merkle Tree.

When doing consistency checks between two immudb transactions, the linear proof
part was not fully checked. In fact only the first entry (last Merkle Tree leaf)
and the last entry (current DB state value) of the linear part of the older transaction
that were *consumed* by the Merkle Tree of the newer transaction were checked against
the new Merkle Tree without ensuring that elements in the middle of that chain are correctly
added as leafs in the new Merkle Tree.

This lack of check means that the database can present different set of
hashes for transactions on the linear proof part to what would later be used once those
become part of the Merkle Tree. This property can be exploited by the database
to expose two different transaction contents depending on the previously known state
that the user requested consistency proof for.

In practice this could lead to the following scenario:

* a client requests a verified write operation
* the server responds with a proof for the transaction
* client stores the state value retrieved from the server and
  expects it to be a confirmation of that write and all the history
  of the database before that transaction
* a series of validated read / write operations is performed by the client,
  each accompanied by a successfully validated consistency proof and
  update of the client state
* the client requests verified get operation on the transaction it
  has written before (and that was verified with a proof from the server)
* the server replies with a completely different transaction that can
  be properly validated according to the currently stored db state on the
  client side

## Mitigation

### Short term

The issue can only be exploited when the length of the linear proof outside of the
Merkle Tree is longer than 1 node.

In order to protect against such an attack, the client library must additionally
check that all the nodes inside the linear proof from the trusted state are properly
consumed by the Merkle Tree of the updated state. The updated immudb server adds additional
`LinearAdvance` proof to the main `DualProof` that contains inclusion proofs for all the
entries in the consumed part of the linear proof.

Note that this additional `LinearAdvance` proof is not needed if the length of the consumed
linear proof part is no longer than 1 node which will be true for all transactions committed
with a normal immudb server in version 1.3.2 and above.

#### Validation algorithm of the additional LinearAdvance proof

To validate the proof, all entries on the consumed linear part are checked against the
target Merkle Tree. Those nodes are computed by performing a walk on the linear proof
that must lead to the last node that we check against.

```plain

# 1. Calculate the range of transactions to check

startTx := oldState.blTxID
endTx := min(oldState.txID, newState.BlTxID)

# 2. Check if the additional proof has to be performed

if endTxID <= startTxID+1 {
  // Linear Advance Proof is not needed, the consumed linear proof part
  // is short enough to be covered by other checks.
  return verificationSuccess
}

# 3. Check the proof data, note that the presence of the proof itself is only
# done at this point. The server may return no proof if it is not needed
# thus accessing proof's internals may result in nil pointer access if done
# before check in 2.

if proof == nil ||
    len(proof.LinearProofTerms) != int(endTxID-startTxID) ||
    len(proof.InclusionProofs) != int(endTxID-startTxID)-1 {
  // Proof must contain specific number of entries
  return verificationFailure
}

# 4. Loop over all consumed linear proof nodes

calculatedAlh := proof.LinearProofTerms.Next() // alh at startTx+1
for txID := startTxID + 1; txID < endTxID; txID++ {

  # 5. inclusion proof of the node

  if ahtree.VerifyInclusion(
    proof.InclusionProofs.Next,
    txID,
    target.BlSize, 
    leafFor(calculatedAlh),
    treeRoot,
  ) == VerificationFailure {
    return VerificationFailure
  }

  # 6. Calculate the Alh for the next transaction, this is the same method as the one used
  # for the LinearProof validation
  calculatedAlh = sha256( toBytes(txID+1) | calculatedAlh | proof.LinearProofTerms.Next() )

}

# 7. Check the final calculated hash - that one is also checked for inclusion but in different part of the proof

if oldState.txID < newState.BlTxID {
  if calculatedAlh != oldState.Alh {
    return VerificationFailure
  }
} else {
  if calculatedAlh != newState.BlTxAlh {
    return VerificationFailure
  }
}

# 8. All steps of the proof succeeded

return VerificationSuccess

#### Compatibility with older immudb servers

To maintain compatibility with older servers that do not compute the `LinearAdvance` part
of the `DualProof`, the client SDK performs a series of additional calls to the immudb server
in order to fill in the missing peaces of the information. This does impose additional
overhead in the number of server calls thus it is advised to update the immudb server
to avoid such penalty.

The algorithm for rebuilding of such `LinearAdvance` proof is as follows:

```plain

# 1. Calculate the range of transactions consumed by the new Merkle Tree

startTx := oldState.blTxID
endTx := min(oldState.txID, newState.BlTxID)

# 2. LinearAdvance proof is only needed if the length of the consumed linear chain is greater than 1.
# The node at `startTx` is validated using `LastInclusion` proof and the `endTx` is validated either
# by using the `InclusionProof` (if it is oldState.txID) or by consistency proof between old and new
# Merkle tree (if it is newState.BlTxID).

if endTx - startTx <= 11 {
  return
}

# 3. Fill in inclusion proofs for nodes in the consumed linear path

inclusionProofs = []
for txID := startTx + 1; txID < endTx; txID++ {

  partialProof = client.VerifiableTxById(
    Tx: targetTxID, ProveSinceTx: txID,
    // Add entries spec to exclude any entries to avoid transferring large responses
    EntriesSpec: { KvEntriesSpec: { Action: EXCLUDE } },
  })

  inclusionProofs.append(partialProof.DualProof.InclusionProof)

}
dualProof.LinearAdvanceProof.InclusionProofs = inclusionProofs

# 4. Fill in the linear proof for all nodes on the checked path to ensure those
# lead to the final node at the endTx

partialProof := client.VerifiableTxById(ctx, &VerifiableTxRequest{
  Tx: endTxID, ProveSinceTx: startTxID + 1,
  // Add entries spec to exclude any entries to avoid transferring large responses
  EntriesSpec: { KvEntriesSpec: { Action: EXCLUDE } },
})

dualProof.LinearAdvanceProof.LinearProofTerms = partialProof.DualProof.LinearProof.Terms
```

### Long term

Since the calculation of the Merkle Tree is always done in a synchronous manner
by default (immudb waits for full recalculation before committing a transaction)
we can completely remove the need of the linear proof part. That additional technique
was in fact added to compensate potential problems with Merkle Tree recalculation
speed, however such compensation was never needed in practice because Merkle Tree
recalculation speed is much faster than other operations necessary to commit a
transaction.

The long-term successor of the fix presented above is to present an updated proof to the client
that only consists of the proof within the Merkle Tree without the Linear part.
Calculation of the linear part is still needed to keep compatibility with the older clients.

## PoC

In the `server` folder there's an implementation of a fake server that will provide
falsified proofs for certain order of operations.

In order to check if the client is vulnerable, it must perform the following operations:

* start with no state
* perform verified read of TX 2 (client stores state for TX 2)
* perform verified read of TX 3 (client performs consistency proof between TX 2 and TX 3)
* perform verified read of TX 5 (client performs consistency proof between TX 3 and TX 5)
* perform verified read of TX 2 again (client gets different data than what was given with
  the first verified read, but the provided proof against state form TX 5 seems correct)

A client with short-term mitigation implemented will fail reading the TX 3 - that transaction
uses linear proof with 2 nodes outside of the Merkle Tree.

To run the server using docker container run the following (assuming that we run in the
directory where this readme file is located):

```sh
docker-compose up -d
```

or

``` sh
docker build -t immufake . && docker run -p 3322:3322 -d immufake
```

An immudb client demonstrating that issue can be found in `go` directory:

```sh
$ cd go
$ go run main.go 
2022/09/01 15:20:34 Reading Tx 2
2022/09/01 15:20:34   Keys from verified read:
2022/09/01 15:20:34      valid-key-0
2022/09/01 15:20:34   Client verified state:
2022/09/01 15:20:34    TxID: 2
2022/09/01 15:20:34    Hash: be6ed4baa7e7b27bd419fea6d5bf52bf76aa9a64f7c6dcd6eb4e6252fc675195
2022/09/01 15:20:34 Reading Tx 3
2022/09/01 15:20:34   Keys from verified read:
2022/09/01 15:20:34      valid-key-1
2022/09/01 15:20:34   Client verified state:
2022/09/01 15:20:34    TxID: 3
2022/09/01 15:20:34    Hash: 6696b4323aaedb89b076fb63ed3bdcf7aca4ff523875c6cecf9ff5bf46eebfbf
2022/09/01 15:20:34 Reading Tx 5
2022/09/01 15:20:34   Keys from verified read:
2022/09/01 15:20:34      key-after-1
2022/09/01 15:20:34   Client verified state:
2022/09/01 15:20:34    TxID: 5
2022/09/01 15:20:34    Hash: f590b73eccf4c19856baf48fdf4fc44c92949655561a059a392d7de1e1057617
2022/09/01 15:20:34 Reading Tx 2
2022/09/01 15:20:34   Keys from verified read:
2022/09/01 15:20:34      fake-key
2022/09/01 15:20:34   Client verified state:
2022/09/01 15:20:34    TxID: 5
2022/09/01 15:20:34    Hash: f590b73eccf4c19856baf48fdf4fc44c92949655561a059a392d7de1e1057617
2022/09/01 15:20:34 FAILURE: Client is vulnerable, was able to read two different datasets for same transaction: 'valid-key-0' and 'fake-key'
exit status 1
```
