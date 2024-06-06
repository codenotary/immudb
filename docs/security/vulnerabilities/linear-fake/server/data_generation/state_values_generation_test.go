//go:build ignore
// +build ignore

/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package store

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/fs"
	"github.com/stretchr/testify/require"
)

// TestVerifyDualProofLongLinearProofWithReplica is a test function that can generate the test data
// stored into `state-values.go` file. Note that this test should be run from within the `embedded/store`
// package of an older immudb version since it does access internals of the store.
func TestVerifyDualProofLongLinearProofWithReplica(t *testing.T) {
	toKV := func(s string) []byte { return append([]byte{0}, s...) }

	// Create two databases, initially those replicate themselves but diverge at some point
	opts := DefaultOptions().WithSynced(false).WithMaxLinearProofLen(10).WithMaxConcurrency(1)
	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)
	defer immustoreClose(t, immuStore)

	opts = DefaultOptions().WithSynced(false).WithMaxLinearProofLen(0).WithMaxConcurrency(1)
	immuStoreRep, err := Open(t.TempDir(), opts)
	require.NoError(t, err)
	defer immustoreClose(t, immuStoreRep)

	t.Run("add first normal transaction", func(t *testing.T) {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		err = tx.Set(toKV("key"), nil, toKV("value"))
		require.NoError(t, err)

		hdr, err := tx.Commit()
		require.NoError(t, err)
		require.EqualValues(t, 1, hdr.ID)
		require.EqualValues(t, 0, hdr.BlTxID)

		txholder := tempTxHolder(t, immuStore)
		etx, err := immuStore.ExportTx(1, false, txholder)
		require.NoError(t, err)

		_, err = immuStoreRep.ReplicateTx(etx, false)
		require.NoError(t, err)
	})

	var alhValuesToInject [][sha256.Size]byte

	t.Run("diverge replica from the primary", func(t *testing.T) {
		tx, err := immuStoreRep.NewWriteOnlyTx()
		require.NoError(t, err)

		err = tx.Set(toKV("fake-key"), nil, toKV("fake-value"))
		require.NoError(t, err)

		hdr, err := tx.Commit()
		require.NoError(t, err)
		require.EqualValues(t, 2, hdr.ID)
		require.EqualValues(t, 1, hdr.BlTxID)

		alhValuesToInject = append(alhValuesToInject, hdr.Alh())
	})

	t.Run("generate long linear proof in primary", func(t *testing.T) {
		immuStore.blDone <- struct{}{}                  // Disable binary linking - we'll be adding entries ourselves
		defer func() { go immuStore.binaryLinking() }() // To enable clean shutdown of the immustore, note this has to run before we continue

		for i := 0; i < 2; i++ {
			// Gather long linear proof by discarding the AHT operations
			tx, err := immuStore.NewWriteOnlyTx()
			require.NoError(t, err)

			err = tx.Set(
				toKV(fmt.Sprintf("valid-key-%d", i)),
				nil,
				toKV(fmt.Sprintf("valid-value-%d", i)),
			)
			require.NoError(t, err)

			hdr, err := tx.Commit()
			require.NoError(t, err)
			require.EqualValues(t, i+2, hdr.ID)
			require.EqualValues(t, 1, hdr.BlTxID)

			<-immuStore.blBuffer // Remove the entry waiting in the buffer

			if i != 0 {
				alhValuesToInject = append(alhValuesToInject, hdr.Alh())
			}
		}
	})

	t.Run("inject Alh values to Merkle Tree (including Alh from diverged replica)", func(t *testing.T) {
		require.EqualValues(t, 1, immuStore.aht.Size())
		for _, alh := range alhValuesToInject {
			_, _, err := immuStore.aht.Append(alh[:])
			require.NoError(t, err)
		}
	})

	t.Run("add normal TX after fake AHT data insertion", func(t *testing.T) {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		err = tx.Set(toKV("key-after-0"), nil, toKV("value-after-0"))
		require.NoError(t, err)

		hdr, err := tx.Commit()
		require.NoError(t, err)
		require.EqualValues(t, 4, hdr.ID)
		require.EqualValues(t, 3, hdr.BlTxID)

		// Wait for the async goroutine to consume the new aht entry
		for immuStore.aht.Size() < 4 {
			time.Sleep(time.Millisecond)
		}

		tx, err = immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		err = tx.Set(toKV("key-after-1"), nil, toKV("value-after-1"))
		require.NoError(t, err)

		hdr, err = tx.Commit()
		require.NoError(t, err)
		require.EqualValues(t, 5, hdr.ID)
		require.EqualValues(t, 4, hdr.BlTxID)
	})

	t.Run("dump test data for fake server", func(t *testing.T) {

		toBArray := func(a []byte, indent int) string {
			ret := &strings.Builder{}
			ret.WriteString("[]byte{")
			for i, b := range a {
				if i%16 == 0 {
					ret.WriteString("\n")
					ret.WriteString(strings.Repeat("\t", indent+1))
				} else {
					ret.WriteString(" ")
				}
				fmt.Fprintf(ret, "0x%02X,", b)
			}
			if len(a) > 0 {
				ret.WriteString("\n")
				ret.WriteString(strings.Repeat("\t", indent))
			}
			ret.WriteString("}")
			return ret.String()
		}

		dumpHdr := func(hdr *TxHeader, indent int) string {
			i := strings.Repeat("\t", indent)
			return fmt.Sprintf(""+
				"&schema.TxHeader{\n"+
				"%s\tId: %d,\n"+
				"%s\tPrevAlh: %s,\n"+
				"%s\tTs:       %d,\n"+
				"%s\tVersion:  %d,\n"+
				"%s\tNentries: %d,\n"+
				"%s\tEH: %s,\n"+
				"%s\tBlTxId: %d,\n"+
				"%s\tBlRoot: %s,\n"+
				"%s}",
				i, hdr.ID,
				i, toBArray(hdr.PrevAlh[:], 4),
				i, hdr.Ts,
				i, hdr.Version,
				i, hdr.NEntries,
				i, toBArray(hdr.Eh[:], 4),
				i, hdr.BlTxID,
				i, toBArray(hdr.BlRoot[:], 4),
				i,
			)
		}

		dumpEntries := func(entries []*TxEntry, indent int) string {
			i := strings.Repeat("\t", indent)
			ret := &strings.Builder{}
			ret.WriteString("[]*schema.TxEntry{")
			for _, e := range entries {
				hValue := e.HVal()

				fmt.Fprintf(ret, "\n"+
					"%s\t{\n"+
					"%s\t\tKey: %s,\n"+
					// "%s	Metadata: KVMetadataToProto(e.Metadata()),
					"%s\t\tHValue: %s,\n"+
					"%s\t\tVLen: %d,\n"+
					"%s\t},\n",
					i,
					i, toBArray(e.Key(), indent+2),
					i, toBArray(hValue[:], indent+2),
					i, e.vLen,
					i,
				)
			}
			if len(entries) > 0 {
				ret.WriteString(i)
			}
			ret.WriteString("}")
			return ret.String()
		}

		dumpDigests := func(digests [][sha256.Size]byte, indent int) string {
			ret := strings.Builder{}

			ret.WriteString("[][]byte{")

			for _, d := range digests {
				ret.WriteString("\n")
				ret.WriteString(strings.Repeat("\t", indent+2))
				ret.WriteString(toBArray(d[:], indent+2)[6:])
				ret.WriteString(",\n")
			}

			if len(digests) > 0 {
				ret.WriteString(strings.Repeat("\t", indent))
			}
			ret.WriteString("}")

			return ret.String()
		}

		dumpVerifiableTx := func(tx *Tx, dualProof *DualProof, indent int) string {
			i := strings.Repeat("\t", indent)
			return fmt.Sprintf("&schema.VerifiableTx{\n"+
				"%s	Tx:	&schema.Tx{\n"+
				"%s		Header: %s,\n"+
				"%s		Entries: %s,\n"+
				"%s	},\n"+
				"%s	DualProof: &schema.DualProof{\n"+
				"%s		SourceTxHeader: %s,\n"+
				"%s		TargetTxHeader: %s,\n"+
				"%s		InclusionProof: %s,\n"+
				"%s		ConsistencyProof: %s,\n"+
				"%s		TargetBlTxAlh: %s,\n"+
				"%s		LastInclusionProof: %s,\n"+
				"%s		LinearProof: &schema.LinearProof{\n"+
				"%s			SourceTxId: %d,\n"+
				"%s			TargetTxId: %d,\n"+
				"%s			Terms: %s,\n"+
				"%s		},\n"+
				"%s	},\n"+
				"%s}"+
				"",
				i,
				i, dumpHdr(tx.Header(), indent+2),
				i, dumpEntries(tx.Entries(), indent+2),
				i,
				i,
				i, dumpHdr(dualProof.SourceTxHeader, indent+2),
				i, dumpHdr(dualProof.TargetTxHeader, indent+2),
				i, dumpDigests(dualProof.InclusionProof, indent+3),
				i, dumpDigests(dualProof.ConsistencyProof, indent+3),
				i, toBArray(dualProof.TargetBlTxAlh[:], indent+3),
				i, dumpDigests(dualProof.LastInclusionProof, indent+3),
				i,
				i, dualProof.LinearProof.SourceTxID,
				i, dualProof.LinearProof.TargetTxID,
				i, dumpDigests(dualProof.LinearProof.Terms, indent+3),
				i,
				i,
				i,
			)
		}

		tx2 := tempTxHolder(t, immuStore)
		err := immuStore.ReadTx(2, tx2)
		require.NoError(t, err)

		tx2Hdr := tx2.Header()
		tx2Alh := tx2Hdr.Alh()

		dualProof2_2, err := immuStore.DualProof(tx2Hdr, tx2Hdr)
		require.NoError(t, err)

		tx3 := tempTxHolder(t, immuStore)
		err = immuStore.ReadTx(3, tx3)
		require.NoError(t, err)

		tx3Hdr := tx3.Header()

		dualProof3_2, err := immuStore.DualProof(tx2Hdr, tx3Hdr)
		require.NoError(t, err)

		tx5 := tempTxHolder(t, immuStore)
		err = immuStore.ReadTx(5, tx5)
		require.NoError(t, err)

		tx5Hdr := tx5.Header()

		dualProof5_3, err := immuStore.DualProof(tx3Hdr, tx5Hdr)
		require.NoError(t, err)

		tx2Fake := tempTxHolder(t, immuStoreRep)
		err = immuStoreRep.ReadTx(2, tx2Fake)
		require.NoError(t, err)

		dualProof5_2_fake, err := immuStore.DualProof(tx2Fake.Header(), tx5Hdr)
		require.NoError(t, err)

		os.WriteFile("../../tools/testing/immufaker/state_values.go", []byte(fmt.Sprintf(""+
			"package main\n"+
			"\n"+
			"import (\n"+
			"	\"github.com/codenotary/immudb/pkg/api/schema\"\n"+
			")\n"+
			"\n"+
			"var (\n"+
			"	stateQueryResult = &schema.ImmutableState{\n"+
			"		Db:   \"defaultdb\",\n"+
			"		TxId: 2,\n"+
			"		TxHash: %s,\n"+
			"	}\n"+
			"\n"+
			"	verifiableTxById_2_2 = %s\n"+
			"\n"+
			"	verifiableTxById_3_2 = %s\n"+
			"\n"+
			"	verifiableTxById_5_3 = %s\n"+
			"\n"+
			"	verifiableTxById_5_2_fake = %s\n"+
			")\n",
			toBArray(tx2Alh[:], 2),
			dumpVerifiableTx(tx2, dualProof2_2, 1),
			dumpVerifiableTx(tx3, dualProof3_2, 1),
			dumpVerifiableTx(tx5, dualProof5_3, 1),
			dumpVerifiableTx(tx2Fake, dualProof5_2_fake, 1),
		)), 0666)
	})

	// Currently there are following transactions in the database:
	//  1 - valid normal TX, {"key": "value"}
	//  2 - diverged TX, in linear linking: {"valid-key-0": "valid-value-0"}, in merkle tree hash for: {"fake-key": "fake-value"}
	//  3 - normal TX using linear linking back to Tx 1: {"valid-key-1": "valid-value-1"}
	//  4 - normal TX, {"key-after-0": "value-after-0"}
	//  5 - normal TX, {"key-after-1": "value-after-1"}
	//
	// Up to Tx 3, values are proven by doing linear linking back to Tx 1, the Merkle Tree at that point is empty.
	//
	// Sequence of actions to run the PoC (as seen by the client):
	//  1. Verified read Tx 2 (no previous state, new stored state for Tx 2)
	//  2. Verified read Tx 3 (previously stored state for Tx 2, new stored state for Tx 3)
	//  3. Verified read Tx 5 (previously stored state for Tx 3, new stored state for Tx 5)
	//  4. Verified read fake Tx 2 (stored state Tx 5, no update, read succeeds with different Tx data)

	t.Run("fake entries by injecting invalid Merkle Tree entries proof PoC", func(t *testing.T) {

		// read tx 2 and ensure it's valid-key-1
		tx2 := tempTxHolder(t, immuStore)
		err := immuStore.ReadTx(2, tx2)
		require.NoError(t, err)
		require.Len(t, tx2.Entries(), 1)
		require.Equal(t, toKV("valid-key-0"), tx2.Entries()[0].key())
		tx2Hdr := tx2.Header()

		// perform dual proof between Tx 2 and Tx 3
		tx3Hdr, err := immuStore.ReadTxHeader(3, false)
		require.NoError(t, err)

		dualProof, err := immuStore.DualProof(tx2Hdr, tx3Hdr)
		require.NoError(t, err)

		verifies := VerifyDualProof(dualProof, tx2Hdr.ID, tx3Hdr.ID, tx2Hdr.Alh(), tx3Hdr.Alh())
		require.True(t, verifies)

		// further perform dual proof to get to Tx 5
		tx5Hdr, err := immuStore.ReadTxHeader(5, false)
		require.NoError(t, err)

		dualProof, err = immuStore.DualProof(tx3Hdr, tx5Hdr)
		require.NoError(t, err)

		verifies = VerifyDualProof(dualProof, tx3Hdr.ID, tx5Hdr.ID, tx3Hdr.Alh(), tx5Hdr.Alh())
		require.True(t, verifies)

		// Read fake Tx from replica DB that has diverged
		fakeTx2 := tempTxHolder(t, immuStoreRep)
		err = immuStoreRep.ReadTx(2, fakeTx2)
		require.NoError(t, err)
		require.Len(t, fakeTx2.Entries(), 1)
		require.Equal(t, toKV("fake-key"), fakeTx2.Entries()[0].key())
		fakeTx2Hdr := fakeTx2.Header()

		// Perform consistency proof between fake Tx2 and the genuine Tx5
		dualProof, err = immuStore.DualProof(fakeTx2Hdr, tx5Hdr)
		require.NoError(t, err)

		// We should never be able to perform this proof by the client !!!
		verifies = VerifyDualProof(dualProof, fakeTx2Hdr.ID, tx5Hdr.ID, fakeTx2Hdr.Alh(), tx5Hdr.Alh())
		require.True(t, verifies)

	})

}

func TestGenerateDataWithLongLinearProof(t *testing.T) {
	const (
		initialNormalTxCount = 10
		linearTxCount        = 10
		finalNormalTxCount   = 10
	)

	opts := DefaultOptions().WithSynced(false).WithMaxLinearProofLen(100).WithMaxConcurrency(1)
	dir := t.TempDir()
	immuStore, err := Open(dir, opts)
	require.NoError(t, err)
	defer func() {
		if immuStore != nil {
			immustoreClose(t, immuStore)
		}
	}()

	t.Run("Prepare initial normal transactions", func(t *testing.T) {
		for i := 0; i < initialNormalTxCount; i++ {
			tx, err := immuStore.NewWriteOnlyTx()
			require.NoError(t, err)

			err = tx.Set([]byte(fmt.Sprintf("step1:key:%d", i)), nil, []byte(fmt.Sprintf("value:%d", i)))
			require.NoError(t, err)

			txhdr, err := tx.AsyncCommit()
			require.NoError(t, err)
			require.EqualValues(t, i+1, txhdr.ID)
			require.EqualValues(t, i, txhdr.BlTxID)

			immuStore.ahtWHub.WaitFor(txhdr.ID, nil)
		}
	})

	t.Run("Add transactions with long linear proof", func(t *testing.T) {
		// Disable binary linking and restore before we finish this step
		immuStore.blDone <- struct{}{}
		defer func() {
			go immuStore.binaryLinking()
			immuStore.ahtWHub.WaitFor(initialNormalTxCount+linearTxCount, nil)
		}()

		for i := 0; i < linearTxCount; i++ {
			tx, err := immuStore.NewWriteOnlyTx()
			require.NoError(t, err)

			err = tx.Set([]byte(fmt.Sprintf("step2:key:%d", i)), nil, []byte(fmt.Sprintf("value:%d", i)))
			require.NoError(t, err)

			txhdr, err := tx.AsyncCommit()
			require.NoError(t, err)
			require.EqualValues(t, i+1+initialNormalTxCount, txhdr.ID)
			require.EqualValues(t, initialNormalTxCount, txhdr.BlTxID)
		}
	})

	t.Run("Add normal transactions at the end", func(t *testing.T) {
		for i := 0; i < finalNormalTxCount; i++ {
			tx, err := immuStore.NewWriteOnlyTx()
			require.NoError(t, err)

			err = tx.Set([]byte(fmt.Sprintf("step3:key:%d", i)), nil, []byte(fmt.Sprintf("value:%d", i)))
			require.NoError(t, err)

			txhdr, err := tx.AsyncCommit()
			require.NoError(t, err)
			require.EqualValues(t, i+1+initialNormalTxCount+linearTxCount, txhdr.ID)
			require.EqualValues(t, i+initialNormalTxCount+linearTxCount, txhdr.BlTxID)

			immuStore.ahtWHub.WaitFor(txhdr.ID, nil)
		}
	})

	t.Run("copy database files to test folder", func(t *testing.T) {
		err := immuStore.Sync()
		require.NoError(t, err)

		err = immuStore.Close()
		require.NoError(t, err)
		immuStore = nil

		destPath := "../../test/data_long_linear_proof"
		copier := fs.NewStandardCopier()

		err = os.RemoveAll(destPath)
		require.NoError(t, err)

		err = copier.CopyDir(dir, destPath)
		require.NoError(t, err)
	})
}
