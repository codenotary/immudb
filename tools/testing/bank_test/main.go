package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/codenotary/immudb/v2/embedded/store"
)

func exitOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	numLedgers := flag.Int("num-ledgers", 1, "number of ledgers")
	numAccounts := flag.Int("num-accounts", 100, "number of accounts")
	initialBalance := flag.Int("balance", 1000, "initial account balance")
	duration := flag.Duration("duration", 10*time.Minute, "test duration")

	flag.Parse()

	indexOpts := store.DefaultIndexOptions().
		WithMaxActiveSnapshots(*numAccounts + 1)

	st, err := store.Open(os.TempDir(), store.DefaultOptions().WithMaxConcurrency(*numAccounts).WithIndexOptions(indexOpts))
	exitOnErr(err)
	defer st.Close()

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(2 * *numLedgers)

	for i := 0; i < *numLedgers; i++ {
		ledgerName := fmt.Sprintf("ledger-%d", i)
		ledger, err := st.OpenLedger(ledgerName)
		exitOnErr(err)

		defer func(name string, ledger *store.Ledger) {
			fmt.Printf("closing ledger %s\n", name)

			err := ledger.Close()
			if err != nil {
				fmt.Printf("%s: while closing ledger\n", err.Error())
			} else {
				fmt.Printf("ledger %s successfully closed\n", name)
			}
		}(ledgerName, ledger)

		createAccounts(ledger, *numAccounts, *initialBalance)
		err = ledger.WaitForIndexingUpto(context.Background(), 1)
		exitOnErr(err)

		startLedgerBankTransfer(
			ctx, &wg, ledger, *numAccounts, *initialBalance)
	}

	time.Sleep(*duration)

	fmt.Println("stopping test...")

	cancel()

	wg.Wait()

	fmt.Println("test stopped succesfully. Exiting...")
}

func startLedgerBankTransfer(
	ctx context.Context,
	wg *sync.WaitGroup,
	ledger *store.Ledger,
	numAccounts,
	initialBalance int,
) {
	go func() {
		defer wg.Done()

		for {
			if err := ctx.Err(); err != nil {
				break
			}

			checkBalances(ledger, numAccounts, numAccounts*initialBalance)

			time.Sleep(time.Millisecond * 10)
		}
	}()

	go func() {
		defer wg.Done()

		for {
			if err := ctx.Err(); err != nil {
				break
			}

			makeTransfers(ledger, numAccounts)
		}
	}()
}

func checkBalances(ledger *store.Ledger, numAccounts, expectedTotalBalance int) {
	tx, err := ledger.NewTx(context.Background(), store.DefaultTxOptions().WithMode(store.ReadOnlyTx))
	exitOnErr(err)
	defer tx.Cancel()

	reader, err := tx.NewKeyReader(store.KeyReaderSpec{})
	exitOnErr(err)

	defer reader.Close()

	n := 0
	totalBalance := uint64(0)
	for {
		_, val, err := reader.Read(context.Background())
		if errors.Is(err, store.ErrNoMoreEntries) {
			break
		}

		value, err := ledger.Resolve(val)
		exitOnErr(err)

		totalBalance += binary.BigEndian.Uint64(value)
		n++
	}

	if numAccounts != n {
		panic(fmt.Sprintf("num accounts should be %d, but is %d", numAccounts, n))
	}

	if totalBalance != uint64(expectedTotalBalance) {
		panic(fmt.Sprintf("total balance should be %d, but is %d", expectedTotalBalance, totalBalance))
	}
}

func makeTransfers(ledger *store.Ledger, numAccounts int) {
	var wg sync.WaitGroup
	wg.Add(numAccounts)

	for i := 0; i < numAccounts; i++ {
		go func() {
			defer wg.Done()

			makeTransfer(ledger, numAccounts)
		}()
	}
	wg.Wait()
}

func makeTransfer(ledger *store.Ledger, numAccounts int) {
	src := rand.Intn(numAccounts)
	dst := rand.Intn(numAccounts)

	srcAccount := getAccountKey(src)
	dstAccount := getAccountKey(dst)

	tx, err := ledger.NewTx(context.Background(), store.DefaultTxOptions())
	exitOnErr(err)
	defer tx.Cancel()

	vref, err := tx.Get(context.Background(), srcAccount)
	exitOnErr(err)

	value, err := ledger.Resolve(vref)
	exitOnErr(err)

	amount := uint64(1 + rand.Intn(10))

	err = tx.Set(srcAccount, nil, addValue(value, ^(amount-1)))
	exitOnErr(err)

	vref, err = tx.Get(context.Background(), dstAccount)
	exitOnErr(err)

	value, err = ledger.Resolve(vref)
	exitOnErr(err)

	err = tx.Set(dstAccount, nil, addValue(value, amount))
	exitOnErr(err)

	_, err = tx.Commit(context.Background())
	if !errors.Is(err, store.ErrTxReadConflict) {
		exitOnErr(err)
	}
}

func createAccounts(ledger *store.Ledger, n int, initialBalance int) {
	tx, err := ledger.NewTx(context.Background(), store.DefaultTxOptions())
	exitOnErr(err)
	defer tx.Cancel()

	for i := 0; i < n; i++ {
		var balance [8]byte
		binary.BigEndian.PutUint64(balance[:], uint64(initialBalance))

		err := tx.Set(getAccountKey(i), nil, balance[:])
		exitOnErr(err)
	}

	_, err = tx.Commit(context.Background())
	exitOnErr(err)
}

func getAccountKey(i int) []byte {
	return []byte(fmt.Sprintf("account-%d", i))
}

func addValue(v []byte, x uint64) []byte {
	balance := binary.BigEndian.Uint64(v)

	var buf [8]byte

	newBalance := balance + x
	binary.BigEndian.PutUint64(buf[:], newBalance)

	return buf[:]
}
