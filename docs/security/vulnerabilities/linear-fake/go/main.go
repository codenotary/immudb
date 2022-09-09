package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	immudb "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/state"
)

func verifyTxRead(client immudb.ImmuClient, txID uint64, stateService state.StateService) (string, error) {

	log.Printf("Reading Tx %d", txID)

	tx, err := client.VerifiedTxByID(context.Background(), txID)
	if err != nil {
		return "", err
	}
	log.Printf("  Keys from verified read:")
	key := ""
	for _, e := range tx.GetEntries() {
		key = string(e.GetKey())
		log.Printf("     %s", key)
	}
	if len(tx.GetEntries()) != 1 {
		return "", fmt.Errorf(
			"Invalid test dataset - expected only a single key in transaction %d, found %d",
			txID,
			len(tx.GetEntries()),
		)
	}

	err = stateService.CacheLock()
	if err != nil {
		return "", err
	}

	state, err := stateService.GetState(context.Background(), "defaultdb")
	if err != nil {
		return "", err
	}
	log.Print("  Client verified state:")
	log.Printf("   TxID: %d", state.TxId)
	log.Printf("   Hash: %x", state.TxHash)

	err = stateService.CacheUnlock()
	if err != nil {
		return "", err
	}

	return key, nil
}

func checkCorruptedErr(err error) bool {
	if err == nil {
		return false
	}

	if strings.Contains(err.Error(), "corrupted") {
		log.Print("SUCCESS: Client version not vulnerable")
		return true
	}

	log.Fatal(err)
	return false
}

func main() {
	opts := immudb.DefaultOptions().
		WithAddress("localhost").
		WithPort(3322)

	ctx := context.Background()

	// Remove any old state that could be problematic here,
	// The test needs to perform test starting with no state
	list, err := filepath.Glob(".state-*")
	if err != nil {
		log.Fatal(err)
	}
	for _, e := range list {
		err := os.Remove(e)
		if err != nil {
			log.Fatal(err)
		}
	}

	client := immudb.NewClient().WithOptions(opts)
	err = client.OpenSession(ctx, []byte(`immudb`), []byte(`immudb`), "defaultdb")
	if err != nil {
		log.Fatal(err)
	}

	defer client.CloseSession(ctx)

	key2, err := verifyTxRead(client, 2, client.StateService)
	if err != nil {
		log.Fatal(err)
	}

	_, err = verifyTxRead(client, 3, client.StateService)
	if checkCorruptedErr(err) {
		return
	}

	_, err = verifyTxRead(client, 5, client.StateService)
	if checkCorruptedErr(err) {
		return
	}

	key2Fake, err := verifyTxRead(client, 2, client.StateService)
	if checkCorruptedErr(err) {
		return
	}

	if key2 == key2Fake {
		log.Fatal("WARNING: Confusing results - are you running against normal immudb server?")
	}

	log.Fatalf(
		"FAILURE: Client is vulnerable, was able to read two different datasets for same transaction: '%s' and '%s'",
		key2,
		key2Fake,
	)
}
