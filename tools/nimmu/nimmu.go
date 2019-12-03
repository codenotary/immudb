/*
Copyright 2019 vChain, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"fmt"
	"math"
	"os"

	"github.com/dgraph-io/badger/v2"

	"github.com/spf13/cobra"
)

func main() {
	cmd := &cobra.Command{
		Use: "ximmu",
	}
	rawSetCmd := &cobra.Command{
		Use:     "rawset",
		Aliases: []string{"s"},
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, err := cmd.Flags().GetString("dir")
			if err != nil {
				return err
			}

			cmd.SilenceUsage = true
			db := makeDB(dir)
			defer db.Close()

			k := []byte(args[0])
			v := []byte(args[1])

			txn := db.NewTransactionAt(math.MaxUint64, true)
			defer txn.Discard()
			item, err := txn.Get(k)
			if err != nil {
				return err
			}

			ts := item.Version()
			if err := txn.Set(k, v); err != nil {
				return err
			}

			if err := txn.CommitAt(ts, nil); err != nil {
				return err
			}

			fmt.Printf(`
key: %s
value: %s

index %d successfully overwritten.

`, k, v, ts-1)

			return nil
		},
		Args: cobra.MinimumNArgs(2),
	}
	configureOptions(rawSetCmd)
	cmd.AddCommand(rawSetCmd)
	if err := cmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func configureOptions(cmd *cobra.Command) {
	cmd.Flags().StringP("dir", "d", "./immudb/data", "immudb data directory")
}

func makeDB(dir string) *badger.DB {
	opts := badger.DefaultOptions(dir).
		WithLogger(nil)

	db, err := badger.OpenManaged(opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return db
}
