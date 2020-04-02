/*
Copyright 2019-2020 vChain, Inc.

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
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/codenotary/immudb/pkg/store"
	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/api"

	"github.com/spf13/cobra"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
)

func main() {
	cmd := &cobra.Command{
		Use: "immu",
	}
	commands := []*cobra.Command{
		&cobra.Command{
			Use:     "get",
			Aliases: []string{"g"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					return err
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)
				key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				ctx := context.Background()
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Get(ctx, key)
				})
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				printItem([]byte(args[0]), nil, response)
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		&cobra.Command{
			Use:     "safeget",
			Aliases: []string{"sg"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					return err
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)
				key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				ctx := context.Background()
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.SafeGet(ctx, key)
				})
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				printItem([]byte(args[0]), nil, response)
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		&cobra.Command{
			Use:     "set",
			Aliases: []string{"s"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					return err
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)
				var reader io.Reader
				if len(args) > 1 {
					reader = bytes.NewReader([]byte(args[1]))
				} else {
					reader = bufio.NewReader(os.Stdin)
				}
				var buf bytes.Buffer
				tee := io.TeeReader(reader, &buf)
				key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				value, err := ioutil.ReadAll(tee)
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				ctx := context.Background()
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Set(ctx, key, value)
				})
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				value2, err := ioutil.ReadAll(&buf)
				if err != nil {
					return err
				}
				printItem([]byte(args[0]), value2, response)
				return nil
			},
			Args: cobra.MinimumNArgs(1),
		},
		&cobra.Command{
			Use:     "safeset",
			Aliases: []string{"ss"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					return err
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)
				var reader io.Reader
				if len(args) > 1 {
					reader = bytes.NewReader([]byte(args[1]))
				} else {
					reader = bufio.NewReader(os.Stdin)
				}
				key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				var buf bytes.Buffer
				tee := io.TeeReader(reader, &buf)
				value, err := ioutil.ReadAll(tee)
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				ctx := context.Background()
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.SafeSet(ctx, key, value)
				})
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				value2, err := ioutil.ReadAll(&buf)
				if err != nil {
					return err
				}
				printItem([]byte(args[0]), value2, response)
				return nil
			},
			Args: cobra.MinimumNArgs(1),
		},
		&cobra.Command{
			Use:     "reference",
			Aliases: []string{"r"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					return err
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)
				var reader io.Reader
				if len(args) > 1 {
					reader = bytes.NewReader([]byte(args[1]))
				} else {
					reader = bufio.NewReader(os.Stdin)
				}
				reference, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				var buf bytes.Buffer
				tee := io.TeeReader(reader, &buf)
				key, err := ioutil.ReadAll(tee)
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				ctx := context.Background()
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Reference(ctx, reference, key)
				})
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				value, err := ioutil.ReadAll(&buf)
				if err != nil {
					return err
				}
				printItem([]byte(args[0]), value, response)
				return nil
			},
			Args: cobra.MinimumNArgs(1),
		},
		&cobra.Command{
			Use:     "safereference",
			Aliases: []string{"sr"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					return err
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)
				var reader io.Reader
				if len(args) > 1 {
					reader = bytes.NewReader([]byte(args[1]))
				} else {
					reader = bufio.NewReader(os.Stdin)
				}
				reference, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				var buf bytes.Buffer
				tee := io.TeeReader(reader, &buf)
				value, err := ioutil.ReadAll(tee)
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				ctx := context.Background()
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.SafeReference(ctx, reference, value)
				})
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				value2, err := ioutil.ReadAll(&buf)
				if err != nil {
					return err
				}
				printItem([]byte(args[0]), value2, response)
				return nil
			},
			Args: cobra.MinimumNArgs(1),
		},
		&cobra.Command{
			Use:     "zadd",
			Aliases: []string{"za"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					return err
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)
				var setReader io.Reader
				var scoreReader io.Reader
				var keyReader io.Reader
				if len(args) > 1 {
					setReader = bytes.NewReader([]byte(args[0]))
					scoreReader = bytes.NewReader([]byte(args[1]))
					keyReader = bytes.NewReader([]byte(args[2]))
				}

				bs, err := ioutil.ReadAll(scoreReader)
				score, err := strconv.ParseFloat(string(bs[:]), 64)
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				set, err := ioutil.ReadAll(setReader)
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				key, err := ioutil.ReadAll(keyReader)
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				ctx := context.Background()
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.ZAdd(ctx, set, score, key)
				})
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				printSetItem([]byte(args[0]), []byte(args[2]), score, response)
				return nil
			},
			Args: cobra.MinimumNArgs(3),
		},
		&cobra.Command{
			Use:     "safezadd",
			Aliases: []string{"sza"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					return err
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)
				var setReader io.Reader
				var scoreReader io.Reader
				var keyReader io.Reader
				if len(args) > 1 {
					setReader = bytes.NewReader([]byte(args[0]))
					scoreReader = bytes.NewReader([]byte(args[1]))
					keyReader = bytes.NewReader([]byte(args[2]))
				}

				bs, err := ioutil.ReadAll(scoreReader)
				score, err := strconv.ParseFloat(string(bs[:]), 64)
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				set, err := ioutil.ReadAll(setReader)
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				key, err := ioutil.ReadAll(keyReader)
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				ctx := context.Background()
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.SafeZAdd(ctx, set, score, key)
				})
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				printSetItem([]byte(args[0]), []byte(args[2]), score, response)
				return nil
			},
			Args: cobra.MinimumNArgs(3),
		},
		&cobra.Command{
			Use:     "zscan",
			Aliases: []string{"zscn"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					return err
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)
				set, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				ctx := context.Background()
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.ZScan(ctx, set)
				})
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				for _, item := range response.(*schema.ItemList).Items {
					printItem(nil, nil, item)
					fmt.Println()
				}

				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		&cobra.Command{
			Use:     "scan",
			Aliases: []string{"scn"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					return err
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)
				prefix, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				ctx := context.Background()
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Scan(ctx, prefix)
				})
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				for _, item := range response.(*schema.ItemList).Items {
					printItem(nil, nil, item)
					fmt.Println()
				}

				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		&cobra.Command{
			Use:     "count",
			Aliases: []string{"cnt"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					return err
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)
				prefix, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				ctx := context.Background()
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Count(ctx, prefix)
				})
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				fmt.Println(response.(*schema.ItemsCount).Count)
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		&cobra.Command{
			Use:     "inclusion",
			Aliases: []string{"i"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					return err
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)

				index, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					return err
				}
				ctx := context.Background()
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Inclusion(ctx, index)
				})
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}

				proof := response.(*schema.InclusionProof)

				var hash []byte
				if len(args) > 1 {
					src := []byte(args[1])
					l := hex.DecodedLen(len(src))
					if l != 32 {
						return fmt.Errorf("invalid hash length")
					}
					hash = make([]byte, l)
					_, err := hex.Decode(hash, src)
					if err != nil {
						return err
					}

				} else {
					response, err := immuClient.Connected(ctx, func() (interface{}, error) {
						return immuClient.ByIndex(ctx, index)
					})
					if err != nil {
						_, _ = fmt.Fprintln(os.Stderr, err)
						os.Exit(1)
					}
					item := response.(*schema.Item)
					hash = item.Hash()
				}

				fmt.Printf(`verified: %t

hash: %x at index: %d
root: %x at index: %d

`, proof.Verify(index, hash), proof.Leaf, proof.Index, proof.Root, proof.At)
				return nil
			},
			Args: cobra.MinimumNArgs(1),
		},
		&cobra.Command{
			Use:     "consistency",
			Aliases: []string{"c"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					return err
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)

				index, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					return err
				}
				ctx := context.Background()
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Consistency(ctx, index)
				})
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}

				proof := response.(*schema.ConsistencyProof)

				var root []byte
				src := []byte(args[1])
				l := hex.DecodedLen(len(src))
				if l != 32 {
					return fmt.Errorf("invalid hash length")
				}
				root = make([]byte, l)
				_, err = hex.Decode(root, src)
				if err != nil {
					return err
				}

				fmt.Printf(`verified: %t

firstRoot: %x at index: %d
secondRoot: %x at index: %d

`, proof.Verify(schema.Root{Index: index, Root: root}), proof.FirstRoot, proof.First, proof.SecondRoot, proof.Second)
				return nil
			},
			Args: cobra.MinimumNArgs(2),
		},
		&cobra.Command{
			Use:     "history",
			Aliases: []string{"h"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					return err
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)
				key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				ctx := context.Background()
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.History(ctx, key)
				})
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				for _, item := range response.(*schema.ItemList).Items {
					printItem(nil, nil, item)
					fmt.Println()
				}
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		&cobra.Command{
			Use:     "ping",
			Aliases: []string{"p"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					return err
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)
				ctx := context.Background()
				_, err = immuClient.Connected(ctx, func() (interface{}, error) {
					return nil, immuClient.HealthCheck(ctx)
				})
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				fmt.Println("Health check OK")
				return nil
			},
			Args: cobra.NoArgs,
		},
	}

	for _, command := range commands {
		configureOptions(command)
		cmd.AddCommand(command)
	}

	if err := cmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func configureOptions(cmd *cobra.Command) {
	cmd.Flags().IntP("port", "p", client.DefaultOptions().Port, "port number")
	cmd.Flags().StringP("address", "a", client.DefaultOptions().Address, "bind address")
}

func options(cmd *cobra.Command) (*client.Options, error) {
	port, err := cmd.Flags().GetInt("port")
	if err != nil {
		return nil, err
	}
	address, err := cmd.Flags().GetString("address")
	if err != nil {
		return nil, err
	}
	options := client.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithDialOptions(false, grpc.WithInsecure()).
		FromEnvironment()
	return &options, nil
}

func printItem(key []byte, value []byte, message interface{}) {
	var index uint64
	switch m := message.(type) {
	case *schema.Index:
		index = m.Index
	case *schema.Item:
		key = m.Key
		value = m.Value
		index = m.Index
	}

	fmt.Printf(`index:	%d
key:	%s
value:	%s
hash:	%x
`, index, key, value, api.Digest(index, key, value))
}

func printSetItem(set []byte, rkey []byte, score float64, message interface{}) {
	index := message.(*schema.Index).Index
	key, err := store.SetKey(rkey, set, score)
	if err != nil {
		fmt.Printf(err.Error())
	}

	fmt.Printf(`index:	%d
set:    %s
key:	%s
score:  %f
value:	%s
hash:	%x
`, index, set, key, score, rkey, api.Digest(index, key, rkey))
}
