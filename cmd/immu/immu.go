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
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/codenotary/immudb/pkg/api"

	"github.com/golang/protobuf/proto"

	"github.com/spf13/cobra"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
)

var marshaller = proto.TextMarshaler{}

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
				response, err := immuClient.Connected(func() (interface{}, error) {
					return immuClient.Get(bytes.NewReader([]byte(args[0])))
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
				response, err := immuClient.Connected(func() (interface{}, error) {
					return immuClient.Set(bytes.NewReader([]byte(args[0])), tee)
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
				response, err := immuClient.Connected(func() (interface{}, error) {
					return immuClient.Scan(bytes.NewReader([]byte(args[0])))
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
				response, err := immuClient.Connected(func() (interface{}, error) {
					return immuClient.Count(bytes.NewReader([]byte(args[0])))
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

				index, err := strconv.ParseUint(string(args[0]), 10, 64)
				if err != nil {
					return err
				}
				response, err := immuClient.Connected(func() (interface{}, error) {
					return immuClient.Inclusion(index)
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
					response, err := immuClient.Connected(func() (interface{}, error) {
						return immuClient.ByIndex(index)
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

				index, err := strconv.ParseUint(string(args[0]), 10, 64)
				if err != nil {
					return err
				}
				response, err := immuClient.Connected(func() (interface{}, error) {
					return immuClient.Consistency(index)
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
				response, err := immuClient.Connected(func() (interface{}, error) {
					return immuClient.History(bytes.NewReader([]byte(args[0])))
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
				_, err = immuClient.Connected(func() (interface{}, error) {
					return nil, immuClient.HealthCheck()
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
