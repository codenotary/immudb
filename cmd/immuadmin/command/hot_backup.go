/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package immuadmin

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"

	"google.golang.org/grpc/metadata"

	"github.com/schollz/progressbar/v2"
	"github.com/spf13/cobra"

	"github.com/codenotary/immudb/pkg/api/schema"
)

const (
	prefix            = "IMMUBACKUP"
	latestFileVersion = 1
)

const (
	prefixOffset     = iota
	versionOffset    = prefixOffset + len(prefix)
	txIdOffset       = versionOffset + 4
	txSignSizeOffset = txIdOffset + 8
	txSizeOffset     = txSignSizeOffset + 4
	headerSize       = txSizeOffset + 4
)

var ErrMalformedFile = errors.New("malformed backup file")
var ErrTxWrongOrder = errors.New("incorrect transaction order in file")
var ErrTxNotInFile = errors.New("last known transaction not in file")

func (cl *commandlineBck) hotBackup(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:   "hot-backup <db_name>",
		Short: "Make a copy of the database without stopping",
		Long: "Backup a database to file/stream without stopping the database engine. " +
			"Backup can run from the beginning or starting from arbitrary transaction.",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			file := io.Writer(os.Stdout)
			output, err := cmd.Flags().GetString("output")
			if err != nil {
				return err
			}
			append, err := cmd.Flags().GetBool("append")
			if err != nil {
				return err
			}
			start, err := cmd.Flags().GetUint64("start-tx")
			if err != nil {
				return err
			}
			if start > 1 && append {
				return errors.New("don't use --append and --start-tx options together")
			}

			udr, err := cl.immuClient.UseDatabase(cl.context, &schema.Database{DatabaseName: args[0]})
			if err != nil {
				return err
			}
			cl.context = metadata.NewOutgoingContext(cl.context, metadata.Pairs("authorization", udr.GetToken()))

			if output != "-" {
				var f *os.File
				_, err := os.Stat(output)
				if err == nil {
					// file exists - find last tx in file and check whether it matches the one in DB
					if !append {
						return errors.New("file already exists, use --append option to append new data to file")
					}
					f, err = os.OpenFile(output, os.O_RDWR, 0)
					if err != nil {
						return err
					}
					last, fileSignature, err := lastTxInFile(f)
					if err != nil {
						return err
					}
					txn, err := cl.immuClient.TxByID(cl.context, last)
					if err != nil {
						return fmt.Errorf("cannot find file's last transaction %d in database: %v", last, err)
					}
					if !bytes.Equal(fileSignature, txn.Header.EH) {
						return fmt.Errorf("signatures for transaction %d in backup file and database differ - probably file was created from different database", last)
					}
					start = last + 1
				} else {
					if !os.IsNotExist(err) {
						return err
					}
					f, err = os.Create(output)
					if err != nil {
						return err
					}
				}
				defer f.Close()
				file = f
			} else if append {
				return errors.New("--append option can be used only when outputting to the file")
			}

			progress, _ := cmd.Flags().GetBool("progress")
			return cl.runHotBackup(file, start, progress)
		},
		Args: cobra.ExactArgs(1),
	}
	ccmd.Flags().StringP("output", "o", "-", "output file, \"-\" for stdout")
	ccmd.Flags().Uint64("start-tx", 1, "Transaction ID to start from")
	ccmd.Flags().Bool("progress", false, "show progress indicator")
	ccmd.Flags().BoolP("append", "i", false, "append to file, if it already exists (for file output only)")
	cmd.AddCommand(ccmd)
}

func (cl *commandlineBck) hotRestore(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:   "hot-restore <db_name>",
		Short: "Restore data from backup file",
		Long: "Restore saved transaction from backup file without stopping the database engine. " +
			"Restore can restore the data from scratch or apply only the missing data.",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			file := io.ReadSeeker(os.Stdin)
			input, err := cmd.Flags().GetString("input")
			if err != nil {
				return err
			}
			if input != "-" {
				f, err := os.Open(input)
				if err != nil {
					return err
				}
				file = f
				defer f.Close()
			}

			verify, err := cmd.Flags().GetBool("verify")
			if verify {
				fileStart, _, _, err := getTxHeader(file)
				if err != nil {
					return err
				}
				_, err = file.Seek(0, io.SeekStart)
				if err != nil {
					return err
				}

				last, _, err := lastTxInFile(file)
				if err != nil {
					return err
				}
				fmt.Printf("Backup file contains transactions %d - %d\n", fileStart, last)
				return nil
			}

			append, err := cmd.Flags().GetBool("append")
			if err != nil {
				return err
			}

			force, err := cmd.Flags().GetBool("force")
			if err != nil {
				return err
			}

			dbList, err := cl.immuClient.DatabaseList(cl.context)
			if err != nil {
				return err
			}
			dbExist := false
			for _, db := range dbList.Databases {
				if db.DatabaseName == args[0] {
					dbExist = true
					break
				}
			}

			if dbExist {
				udr, err := cl.immuClient.UseDatabase(cl.context, &schema.Database{DatabaseName: args[0]})
				if err != nil {
					return err
				}
				cl.context = metadata.NewOutgoingContext(cl.context, metadata.Pairs("authorization", udr.GetToken()))

				state, err := cl.immuClient.CurrentState(cl.context)
				if err != nil {
					return err
				}
				if state.TxId > 0 {
					if !append {
						return errors.New("cannot restore to non-empty database without --append flag")
					}
					// find the transaction in backup file and position the file just after the transaction
					fileSignature, err := findTxSignature(file, state.TxId)
					if err == ErrTxNotInFile {
						// first transaction in the file is after last transaction in DB
						// processing is still possible if there is no gap between transactions
						_, err = file.Seek(0, io.SeekStart)
						if err != nil {
							return err
						}
						fileStart, _, _, err := getTxHeader(file)
						if err != nil {
							return err
						}
						if fileStart-state.TxId > 1 {
							return fmt.Errorf("there is a gap of %d transaction(s) between database and file - restore not possible", fileStart-state.TxId-1)
						}
						if !force {
							return errors.New("not possible to validate transaction sequence - use --force to override")
						}
						_, err = file.Seek(0, io.SeekStart)
						if err != nil {
							return err
						}
					} else if err != nil {
						return err
					} else if !force {
						txn, err := cl.immuClient.TxByID(cl.context, state.TxId)
						if err != nil {
							return err
						}
						if !bytes.Equal(fileSignature, txn.Header.EH) {
							return fmt.Errorf("signatures for tx %d in backup file and database differ - cannot append data to the database", state.TxId)
						}
					}
				}
				err = cl.immuClient.UpdateDatabase(cl.context, &schema.DatabaseSettings{DatabaseName: args[0], Replica: true})
				if err != nil {
					return fmt.Errorf("cannot switch on replica mode for db: %v", err)
				}
			} else {
				// db does not exist - create as replica and use it
				err = cl.immuClient.CreateDatabase(cl.context, &schema.DatabaseSettings{DatabaseName: args[0], Replica: true, MasterDatabase: "dummy"})
				if err != nil {
					return err
				}

				udr, err := cl.immuClient.UseDatabase(cl.context, &schema.Database{DatabaseName: args[0]})
				if err != nil {
					return err
				}
				cl.context = metadata.NewOutgoingContext(cl.context, metadata.Pairs("authorization", udr.GetToken()))
			}
			defer func() {
				err = cl.immuClient.UpdateDatabase(cl.context, &schema.DatabaseSettings{DatabaseName: args[0], Replica: false})
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error switching off replica mode for db: %v", err)
				}
			}()

			progress, _ := cmd.Flags().GetBool("progress")
			return cl.runHotRestore(file, progress)
		},
		Args: func(cmd *cobra.Command, args []string) error {
			verify, _ := cmd.Flags().GetBool("verify")
			if verify {
				return cobra.ExactArgs(0)(cmd, args)
			}
			return cobra.ExactArgs(1)(cmd, args)
		},
	}
	ccmd.Flags().StringP("input", "i", "-", "input file file, \"-\" for stdin")
	ccmd.Flags().Bool("verify", false, "do not restore data, only verify backup")
	ccmd.Flags().Bool("append", false, "appending to DB, if it already exists")
	ccmd.Flags().Bool("progress", false, "show progress indicator")
	ccmd.Flags().Bool("force", false, "don't check transaction sequence")
	cmd.AddCommand(ccmd)
}

func (cl *commandlineBck) runHotBackup(output io.Writer, startFrom uint64, progress bool) error {
	state, err := cl.immuClient.CurrentState(cl.context)
	if err != nil {
		return err
	}
	txnCount := state.TxId

	if txnCount < startFrom {
		fmt.Fprintf(os.Stderr, "All backed up, nothing to do\n")
		return nil
	}

	fmt.Fprintf(os.Stderr, "Backing up transactions %d - %d\n", startFrom, txnCount)

	var bar *progressbar.ProgressBar
	if progress {
		bar = progressbar.New(int(txnCount))
	}

	done := make(chan struct{}, 1)
	defer close(done)
	terminated := make(chan os.Signal, 1)
	signal.Notify(terminated, os.Interrupt)

	stop := false
	go func() {
		select {
		case <-done:
		case <-terminated:
			stop = true
		}
	}()

	for i := startFrom; i <= txnCount; i++ {
		if stop {
			fmt.Fprintf(os.Stderr, "Terminated by signal - stopped after tx %d\n", i-1)
			return nil
		}
		err = cl.backupTx(i, output)
		if err != nil {
			return err
		}
		if bar != nil {
			bar.Add(1)
		}
	}

	fmt.Fprintf(os.Stderr, "Done\n")
	return nil
}

func (cl *commandlineBck) runHotRestore(input io.Reader, progress bool) error {
	var startTx, lastTx uint64

	var bar *progressbar.ProgressBar
	if progress {
		bar = progressbar.New(-1)
	}

	done := make(chan struct{}, 1)
	defer close(done)
	terminated := make(chan os.Signal, 1)
	signal.Notify(terminated, os.Interrupt)

	stop := false
	go func() {
		select {
		case <-done:
		case <-terminated:
			stop = true
		}
	}()

	for {
		if stop {
			fmt.Fprintf(os.Stderr, "Terminated by signal\n")
			break
		}
		tx, signSize, txSize, err := getTxHeader(input)
		if errors.Is(err, io.EOF) {
			break
		}
		if startTx == 0 {
			startTx = tx
		}
		if err != nil {
			return err
		}
		payload := make([]byte, signSize+txSize)
		got, err := input.Read(payload)
		if err != nil {
			return err
		}
		if uint32(got) < signSize+txSize {
			return ErrMalformedFile
		}
		err = cl.restoreTx(input, payload[signSize:], payload[:signSize], terminated)
		if err != nil {
			return err
		}
		lastTx = tx
		if bar != nil {
			bar.Add(1)
		}
	}
	fmt.Printf("Restored transactions %d - %d\n", startTx, lastTx)

	return nil
}

func (cl *commandlineBck) backupTx(tx uint64, output io.Writer) error {
	stream, err := cl.immuClient.ExportTx(cl.context, &schema.TxRequest{Tx: tx})
	if err != nil {
		return fmt.Errorf("failed to export transaction: %w", err)
	}

	var content []byte
	for {
		var chunk *schema.Chunk
		chunk, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			err = nil
			break
		} else if err != nil {
			break
		}
		content = append(content, chunk.Content...)
	}

	if err != nil {
		return fmt.Errorf("cannot process transaction data: %w", err)
	}
	err = stream.CloseSend()
	if err != nil {
		return fmt.Errorf("CloseSend returned %v", err)
	}
	txn, err := cl.immuClient.TxByID(cl.context, tx)
	if err != nil {
		return err
	}
	err = outputTx(tx, output, txn.Header.EH, content)
	if err != nil {
		return err
	}

	return nil
}

func (cl *commandlineBck) restoreTx(input io.Reader, payload []byte, signature []byte, terminated chan os.Signal) error {
	maxPayload := uint32(cl.options.MaxRecvMsgSize)

	stream, err := cl.immuClient.ReplicateTx(cl.context)
	if err != nil {
		return err
	}
	offset := uint32(0)
	remainder := uint32(len(payload))
	for remainder > 0 {
		chunkSize := maxPayload
		if remainder < maxPayload {
			chunkSize = remainder
		}
		err = stream.Send(&schema.Chunk{Content: payload[offset : offset+chunkSize]})
		if err != nil {
			return err
		}
		remainder -= chunkSize
		offset += chunkSize
	}
	metadata, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if !bytes.Equal(signature, metadata.EH) {
		return fmt.Errorf("transaction signatures don't match")
	}

	return nil
}

func outputTx(tx uint64, output io.Writer, signature []byte, content []byte) error {
	payload := make([]byte, headerSize, headerSize+len(signature)+len(content))
	copy(payload[prefixOffset:], prefix)
	binary.BigEndian.PutUint32(payload[versionOffset:], latestFileVersion)
	binary.BigEndian.PutUint64(payload[txIdOffset:], tx)
	binary.BigEndian.PutUint32(payload[txSignSizeOffset:], uint32(len(signature)))
	binary.BigEndian.PutUint32(payload[txSizeOffset:], uint32(len(content)))
	payload = append(payload, signature...)
	payload = append(payload, content...)

	_, err := output.Write(payload)
	return err
}

// lastTxInFile assumes that file is positioned on transaction boundary
// it also can be used to validate file correctness
// in case of success file pointer is positioned to file end
func lastTxInFile(file io.ReadSeeker) (uint64, []byte, error) {
	last := uint64(0)
	var signature []byte
	for {
		cur, signSize, txSize, err := getTxHeader(file)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, nil, err
		}
		signature = make([]byte, signSize)
		count, err := file.Read(signature)
		if err != nil {
			return 0, nil, nil
		}
		if uint32(count) != signSize {
			return 0, nil, ErrMalformedFile
		}
		_, err = file.Seek(int64(txSize), io.SeekCurrent)
		if err != nil {
			return 0, nil, ErrMalformedFile
		}
		if cur != last+1 && last > 0 {
			return 0, nil, ErrTxWrongOrder
		}
		last = cur
	}

	return last, signature, nil
}

// reads header for the next transaction and position file pointer just after the header, before the payload
func getTxHeader(file io.Reader) (tx uint64, signSize uint32, txSize uint32, err error) {
	header := make([]byte, headerSize)

	got, err := file.Read(header)
	if errors.Is(err, io.EOF) {
		if got != 0 {
			err = ErrMalformedFile
		}
		return
	}
	if err != nil {
		return
	}
	if got != len(header) {
		err = ErrMalformedFile
		return
	}
	if !bytes.Equal(header[:versionOffset], []byte(prefix)) {
		err = ErrMalformedFile
		return
	}
	formatVersion := binary.BigEndian.Uint32(header[versionOffset:])
	if formatVersion != latestFileVersion {
		err = ErrMalformedFile
		return
	}

	tx = binary.BigEndian.Uint64(header[txIdOffset:])
	signSize = binary.BigEndian.Uint32(header[txSignSizeOffset:])
	txSize = binary.BigEndian.Uint32(header[txSizeOffset:])

	return
}

// returns tx signature from backup file and position file pointer after the transaction
func findTxSignature(file io.ReadSeeker, tx uint64) ([]byte, error) {
	for {
		cur, signSize, txSize, err := getTxHeader(file)
		if err != nil {
			return nil, err
		}
		if cur == tx {
			signature := make([]byte, signSize)
			got, err := file.Read(signature)
			if err != nil {
				return nil, err
			}
			if got != len(signature) {
				return nil, ErrMalformedFile
			}
			_, err = file.Seek(int64(txSize), io.SeekCurrent)
			if err != nil {
				return nil, ErrMalformedFile
			}
			return signature, nil
		}
		if cur > tx {
			// should never happen - we should reach EOF instead
			return nil, ErrTxNotInFile
		}
		_, err = file.Seek(int64(signSize+txSize), io.SeekCurrent)
		if err != nil {
			return nil, ErrMalformedFile
		}
	}
}
