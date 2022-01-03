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

//	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
//	"google.golang.org/grpc/status"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"

	"github.com/codenotary/immudb/pkg/api/schema"
//	immuerrors "github.com/codenotary/immudb/pkg/errors"
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
			inc, err := cmd.Flags().GetBool("incremental")
			if err != nil {
				return err
			}
			last := uint64(0)
			if output != "-" {
				var f *os.File
				_, err := os.Stat(output)
				if err == nil {
					if !inc {
						return errors.New("file already exists, use 'incremental' option to append to the file")
					}
					f, err = os.OpenFile(output, os.O_RDWR, 0)
					if err != nil {
						return err
					}
					last, err = lastTx(f)
					if err != nil {
						return err
					}
					_, err = f.Seek(0, io.SeekEnd)
					if err != nil {
						return err
					}
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
			} else if inc {
				return errors.New("'incremental' option can be used only when outputting to the file")
			}

			udr, err := cl.immuClient.UseDatabase(cl.context, &schema.Database{DatabaseName: args[0]})
			if err != nil {
				return err
			}
			cl.context = metadata.NewOutgoingContext(cl.context, metadata.Pairs("authorization", udr.GetToken()))
			progress, _ := cmd.Flags().GetBool("progress")
			return cl.runHotBackup(file, last+1, progress)
		},
		Args: cobra.ExactArgs(1),
	}
	ccmd.Flags().StringP("output", "o", "-", "output file, \"-\" for stdout")
	ccmd.Flags().Uint64("start-tx", 0, "Transaction ID to start from")
	ccmd.Flags().Bool("progress", false, "show progress indicator")
	ccmd.Flags().BoolP("incremental", "i", false, "allow incremental backup (for file output only)")
	cmd.AddCommand(ccmd)
}

func (cl *commandlineBck) hotRestore(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:   "hot-restore <db_name>",
		Short: "Restore data from backup file",
		Long: "Restore saved transaction from backup file without stopping the database engine. " +
			"Restore can create the database or apply only the missing data.",
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
				last, err := lastTx(file)
				if err != nil {
					return err
				}
				fmt.Printf("Backup file contains %d transactions\n", last)
				return nil
			}

			append, err := cmd.Flags().GetBool("append")
			if err != nil {
				return err
			}

			dbList, err := cl.immuClient.DatabaseList(cl.context)
			if err != nil {
				return err
			}
			exist := false
			for _, db := range dbList.Databases {
				if db.DatabaseName == args[0] {
					exist = true
					break
				}
			}

			if exist {
				udr, err := cl.immuClient.UseDatabase(cl.context, &schema.Database{DatabaseName: args[0]})
				if err != nil {
					return err
				}
				cl.context = metadata.NewOutgoingContext(cl.context, metadata.Pairs("authorization", udr.GetToken()))

				// find the last tansaction and verify it
				state, err := cl.immuClient.CurrentState(cl.context)
				if err != nil {
					return err
				}
				if state.TxId > 0 {
					if !append {
						return errors.New("cannot restore to non-empty database without `append` flag")
					}
					// find the transaction in backup file and validate the signature
					fileSignature, err := findTxSignature(file, state.TxId)
					if err != nil {
						return err
					}
					txn, err := cl.immuClient.TxByID(cl.context, state.TxId)
					if err != nil {
						return err
					}
					if !bytes.Equal(fileSignature, txn.Header.EH) {
						return fmt.Errorf("signatures for tx %d in backup file and database differ - cannot perform incremental restore", state.TxId)
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

				// find the last tansaction and verify it
				state, err := cl.immuClient.CurrentState(cl.context)
				if err != nil {
					return err
				}
				fmt.Printf("DB has %d txns\n", state.TxId)				
			}
			defer func() {
				err = cl.immuClient.UpdateDatabase(cl.context, &schema.DatabaseSettings{DatabaseName: args[0], Replica: false})
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error switching off replica mode for db: %v", err)
				}
			}()

			return cl.runHotRestore(file)
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
	ccmd.Flags().Bool("append", false, "allow appending to DB, if it already exists")
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
		bar = progressbar.Default(int64(txnCount))
	}

	terminated := make(chan os.Signal, 1)
	signal.Notify(terminated, os.Interrupt)

	for i := startFrom; i <= txnCount; i++ {
		err = cl.processTx(i, output, bar, terminated)
		if err != nil {
			return err
		}
	}

	fmt.Fprintf(os.Stderr, "Done\n")
	return nil
}

func (cl *commandlineBck) runHotRestore(input io.Reader) error {
	var startTx, lastTx uint64
	maxPayload := uint32(cl.options.MaxRecvMsgSize)
	for {
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
		payload := make([]byte, signSize + txSize)
		got, err := input.Read(payload)
		if err != nil {
			return err
		}
		if uint32(got) < signSize + txSize {
			return ErrMalformedFile
		}
		stream, err := cl.immuClient.ReplicateTx(cl.context)
		if err != nil {
			return err
		}
		offset := signSize
		for txSize > 0 {
			chunkSize := maxPayload
			if txSize < maxPayload {
				chunkSize = txSize
			}
			err = stream.Send(&schema.Chunk{Content: payload[offset:offset+chunkSize]})
			if err != nil {
				return err
			}
			txSize -= chunkSize
			offset += chunkSize
		}
		metadata, err := stream.CloseAndRecv()
		if err != nil {
			return err
		}
		if !bytes.Equal(payload[:signSize], metadata.EH) {
			return fmt.Errorf("signatures for tx %d don't match", tx)
		}
		lastTx = tx
	}
	fmt.Printf("Restored transactions %d - %d\n", startTx, lastTx)

	return nil
}

func (cl *commandlineBck) processTx(tx uint64, output io.Writer, bar *progressbar.ProgressBar, terminated chan os.Signal) error {
	stream, err := cl.immuClient.ExportTx(cl.context, &schema.TxRequest{Tx: tx})
	if err != nil {
		return fmt.Errorf("failed to export transaction: %w", err)
	}

	completed := make(chan struct{})
	var content []byte
	go func() {
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
		close(completed)
	}()

	select {
	case <-completed:
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
		if bar != nil {
			bar.Add(1)
		}

	case <-terminated:
		return fmt.Errorf("terminated by signal")
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

// lastTx assumes that file is positioned on transaction boundary
// it also can be used to validate file correctness
func lastTx(file io.ReadSeeker) (uint64, error) {
	last := uint64(0)
	for {
		cur, signSize, txSize, err := getTxHeader(file)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, err
		}
		_, err = file.Seek(int64(signSize+txSize), io.SeekCurrent)
		if err != nil {
			return 0, ErrMalformedFile
		}
		if cur != last+1 {
			return 0, ErrTxWrongOrder
		}
		last = cur
	}

	return last, nil
}

// reads header for the next transaction and position file pointer just after the headder, before the payload
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
			return nil, errors.New("transaction not in file")
		}
		_, err = file.Seek(int64(signSize+txSize), io.SeekCurrent)
		if err != nil {
			return nil, ErrMalformedFile
		}
	}
}
