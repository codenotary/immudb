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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"

	"google.golang.org/grpc/metadata"

	"github.com/schollz/progressbar/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/immuos"
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

type commandlineHotBck struct {
	commandline
}

func newCommandlineHotBck(os immuos.OS) (*commandlineHotBck, error) {
	cl := commandline{}
	cl.config.Name = "immuadmin"
	cl.context = context.Background()
	cl.os = os

	return &commandlineHotBck{cl}, nil
}

func (clb *commandlineHotBck) Register(rootCmd *cobra.Command) *cobra.Command {
	clb.hotBackup(rootCmd)
	clb.hotRestore(rootCmd)
	return rootCmd
}

type backupParams struct {
	output   string
	startTx  uint64
	append   bool
	progress bool
}

func (cl *commandlineHotBck) hotBackup(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:   "hot-backup <db_name>",
		Short: "Make a copy of the database without stopping",
		Long: "Backup a database to file/stream without stopping the database engine. " +
			"Backup can run from the beginning or starting from arbitrary transaction.",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			file := io.Writer(os.Stdout)
			params, err := prepareBackupParams(cmd.Flags())
			if err != nil {
				return err
			}

			udr, err := cl.immuClient.UseDatabase(cl.context, &schema.Database{DatabaseName: args[0]})
			if err != nil {
				return err
			}
			cl.context = metadata.NewOutgoingContext(cl.context, metadata.Pairs("authorization", udr.GetToken()))

			if params.output != "-" {
				f, err := cl.verifyBackupFile(params)
				if err != nil {
					return err
				}
				defer f.Close()
				file = f
			}

			return cl.runHotBackup(file, params.startTx, params.progress)
		},
		Args: cobra.ExactArgs(1),
	}
	ccmd.Flags().StringP("output", "o", "-", "output file, \"-\" for stdout")
	ccmd.Flags().Uint64("start-tx", 1, "Transaction ID to start from")
	ccmd.Flags().Bool("progress", false, "show progress indicator")
	ccmd.Flags().BoolP("append", "i", false, "append to file, if it already exists (for file output only)")
	cmd.AddCommand(ccmd)
}

func prepareBackupParams(flags *pflag.FlagSet) (*backupParams, error) {
	var params backupParams
	var err error

	params.output, err = flags.GetString("output")
	if err != nil {
		return nil, err
	}
	params.append, err = flags.GetBool("append")
	if err != nil {
		return nil, err
	}
	params.startTx, err = flags.GetUint64("start-tx")
	if err != nil {
		return nil, err
	}
	params.progress, err = flags.GetBool("progress")
	if err != nil {
		return nil, err
	}

	if params.startTx > 1 && params.append {
		return nil, errors.New("don't use --append and --start-tx options together")
	}

	if params.output == "-" && params.append {
		return nil, errors.New("--append option can be used only when outputting to the file")
	}

	return &params, nil
}

func (cl *commandlineHotBck) verifyBackupFile(params *backupParams) (*os.File, error) {
	var f *os.File

	_, err := os.Stat(params.output)
	if err == nil {
		// file exists - find last tx in file and check whether it matches the one in DB
		if !params.append {
			return nil, errors.New("file already exists, use --append option to append new data to file")
		}
		f, err = os.OpenFile(params.output, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		last, fileSignature, err := lastTxInFile(f)
		if err != nil {
			return nil, err
		}
		txn, err := cl.immuClient.TxByID(cl.context, last)
		if err != nil {
			return nil, fmt.Errorf("cannot find file's last transaction %d in database: %v", last, err)
		}
		if !bytes.Equal(fileSignature, txn.Header.EH) {
			return nil, fmt.Errorf("signatures for transaction %d in backup file and database differ - probably file was created from different database", last)
		}
		params.startTx = last + 1
	} else if os.IsNotExist(err) {
		f, err = os.Create(params.output)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}

	return f, nil
}

func (cl *commandlineHotBck) runHotBackup(output io.Writer, startFrom uint64, progress bool) error {
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
		bar = progressbar.NewOptions64(int64(txnCount), progressbar.OptionSetWriter(os.Stderr))
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

func (cl *commandlineHotBck) backupTx(tx uint64, output io.Writer) error {
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

type restoreParams struct {
	input    string
	append   bool
	progress bool
	force    bool
	verify   bool
}

func (cl *commandlineHotBck) hotRestore(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:   "hot-restore <db_name>",
		Short: "Restore data from backup file",
		Long: "Restore saved transaction from backup file without stopping the database engine. " +
			"Restore can restore the data from scratch or apply only the missing data.",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {

			params, err := prepareRestoreParams(cmd.Flags())
			if err != nil {
				return err
			}

			file := io.Reader(os.Stdin)
			if params.input != "-" {
				f, err := os.Open(params.input)
				if err != nil {
					return err
				}
				file = f
				defer f.Close()
			}

			if params.verify {
				return verifyFile(file)
			}

			dbExist, err := cl.isDbExists(args[0])
			if err != nil {
				return err
			}

			var signature, payload []byte
			var firstTx uint64
			if dbExist {
				firstTx, signature, payload, err = cl.verifyDb(params, args[0], file)
				if err != nil {
					return err
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

			return cl.runHotRestore(file, params.progress, firstTx, signature, payload)
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

func prepareRestoreParams(flags *pflag.FlagSet) (*restoreParams, error) {
	var params restoreParams
	var err error

	params.input, err = flags.GetString("input")
	if err != nil {
		return nil, err
	}
	params.append, err = flags.GetBool("append")
	if err != nil {
		return nil, err
	}
	params.force, err = flags.GetBool("force")
	if err != nil {
		return nil, err
	}
	params.verify, err = flags.GetBool("verify")
	if err != nil {
		return nil, err
	}
	params.progress, err = flags.GetBool("progress")
	if err != nil {
		return nil, err
	}

	return &params, nil
}

func verifyFile(file io.Reader) error {
	fileStart, _, _, err := getTx(file)
	if err != nil {
		return err
	}

	last := fileStart
	last, _, err = lastTxInFile(file)
	if err != nil {
		return err
	}
	fmt.Printf("Backup file contains transactions %d - %d\n", fileStart, last)
	return nil
}

func (cl *commandlineHotBck) isDbExists(name string) (bool, error) {
	dbList, err := cl.immuClient.DatabaseList(cl.context)
	if err != nil {
		return false, err
	}

	dbExist := false
	for _, db := range dbList.Databases {
		if db.DatabaseName == name {
			dbExist = true
			break
		}
	}

	return dbExist, nil
}

func (cl *commandlineHotBck) verifyDb(params *restoreParams, name string, file io.Reader) (uint64, []byte, []byte, error) {
	udr, err := cl.immuClient.UseDatabase(cl.context, &schema.Database{DatabaseName: name})
	if err != nil {
		return 0, nil, nil, err
	}
	cl.context = metadata.NewOutgoingContext(cl.context, metadata.Pairs("authorization", udr.GetToken()))

	state, err := cl.immuClient.CurrentState(cl.context)
	if err != nil {
		return 0, nil, nil, err
	}

	if state.TxId > 0 {
		// database isn't empty
		if !params.append {
			return 0, nil, nil, errors.New("cannot restore to non-empty database without --append flag")
		}
		// find the nearest transaction in backup file and position the file just after the transaction
		txId, signature, payload, err := findTx(file, state.TxId)
		if err != nil {
			return 0, nil, nil, err
		}
		gap := txId - state.TxId
		if gap > 1 {
			return 0, nil, nil, fmt.Errorf("there is a gap of %d transaction(s) between database and file - restore not possible", gap-1)
		}
		if gap == 1 {
			if !params.force {
				return 0, nil, nil, errors.New("not possible to validate last transaction in DB - use --force to override")
			}
			return txId, signature, payload, nil // indicate to caller that first transaction to restore is already read
		}

		txn, err := cl.immuClient.TxByID(cl.context, state.TxId)
		if err != nil {
			return 0, nil, nil, err
		}
		if !bytes.Equal(signature, txn.Header.EH) {
			return 0, nil, nil, fmt.Errorf("signatures for tx %d in backup file and database differ - cannot append data to the database", state.TxId)
		}
	}

	return 0, nil, nil, nil
}

func (cl *commandlineHotBck) runHotRestore(input io.Reader, progress bool, firstTx uint64, signature, payload []byte) error {
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
			fmt.Fprintf(os.Stderr, "Terminated by signal\n")
			stop = true
		}
	}()

	if firstTx != 0 {
		// first transaction was already read during DB verification, so process it first
		err := cl.restoreTx(signature, payload)
		if err != nil {
			return err
		}
		startTx = firstTx
		lastTx = firstTx
	}

	for !stop {
		tx, signature, payload, err := getTx(input)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		err = cl.restoreTx(signature, payload)
		if err != nil {
			return err
		}

		if startTx == 0 {
			startTx = tx
		}
		lastTx = tx
		if bar != nil {
			bar.Add(1)
		}
	}
	if startTx == 0 {
		fmt.Printf("Target database is up-to-date, nothing restored\n")
	} else {
		fmt.Printf("Restored transactions %d - %d\n", startTx, lastTx)
	}

	return nil
}

func (cl *commandlineHotBck) restoreTx(signature, payload []byte) error {
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

// lastTxInFile assumes that file is positioned on transaction boundary
// it also can be used to validate file correctness
// in case of success file pointer is positioned to file end
func lastTxInFile(file io.Reader) (uint64, []byte, error) {
	last := uint64(0)
	var signature []byte
	for {
		var cur uint64
		var err error
		cur, signature, _, err = getTx(file)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, nil, err
		}

		if cur != last+1 && last > 0 {
			return 0, nil, ErrTxWrongOrder
		}
		last = cur
	}

	return last, signature, nil
}

// returns tx smallest possible ID which is greater of equal to the one requested
func findTx(file io.Reader, tx uint64) (txID uint64, signature []byte, payload []byte, err error) {
	for {
		txID, signature, payload, err = getTx(file)
		if err != nil {
			return
		}
		if txID >= tx {
			break
		}
	}

	return
}

// read transaction, position stream pointer just after the transaction
func getTx(file io.Reader) (txId uint64, signature []byte, tx []byte, err error) {
	header := make([]byte, headerSize)
	_, err = io.ReadFull(file, header)
	if err != nil {
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

	txId = binary.BigEndian.Uint64(header[txIdOffset:])
	signSize := binary.BigEndian.Uint32(header[txSignSizeOffset:])
	txSize := binary.BigEndian.Uint32(header[txSizeOffset:])

	payload := make([]byte, signSize+txSize)
	_, err = io.ReadFull(file, payload)
	if err != nil {
		return
	}

	signature = payload[:signSize]
	tx = payload[signSize:]

	return
}
