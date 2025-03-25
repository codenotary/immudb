/*
Copyright 2025 Codenotary Inc. All rights reserved.

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
	cmd *cobra.Command
}

func newCommandlineHotBck(os immuos.OS) (*commandlineHotBck, error) {
	cl := commandline{}
	cl.config.Name = "immuadmin"
	cl.context = context.Background()
	cl.os = os

	return &commandlineHotBck{commandline: cl}, nil
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
				f, err := cl.verifyOrCreateBackupFile(params)
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
	ccmd.Flags().Bool("progress-bar", false, "show progress indicator")
	ccmd.Flags().Bool("append", false, "append to file, if it already exists (for file output only)")
	cmd.AddCommand(ccmd)
	cl.cmd = cmd
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
	params.progress, err = flags.GetBool("progress-bar")
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

func (cl *commandlineHotBck) verifyOrCreateBackupFile(params *backupParams) (*os.File, error) {
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
		last, fileChecksum, err := lastTxInFile(f)
		if err != nil {
			return nil, err
		}
		txn, err := cl.immuClient.TxByID(cl.context, last)
		if err != nil {
			return nil, fmt.Errorf("cannot find file's last transaction %d in database: %v", last, err)
		}
		alh := schema.TxHeaderFromProto(txn.Header).Alh()
		if !bytes.Equal(fileChecksum, alh[:]) {
			return nil, fmt.Errorf("checksums for transaction %d in backup file and database differ - probably file was created from different database", last)
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

func (cl *commandlineHotBck) runHotBackup(output io.Writer, startTx uint64, progress bool) error {
	state, err := cl.immuClient.CurrentState(cl.context)
	if err != nil {
		return err
	}
	latestTx := state.TxId

	if latestTx < startTx {
		fmt.Fprintf(cl.cmd.ErrOrStderr(), "All backed up, nothing to do\n")
		return nil
	}

	if startTx == latestTx {
		fmt.Fprintf(cl.cmd.ErrOrStderr(), "Backing up transaction %d\n", startTx)
	} else {
		fmt.Fprintf(cl.cmd.ErrOrStderr(), "Backing up transactions from %d to %d\n", startTx, latestTx)
	}

	var bar *progressbar.ProgressBar
	if progress {
		bar = progressbar.NewOptions64(int64(latestTx-startTx+1), progressbar.OptionSetWriter(os.Stderr))
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

	for i := startTx; i <= latestTx; i++ {
		if stop {
			fmt.Fprintf(cl.cmd.ErrOrStderr(), "Terminated by signal - stopped after tx %d\n", i-1)
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

	fmt.Fprintf(cl.cmd.ErrOrStderr(), "Done\n")
	return nil
}

func (cl *commandlineHotBck) backupTx(tx uint64, output io.Writer) error {
	stream, err := cl.immuClient.ExportTx(cl.context, &schema.ExportTxRequest{Tx: tx})
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

	alh := schema.TxHeaderFromProto(txn.Header).Alh()

	err = outputTx(tx, output, alh[:], content)
	if err != nil {
		return err
	}

	return nil
}

func outputTx(tx uint64, output io.Writer, checksum []byte, content []byte) error {
	payload := make([]byte, headerSize, headerSize+len(checksum)+len(content))
	copy(payload[prefixOffset:], prefix)
	binary.BigEndian.PutUint32(payload[versionOffset:], latestFileVersion)
	binary.BigEndian.PutUint64(payload[txIdOffset:], tx)
	binary.BigEndian.PutUint32(payload[txSignSizeOffset:], uint32(len(checksum)))
	binary.BigEndian.PutUint32(payload[txSizeOffset:], uint32(len(content)))
	payload = append(payload, checksum...)
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
	replica  bool
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
				return cl.verifyFile(file)
			}

			dbExist, err := cl.isDbExists(args[0])
			if err != nil {
				return err
			}

			var firstTx uint64
			if dbExist {
				// if initDbForRestore inserts first transaction, it returns non-zero firstTx
				firstTx, err = cl.initDbForRestore(params, args[0], file)
				if err != nil {
					return err
				}
			} else {
				// db does not exist - create as replica and use it
				err = cl.createDb(args[0])
				if err != nil {
					return err
				}
				params.replica = true
			}
			if params.replica {
				defer func() {
					err = cl.immuClient.UpdateDatabase(cl.context, &schema.DatabaseSettings{DatabaseName: args[0], Replica: false})
					if err != nil {
						fmt.Fprintf(cl.cmd.ErrOrStderr(), "Error switching off replica mode for db: %v", err)
					}
				}()
			}

			return cl.runHotRestore(file, params.progress, firstTx)
		},
		Args: func(cmd *cobra.Command, args []string) error {
			verify, _ := cmd.Flags().GetBool("verify-only")
			if verify {
				return cobra.ExactArgs(0)(cmd, args)
			}
			return cobra.ExactArgs(1)(cmd, args)
		},
	}
	ccmd.Flags().StringP("input", "i", "-", "input file file, \"-\" for stdin")
	ccmd.Flags().Bool("verify-only", false, "do not restore data, only verify backup")
	ccmd.Flags().Bool("append", false, "appending to DB, if it already exists")
	ccmd.Flags().Bool("progress-bar", false, "show progress indicator")
	ccmd.Flags().Bool("force", false, "don't check transaction sequence")
	ccmd.Flags().Bool("force-replica", false, "switch database to replica mode for the duration of restore")
	cmd.AddCommand(ccmd)
	cl.cmd = cmd
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
	params.verify, err = flags.GetBool("verify-only")
	if err != nil {
		return nil, err
	}
	params.progress, err = flags.GetBool("progress-bar")
	if err != nil {
		return nil, err
	}
	params.replica, err = flags.GetBool("force-replica")
	if err != nil {
		return nil, err
	}

	return &params, nil
}

func (cl *commandlineHotBck) verifyFile(file io.Reader) error {
	firstTx, _, _, err := nextTx(file)
	if err != nil {
		return err
	}

	lastTx := firstTx
	lastTx, _, err = lastTxInFile(file)
	if err != nil {
		return err
	}

	if lastTx == firstTx {
		fmt.Fprintf(cl.cmd.OutOrStdout(), "Backup file contains transaction %d\n", firstTx)
	} else {
		fmt.Fprintf(cl.cmd.OutOrStdout(), "Backup file contains transactions from %d to %d\n", firstTx, lastTx)
	}
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

// prepare existing Db for restore
// What DB is not empty, it reads first transaction from input stream. Because we cannot re-position in the stream,
// in some situation (when there is no gap and no overlap between DB and file) this transaction gets restored to DB,
// in this case non-zero tx ID is returned
func (cl *commandlineHotBck) initDbForRestore(params *restoreParams, name string, file io.Reader) (uint64, error) {
	lastTx, checksum, err := cl.useDb(name, params.replica)
	if err != nil {
		return 0, nil
	}
	if lastTx == 0 { // db is empty - nothing to verify
		return 0, nil
	}

	if !params.append {
		return 0, errors.New("cannot restore to non-empty database without --append flag")
	}

	// find the nearest transaction in backup file and position the file just after the transaction
	txId, fileChecksum, payload, err := findTx(file, lastTx)
	if err != nil {
		return 0, err
	}

	gap := txId - lastTx

	if gap > 1 {
		return 0, fmt.Errorf("there is a gap of %d transaction(s) between database and file - restore not possible", gap-1)
	}

	if gap == 1 {
		if !params.force {
			return 0, errors.New("not possible to validate last transaction in DB - use --force to override")
		}

		err = cl.restoreTx(fileChecksum, payload)
		if err != nil {
			return 0, err
		}

		return txId, nil // indicate to caller that first transaction to restore is already read
	}

	if !bytes.Equal(fileChecksum, checksum) {
		return 0, fmt.Errorf("checksums for tx %d in backup file and database differ - cannot append data to the database", lastTx)
	}

	return 0, nil
}

func (cl *commandlineHotBck) useDb(name string, replica bool) (uint64, []byte, error) {
	udr, err := cl.immuClient.UseDatabase(cl.context, &schema.Database{DatabaseName: name})
	if err != nil {
		return 0, nil, err
	}
	cl.context = metadata.NewOutgoingContext(cl.context, metadata.Pairs("authorization", udr.GetToken()))

	if replica {
		err = cl.immuClient.UpdateDatabase(cl.context, &schema.DatabaseSettings{DatabaseName: name, Replica: true})
		if err != nil {
			return 0, nil, fmt.Errorf("cannot switch on replica mode for db: %v", err)
		}
	}

	state, err := cl.immuClient.CurrentState(cl.context)
	if err != nil {
		return 0, nil, err
	}

	txn, err := cl.immuClient.TxByID(cl.context, state.TxId)
	if err != nil {
		return 0, nil, err
	}

	alh := schema.TxHeaderFromProto(txn.Header).Alh()
	return state.TxId, alh[:], nil
}

func (cl *commandlineHotBck) createDb(name string) error {
	err := cl.immuClient.CreateDatabase(cl.context, &schema.DatabaseSettings{DatabaseName: name, Replica: true, PrimaryDatabase: "dummy"})
	if err != nil {
		return err
	}

	udr, err := cl.immuClient.UseDatabase(cl.context, &schema.Database{DatabaseName: name})
	if err != nil {
		return err
	}
	cl.context = metadata.NewOutgoingContext(cl.context, metadata.Pairs("authorization", udr.GetToken()))

	return nil
}

// run actual restore. First transaction maybe already restored, so use firstTx as start value, when set
func (cl *commandlineHotBck) runHotRestore(input io.Reader, progress bool, firstTx uint64) error {
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
			fmt.Fprintf(cl.cmd.ErrOrStderr(), "Terminated by signal\n")
			stop = true
		}
	}()

	lastTx := firstTx
	for !stop {
		tx, checksum, payload, err := nextTx(input)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		err = cl.restoreTx(checksum, payload)
		if err != nil {
			return err
		}

		if firstTx == 0 {
			firstTx = tx
		}
		lastTx = tx
		if bar != nil {
			bar.Add(1)
		}
	}

	if firstTx == 0 {
		fmt.Fprintf(cl.cmd.OutOrStdout(), "Target database is up-to-date, nothing restored\n")
	} else if firstTx == lastTx {
		fmt.Fprintf(cl.cmd.OutOrStdout(), "Restored transaction %d\n", firstTx)
	} else {
		fmt.Fprintf(cl.cmd.OutOrStdout(), "Restored transactions from %d to %d\n", firstTx, lastTx)
	}

	return nil
}

func (cl *commandlineHotBck) restoreTx(checksum, payload []byte) error {
	maxChunkSize := uint32(cl.options.StreamChunkSize)

	stream, err := cl.immuClient.ReplicateTx(cl.context)
	if err != nil {
		return err
	}

	offset := uint32(0)
	remainder := uint32(len(payload))

	for remainder > 0 {
		chunkSize := maxChunkSize
		if remainder < maxChunkSize {
			chunkSize = remainder
		}

		err = stream.Send(&schema.Chunk{Content: payload[offset : offset+chunkSize]})
		if err != nil {
			return err
		}

		remainder -= chunkSize
		offset += chunkSize
	}

	hdr, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}

	alh := schema.TxHeaderFromProto(hdr).Alh()

	if !bytes.Equal(checksum, alh[:]) {
		return fmt.Errorf("transaction checksums don't match")
	}

	return nil
}

// lastTxInFile assumes that file is positioned on transaction boundary
// it also can be used to validate file correctness
// in case of success file pointer is positioned to file end
func lastTxInFile(file io.Reader) (uint64, []byte, error) {
	last := uint64(0)
	var checksum []byte
	for {
		var cur uint64
		var err error
		var txSign []byte
		cur, txSign, _, err = nextTx(file)
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
		checksum = txSign
	}

	return last, checksum, nil
}

// returns tx smallest possible ID which is greater of equal to the one requested
func findTx(file io.Reader, tx uint64) (txID uint64, checksum []byte, payload []byte, err error) {
	for {
		txID, checksum, payload, err = nextTx(file)
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
func nextTx(file io.Reader) (txId uint64, checksum []byte, tx []byte, err error) {
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

	checksum = payload[:signSize]
	tx = payload[signSize:]

	return
}
