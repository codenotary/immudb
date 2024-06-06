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

package immuadmin

import (
	"context"
	"errors"
	"fmt"
	stdos "os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	daem "github.com/takama/daemon"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client/homedir"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"github.com/codenotary/immudb/pkg/fs"
	"github.com/codenotary/immudb/pkg/immuos"
	"github.com/codenotary/immudb/pkg/server"
)

type backupper struct {
	daemon daem.Daemon
	os     immuos.OS
	copier fs.Copier
	tarer  fs.Tarer
	ziper  fs.Ziper
}

func newBackupper(os immuos.OS) (*backupper, error) {
	d, err := daem.New("immudb", "", "")
	if err != nil {
		return nil, err
	}
	return &backupper{
			daemon: d,
			os:     os,
			copier: fs.NewStandardCopier(),
			tarer:  fs.NewStandardTarer(),
			ziper:  fs.NewStandardZiper()},
		nil
}

// Backupper ...
type Backupper interface {
	mustNotBeWorkingDir(p string) error
	stopImmudbService() (func(), error)
	offlineBackup(src string, uncompressed bool, manualStopStart bool) (string, error)
	offlineRestore(src string, dst string, manualStopStart bool) (string, error)
}

type commandlineBck struct {
	commandline
	Backupper
	c.TerminalReader
}

func newCommandlineBck(os immuos.OS) (*commandlineBck, error) {
	b, err := newBackupper(os)
	if err != nil {
		return nil, err
	}
	cl := commandline{}
	cl.config.Name = "immuadmin"
	cl.passwordReader = c.DefaultPasswordReader
	cl.context = context.Background()
	cl.os = os
	tr := c.NewTerminalReader(stdos.Stdin)

	return &commandlineBck{cl, b, tr}, nil
}

func (clb *commandlineBck) Register(rootCmd *cobra.Command) *cobra.Command {
	clb.dumpToFile(rootCmd)
	clb.backup(rootCmd)
	clb.restore(rootCmd)
	return rootCmd
}

func (cl *commandlineBck) ConfigChain(post func(cmd *cobra.Command, args []string) error) func(cmd *cobra.Command, args []string) (err error) {
	return func(cmd *cobra.Command, args []string) (err error) {
		if err = cl.config.LoadConfig(cmd); err != nil {
			return err
		}
		// here all command line options and services need to be configured by options retrieved from viper
		cl.options = Options()
		cl.ts = tokenservice.NewFileTokenService().WithHds(homedir.NewHomedirService()).WithTokenFileName(cl.options.TokenFileName)
		if post != nil {
			return post(cmd, args)
		}
		return nil
	}
}

func (cl *commandlineBck) dumpToFile(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "dump [file]",
		Short:             "Dump database content to a file",
		PersistentPreRunE: cl.ConfigChain(cl.checkLoggedInAndConnect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			filename := fmt.Sprint("immudb_" + time.Now().Format("2006-01-02_15-04-05") + ".bkp")
			if len(args) > 0 {
				filename = args[0]
			}
			file, err := cl.os.Create(filename)
			defer file.Close()
			if err != nil {
				cl.quit(err)
				return nil
			}
			ctx := cl.context
			response, err := cl.immuClient.Dump(ctx, file)
			if err != nil {
				color.Set(color.FgHiBlue, color.Bold)
				fmt.Println("Backup failed.")
				color.Unset()
				cl.os.Remove(filename)
				cl.quit(err)
				return nil
			} else if response == 0 {
				fmt.Println("Database is empty.")
				cl.os.Remove(filename)
				return nil
			}
			fmt.Printf("SUCCESS: %d key-value entries were backed-up to file %s\n", response, filename)
			return nil
		},
		Args: cobra.MaximumNArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandlineBck) backup(cmd *cobra.Command) {
	defaultDbDir := server.DefaultOptions().Dir
	ccmd := &cobra.Command{
		Use:   "backup [--dbdir] [--manual-stop-start] [--uncompressed]",
		Short: "Make a copy of the database files and folders",
		Long: "Pause the immudb server, create and save on the server machine a snapshot " +
			"of the database files and folders (zip on Windows, tar.gz on Linux or uncompressed).",
		PersistentPreRunE: cl.ConfigChain(nil),
		RunE: func(cmd *cobra.Command, args []string) error {
			dbDir, err := cmd.Flags().GetString("dbdir")
			if err != nil {
				cl.quit(err)
				return nil
			}
			if err = cl.mustNotBeWorkingDir(dbDir); err != nil {
				cl.quit(err)
				return nil
			}
			manualStopStart, err := cmd.Flags().GetBool("manual-stop-start")
			if err != nil {
				cl.quit(err)
				return nil
			}
			uncompressed, err := cmd.Flags().GetBool("uncompressed")
			if err != nil {
				cl.quit(err)
				return nil
			}
			if err := cl.askUserConfirmation("backup", manualStopStart); err != nil {
				cl.quit(err)
				return nil
			}
			backupPath, err := cl.offlineBackup(dbDir, uncompressed, manualStopStart)
			if err != nil {
				cl.quit(err)
				return nil
			}
			fmt.Printf("Database backup created: %s\n", backupPath)
			return nil
		},
		Args: cobra.NoArgs,
	}
	ccmd.Flags().String("dbdir", defaultDbDir, fmt.Sprintf("path to the server database directory to backup (default %s)", defaultDbDir))
	ccmd.Flags().Bool("manual-stop-start", false, "server stop before and restart after the backup are to be handled manually by the user (default false)")
	ccmd.Flags().BoolP("uncompressed", "u", false, "create an uncompressed backup (i.e. make just a copy of the db directory)")
	cmd.AddCommand(ccmd)
}

func (cl *commandlineBck) restore(cmd *cobra.Command) {
	defaultDbDir := server.DefaultOptions().Dir
	ccmd := &cobra.Command{
		Use:   "restore snapshot-path [--dbdir] [--manual-stop-start]",
		Short: "Restore the database from a snapshot archive or folder",
		Long: "Pause the immudb server and restore the database files and folders from a snapshot " +
			"file (zip or tar.gz) or folder (uncompressed) residing on the server machine.",
		PersistentPreRunE: cl.ConfigChain(nil),
		RunE: func(cmd *cobra.Command, args []string) error {
			snapshotPath := args[0]
			dbDir, err := cmd.Flags().GetString("dbdir")
			if err != nil {
				cl.quit(err)
				return nil
			}
			manualStopStart, err := cmd.Flags().GetBool("manual-stop-start")
			if err != nil {
				cl.quit(err)
				return nil
			}
			if err := cl.askUserConfirmation("restore", manualStopStart); err != nil {
				cl.quit(err)
				return nil
			}
			autoBackupPath, err := cl.offlineRestore(snapshotPath, dbDir, manualStopStart)
			if err != nil {
				cl.quit(err)
				return nil
			}
			fmt.Printf("Database restored from backup %s\n", snapshotPath)
			fmt.Printf("A backup of the previous database has been also created: %s\n", autoBackupPath)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	ccmd.Flags().String("dbdir", defaultDbDir, fmt.Sprintf("path to the server database directory which will be replaced by the backup (default %s)", defaultDbDir))
	ccmd.Flags().Bool("manual-stop-start", false, "server stop before and restart after the backup are to be handled manually by the user (default false)")
	cmd.AddCommand(ccmd)
}

func (cl *commandlineBck) askUserConfirmation(process string, manualStopStart bool) error {
	if !manualStopStart {
		fmt.Printf(
			"Server will be stopped and then restarted during the %s process.\n"+
				"NOTE: If the backup process is forcibly interrupted, a manual restart "+
				"of the immudb service may be needed.\n"+
				"Are you sure you want to proceed? [y/N]: ", process)
		answer, err := cl.ReadFromTerminalYN("N")
		if err != nil || !(strings.ToUpper("Y") == strings.TrimSpace(strings.ToUpper(answer))) {
			return errors.New("Canceled")

		}
		pass, err := cl.passwordReader.Read("Enter admin password:")
		if err != nil {
			return err
		}
		_ = cl.checkLoggedInAndConnect(nil, nil)
		defer cl.disconnect(nil, nil)
		if _, err = cl.immuClient.Login(cl.context, []byte(auth.SysAdminUsername), pass); err != nil {
			return err
		}
	} else {
		fmt.Print("Please make sure the immudb server is not running before proceeding. Are you sure you want to proceed? [y/N]: ")
		answer, err := cl.ReadFromTerminalYN("N")
		if err != nil || !(strings.ToUpper("Y") == strings.TrimSpace(strings.ToUpper(answer))) {
			return errors.New("Canceled")
		}
	}
	return nil
}

func (b *backupper) mustNotBeWorkingDir(p string) error {
	currDir, err := b.os.Getwd()
	if err != nil {
		return err
	}
	pathAbs, err := b.os.Abs(p)
	if err != nil {
		return err
	}
	currDirAbs, err := b.os.Abs(currDir)
	if err != nil {
		return err
	}
	if pathAbs == currDirAbs {
		return fmt.Errorf(
			"cannot backup the current directory, please specify a subdirectory, for example ./data")
	}
	return nil
}

func (b *backupper) stopImmudbService() (func(), error) {
	if _, err := b.daemon.Stop(); err != nil {
		return nil, fmt.Errorf("error stopping immudb server: %v", err)
	}
	return func() {
		if _, err := b.daemon.Start(); err != nil {
			fmt.Fprintf(stdos.Stderr, "error restarting immudb server: %v", err)
		}
	}, nil
}

func (b *backupper) offlineBackup(src string, uncompressed bool, manualStopStart bool) (string, error) {
	srcInfo, err := b.os.Stat(src)
	if err != nil {
		return "", err
	}
	if !srcInfo.IsDir() {
		return "", fmt.Errorf("%s is not a directory", src)
	}

	if !manualStopStart {
		startImmudbService, err := b.stopImmudbService()
		if err != nil {
			return "", err
		}
		defer startImmudbService()
	}

	srcBase := b.os.Base(src)
	snapshotPath := srcBase + "_bkp_" + time.Now().Format("2006-01-02_15-04-05")
	if err = b.copier.CopyDir(src, snapshotPath); err != nil {
		return "", err
	}
	// remove the immudb.identifier file from the backup
	if err = b.os.Remove(snapshotPath + "/" + server.IDENTIFIER_FNAME); err != nil {
		fmt.Fprintf(stdos.Stderr,
			"error removing immudb identifier file %s from db snapshot %s: %v",
			server.IDENTIFIER_FNAME, snapshotPath, err)
	}
	if uncompressed {
		absSnapshotPath, err := b.os.Abs(snapshotPath)
		if err != nil {
			fmt.Fprintf(stdos.Stderr,
				"error converting to absolute path the rel path %s of the uncompressed backup: %v",
				snapshotPath, err)
			absSnapshotPath = snapshotPath
		}
		return absSnapshotPath, nil
	}

	var archivePath string
	var archiveErr error
	if runtime.GOOS != "windows" {
		archivePath = snapshotPath + ".tar.gz"
		archiveErr = b.tarer.TarIt(snapshotPath, archivePath)
	} else {
		archivePath = snapshotPath + ".zip"
		archiveErr = b.ziper.ZipIt(snapshotPath, archivePath, fs.ZipDefaultCompression)
	}
	if archiveErr != nil {
		return "", fmt.Errorf(
			"database copied successfully to %s, but compression to %s failed: %v",
			snapshotPath, archivePath, archiveErr)
	}
	if err = b.os.RemoveAll(snapshotPath); err != nil {
		fmt.Fprintf(stdos.Stderr,
			"error removing db snapshot dir %s after successfully compressing it to %s: %v",
			snapshotPath, archivePath, err)
	}

	absArchivePath, err := b.os.Abs(archivePath)
	if err != nil {
		fmt.Fprintf(stdos.Stderr,
			"error converting to absolute path the rel path %s of the archived backup: %v",
			archivePath, err)
		absArchivePath = archivePath
	}

	return absArchivePath, nil
}

func (b *backupper) offlineRestore(src string, dst string, manualStopStart bool) (string, error) {
	snapshotPath := src
	_, err := b.os.Stat(snapshotPath)
	if err != nil {
		return "", err
	}
	snapshotExt := b.os.Ext(snapshotPath)
	snapshotName := b.os.Base(snapshotPath)
	snapshotNameNoExt := strings.TrimSuffix(snapshotName, snapshotExt)
	if strings.ToLower(snapshotExt) == ".gz" {
		snapshotExt = b.os.Ext(snapshotNameNoExt) + snapshotExt
		snapshotNameNoExt = strings.TrimSuffix(snapshotName, snapshotExt)
	}
	dbParentDir := b.os.Dir(dst) + string(stdos.PathSeparator)
	extractedSnapshotDir := dbParentDir + snapshotNameNoExt
	now := time.Now().Format("2006-01-02_15-04-05")
	var extract func(string, string) error
	switch snapshotExt {
	case ".tar.gz":
		extract = b.tarer.UnTarIt
	case ".zip":
		extract = b.ziper.UnZipIt
	case "": // uncompressed
		// TODO OGG: this will result in the backup being renamed directly to the db folder
		if dbParentDir != b.os.Dir(snapshotPath)+string(stdos.PathSeparator) {
			extract = b.copier.CopyDir
		}
	default:
		return "", fmt.Errorf(
			"snapshot %s has unsupported format %s; supported formats: .tar.gz, .zip or none (uncompressed)",
			snapshotPath, snapshotExt)
	}

	if !manualStopStart {
		startImmudbService, err := b.stopImmudbService()
		if err != nil {
			return "", err
		}
		defer startImmudbService()
	}

	if extract != nil {
		if err = extract(snapshotPath, dbParentDir); err != nil {
			return "", err
		}
	}
	// keep the same db identifier
	serverIDSrc := path.Join(dst, server.IDENTIFIER_FNAME)
	serverIDDst := path.Join(extractedSnapshotDir, server.IDENTIFIER_FNAME)
	if err = b.copier.CopyFile(serverIDSrc, serverIDDst); err != nil {
		fmt.Fprintf(stdos.Stderr,
			"error copying immudb identifier file %s to %s: %v",
			serverIDSrc, serverIDDst, err)
	}

	dbDirAutoBackupPath := dst + "_bkp_before_restore_" + now
	if err = b.os.Rename(dst, dbDirAutoBackupPath); err != nil {
		return "", fmt.Errorf(
			"error renaming previous db dir %s to %s during restore: %v",
			dst, dbDirAutoBackupPath, err)
	}
	if err = b.os.Rename(extractedSnapshotDir, dst); err != nil {
		return "", fmt.Errorf(
			"error renaming new tmp snapshot dir %s to db dir %s during restore: %v",
			extractedSnapshotDir, dst, err)
	}

	return dbDirAutoBackupPath, nil
}
