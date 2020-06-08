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

package immuadmin

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/immuadmin/command/service"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/fs"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

func (cl *commandline) dumpToFile(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "dump [file]",
		Short:             "Dump database content to a file",
		PersistentPreRunE: cl.checkLoggedInAndConnect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			filename := fmt.Sprint("immudb_" + time.Now().Format("2006-01-02_15-04-05") + ".bkp")
			if len(args) > 0 {
				filename = args[0]
			}
			file, err := os.Create(filename)
			defer file.Close()
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := cl.context
			response, err := cl.immuClient.Dump(ctx, file)
			if err != nil {
				color.Set(color.FgHiBlue, color.Bold)
				fmt.Println("Backup failed.")
				color.Unset()
				os.Remove(filename)
				c.QuitWithUserError(err)
			} else if response == 0 {
				fmt.Println("Database is empty.")
				os.Remove(filename)
				return nil
			}
			fmt.Printf("SUCCESS: %d key-value entries were backed-up to file %s\n", response, filename)
			return nil
		},
		Args: cobra.MaximumNArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) backup(cmd *cobra.Command) {
	defaultDbDir := server.DefaultOptions().Dir
	ccmd := &cobra.Command{
		Use:   "backup [--dbdir] [--manual-stop-start] [--uncompressed]",
		Short: "Make a copy of the database files and folders",
		Long: "Pause the immudb server, create and save on the server machine a snapshot " +
			"of the database files and folders (zip on Windows, tar.gz on Linux or uncompressed).",
		RunE: func(cmd *cobra.Command, args []string) error {
			dbDir, err := cmd.Flags().GetString("dbdir")
			if err != nil {
				c.QuitToStdErr(err)
			}
			if err = mustNotBeWorkingDir(dbDir); err != nil {
				c.QuitToStdErr(err)
			}
			manualStopStart, err := cmd.Flags().GetBool("manual-stop-start")
			if err != nil {
				c.QuitToStdErr(err)
			}
			uncompressed, err := cmd.Flags().GetBool("uncompressed")
			if err != nil {
				c.QuitToStdErr(err)
			}
			cl.askUserConfirmation("backup", manualStopStart)
			backupPath, err := offlineBackup(dbDir, uncompressed, manualStopStart)
			if err != nil {
				c.QuitToStdErr(err)
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

func (cl *commandline) restore(cmd *cobra.Command) {
	defaultDbDir := server.DefaultOptions().Dir
	ccmd := &cobra.Command{
		Use:   "restore snapshot-path [--dbdir] [--manual-stop-start]",
		Short: "Restore the database from a snapshot archive or folder",
		Long: "Pause the immudb server and restore the database files and folders from a snapshot " +
			"file (zip or tar.gz) or folder (uncompressed) residing on the server machine.",
		RunE: func(cmd *cobra.Command, args []string) error {
			snapshotPath := args[0]
			dbDir, err := cmd.Flags().GetString("dbdir")
			if err != nil {
				c.QuitToStdErr(err)
			}
			manualStopStart, err := cmd.Flags().GetBool("manual-stop-start")
			if err != nil {
				c.QuitToStdErr(err)
			}
			cl.askUserConfirmation("restore", manualStopStart)
			autoBackupPath, err := offlineRestore(snapshotPath, dbDir, manualStopStart)
			if err != nil {
				c.QuitToStdErr(err)
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

func mustNotBeWorkingDir(p string) error {
	currDir, err := os.Getwd()
	if err != nil {
		return err
	}
	pathAbs, err := filepath.Abs(p)
	if err != nil {
		return err
	}
	currDirAbs, err := filepath.Abs(currDir)
	if err != nil {
		return err
	}
	if pathAbs == currDirAbs {
		return fmt.Errorf(
			"cannot backup the current directory, please specify a subdirectory, for example ./db")
	}
	return nil
}

func (cl *commandline) askUserConfirmation(process string, manualStopStart bool) {
	if !manualStopStart {
		fmt.Printf(
			"Server will be stopped and then restarted during the %s process.\n"+
				"NOTE: If the backup process is forcibly interrupted, a manual restart "+
				"of the immudb service may be needed.\n"+
				"Are you sure you want to proceed? [y/N]: ", process)
		answer, err := c.ReadFromTerminalYN("N")
		if err != nil || !(strings.ToUpper("Y") == strings.TrimSpace(strings.ToUpper(answer))) {
			c.QuitToStdErr("Canceled")
		}
		pass, err := cl.passwordReader.Read("Enter admin password:")
		if err != nil {
			c.QuitToStdErr(err)
		}
		_ = cl.checkLoggedInAndConnect(nil, nil)
		defer cl.disconnect(nil, nil)
		if _, err = cl.immuClient.Login(cl.context, []byte(auth.AdminUsername), pass); err != nil {
			c.QuitWithUserError(err)
		}
	} else {
		fmt.Print("Please make sure the immudb server is not running before proceeding. Are you sure you want to proceed? [y/N]: ")
		answer, err := c.ReadFromTerminalYN("N")
		if err != nil || !(strings.ToUpper("Y") == strings.TrimSpace(strings.ToUpper(answer))) {
			c.QuitToStdErr("Canceled")
		}
	}
}

func stopImmudbService() (func(), error) {
	daemon, err := service.NewDaemon("immudb", "", "")
	if err != nil {
		return nil, fmt.Errorf("error finding immudb service: %v", err)
	}
	if _, err = daemon.Stop(); err != nil {
		return nil, fmt.Errorf("error stopping immudb server: %v", err)
	}
	return func() {
		if _, err = daemon.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "error restarting immudb server: %v", err)
		}
	}, nil
}

func offlineBackup(src string, uncompressed bool, manualStopStart bool) (string, error) {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return "", err
	}
	if !srcInfo.IsDir() {
		return "", fmt.Errorf("%s is not a directory", src)
	}

	if !manualStopStart {
		startImmudbService, err := stopImmudbService()
		if err != nil {
			return "", err
		}
		defer startImmudbService()
	}

	srcBase := filepath.Base(src)
	snapshotPath := srcBase + "_bkp_" + time.Now().Format("2006-01-02_15-04-05")
	if err = fs.CopyDir(src, snapshotPath); err != nil {
		return "", err
	}
	// remove the immudb.identifier file from the backup
	if err = os.Remove(snapshotPath + "/" + server.IDENTIFIER_FNAME); err != nil {
		fmt.Fprintf(os.Stderr,
			"error removing immudb identifier file %s from db snapshot %s: %v",
			server.IDENTIFIER_FNAME, snapshotPath, err)
	}
	if uncompressed {
		absSnapshotPath, err := filepath.Abs(snapshotPath)
		if err != nil {
			fmt.Fprintf(os.Stderr,
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
		archiveErr = fs.TarIt(snapshotPath, archivePath)
	} else {
		archivePath = snapshotPath + ".zip"
		archiveErr = fs.ZipIt(snapshotPath, archivePath, fs.ZipDefaultCompression)
	}
	if archiveErr != nil {
		return "", fmt.Errorf(
			"database copied successfully to %s, but compression to %s failed: %v",
			snapshotPath, archivePath, archiveErr)
	}
	if err = os.RemoveAll(snapshotPath); err != nil {
		fmt.Fprintf(os.Stderr,
			"error removing db snapshot dir %s after successfully compressing it to %s: %v",
			snapshotPath, archivePath, err)
	}

	absArchivePath, err := filepath.Abs(archivePath)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"error converting to absolute path the rel path %s of the archived backup: %v",
			archivePath, err)
		absArchivePath = archivePath
	}

	return absArchivePath, nil
}

func offlineRestore(src string, dst string, manualStopStart bool) (string, error) {
	snapshotPath := src
	_, err := os.Stat(snapshotPath)
	if err != nil {
		return "", err
	}
	snapshotExt := filepath.Ext(snapshotPath)
	snapshotName := filepath.Base(snapshotPath)
	snapshotNameNoExt := strings.TrimSuffix(snapshotName, snapshotExt)
	if strings.ToLower(snapshotExt) == ".gz" {
		snapshotExt = filepath.Ext(snapshotNameNoExt) + snapshotExt
		snapshotNameNoExt = strings.TrimSuffix(snapshotName, snapshotExt)
	}
	dbParentDir := filepath.Dir(dst) + string(os.PathSeparator)
	extractedSnapshotDir := dbParentDir + snapshotNameNoExt
	now := time.Now().Format("2006-01-02_15-04-05")
	var extract func(string, string) error
	switch snapshotExt {
	case ".tar.gz":
		extract = fs.UnTarIt
	case ".zip":
		extract = fs.UnZipIt
	case "": // uncompressed
		// TODO OGG: this will result in the backup being renamed directly to the db folder
		if dbParentDir != filepath.Dir(snapshotPath)+string(os.PathSeparator) {
			extract = fs.CopyDir
		}
	default:
		return "", fmt.Errorf(
			"snapshot %s has unsupported format %s; supported formats: .tar.gz, .zip or none (uncompressed)",
			snapshotPath, snapshotExt)
	}

	if !manualStopStart {
		startImmudbService, err := stopImmudbService()
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
	if err = fs.CopyFile(
		path.Join(dst, server.IDENTIFIER_FNAME),
		path.Join(extractedSnapshotDir, server.IDENTIFIER_FNAME)); err != nil {
		return "", err
	}

	dbDirAutoBackupPath := dst + "_bkp_before_restore_" + now
	if err = os.Rename(dst, dbDirAutoBackupPath); err != nil {
		return "", fmt.Errorf(
			"error renaming previous db dir %s to %s during restore: %v",
			dst, dbDirAutoBackupPath, err)
	}
	if err = os.Rename(extractedSnapshotDir, dst); err != nil {
		return "", fmt.Errorf(
			"error renaming new tmp snapshot dir %s to db dir %s during restore: %v",
			extractedSnapshotDir, dst, err)
	}

	return dbDirAutoBackupPath, nil
}
