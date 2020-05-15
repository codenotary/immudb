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
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
				c.QuitWithUserError(err)
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
		Use:   "backup [--dbdir] [--manual-stop-start] [--online] [--uncompressed]",
		Short: "Make a copy of the database files and folders",
		Long: "Pause the immudb server, create and save on the server machine a snapshot " +
			"of the database files and folders (zip on Windows, tar.gz on Linux or uncompressed).",
		RunE: func(cmd *cobra.Command, args []string) error {
			dbDir, err := cmd.Flags().GetString("dbdir")
			if err != nil {
				c.QuitToStdErr(err)
			}
			manualStopStart, err := cmd.Flags().GetBool("manual-stop-start")
			if err != nil {
				c.QuitToStdErr(err)
			}
			online, err := cmd.Flags().GetBool("online")
			if err != nil {
				c.QuitToStdErr(err)
			}
			uncompressed, err := cmd.Flags().GetBool("uncompressed")
			if err != nil {
				c.QuitToStdErr(err)
			}
			if online {
				if dbDir != defaultDbDir {
					fmt.Println("WARNING: --online flag specified, --dbdir flag value will be ignored")
				}
				if manualStopStart {
					fmt.Println("WARNING: --online flag specified, --manual-stop-start flag value will be ignored")
				}
			}

			ctx := cl.context

			if !online {
				if !manualStopStart {
					var answer string
					fmt.Print("Server will be stopped and then restarted during the backup process. Are you sure you want to proceed? [y/N]: ")
					if _, err = fmt.Scanln(&answer); err != nil ||
						!(strings.ToUpper("Y") == strings.TrimSpace(strings.ToUpper(answer))) {
						c.QuitToStdErr("Canceled")
					}
					pass, err := cl.passwordReader.Read(fmt.Sprintf("Enter %s's password:", auth.AdminUsername))
					if err != nil {
						c.QuitToStdErr(err)
					}
					cl.checkLoggedInAndConnect(nil, nil)
					defer cl.disconnect(nil, nil)
					if _, err = cl.immuClient.Login(ctx, []byte(auth.AdminUsername), pass); err != nil {
						c.QuitWithUserError(err)
					}
				}
				backupPath, err := backup(dbDir, uncompressed, manualStopStart)
				if err != nil {
					c.QuitToStdErr(err)
				}
				fmt.Printf("Database backup created: %s\n", backupPath)
				return nil
			}

			cl.checkLoggedInAndConnect(nil, nil)
			defer cl.disconnect(nil, nil)
			response, err := cl.immuClient.Backup(ctx, uncompressed)
			if err != nil {
				c.QuitWithUserError(err)
			}
			fmt.Printf("Database backup created: %s\n", response.GetMessage())
			return nil
		},
		Args: cobra.NoArgs,
	}
	ccmd.Flags().String("dbdir", defaultDbDir, fmt.Sprintf("path to the server database directory to backup (default %s)", defaultDbDir))
	ccmd.Flags().Bool("online", false, "only send a signal to the server which will then handle the complete backup process (default false)")
	ccmd.Flags().Bool("manual-stop-start", false, "server stop before and restart after the backup are to be handled manually by the user (default false)")
	ccmd.Flags().BoolP("uncompressed", "u", false, "create an uncompressed backup (i.e. make just a copy of the db directory)")
	cmd.AddCommand(ccmd)
}

func (cl *commandline) restore(cmd *cobra.Command) {
	defaultDbDir := server.DefaultOptions().Dir
	ccmd := &cobra.Command{
		Use:   "restore snapshot-path [--dbdir] [--manual-stop-start] [--online]",
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
			online, err := cmd.Flags().GetBool("online")
			if err != nil {
				c.QuitToStdErr(err)
			}
			if online {
				if dbDir != defaultDbDir {
					fmt.Println("WARNING: --online flag specified, --dbdir flag value will be ignored")
				}
				if manualStopStart {
					fmt.Println("WARNING: --online flag specified, --manual-stop-start flag value will be ignored")
				}
			}

			ctx := cl.context

			if !online {
				if !manualStopStart {
					var answer string
					fmt.Print("Server will be stopped and then restarted during the restore process. Are you sure you want to proceed? [y/N]: ")
					if _, err := fmt.Scanln(&answer); err != nil ||
						!(strings.ToUpper("Y") == strings.TrimSpace(strings.ToUpper(answer))) {
						c.QuitToStdErr("Canceled")
					}
					pass, err := cl.passwordReader.Read(fmt.Sprintf("Enter %s's password:", auth.AdminUsername))
					if err != nil {
						c.QuitToStdErr(err)
					}
					cl.checkLoggedInAndConnect(nil, nil)
					defer cl.disconnect(nil, nil)
					if _, err = cl.immuClient.Login(ctx, []byte(auth.AdminUsername), pass); err != nil {
						c.QuitWithUserError(err)
					}
				}
				if err := restore(snapshotPath, dbDir, manualStopStart); err != nil {
					c.QuitToStdErr(err)
				}
				fmt.Printf("Dabase restored from backup %s\n", snapshotPath)
				return nil
			}

			cl.checkLoggedInAndConnect(nil, nil)
			defer cl.disconnect(nil, nil)
			if err := cl.immuClient.Restore(ctx, []byte(snapshotPath)); err != nil {
				c.QuitWithUserError(err)
			}
			fmt.Printf("Database restored from backup %s\n", snapshotPath)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	ccmd.Flags().String("dbdir", defaultDbDir, fmt.Sprintf("path to the server database directory which will be replaced by the backup (default %s)", defaultDbDir))
	ccmd.Flags().Bool("manual-stop-start", false, "server stop before and restart after the backup are to be handled manually by the user (default false)")
	ccmd.Flags().Bool("online", false, "only send a signal to the server which will then handle the complete restore process (default false)")
	cmd.AddCommand(ccmd)
}

func backup(src string, uncompressed bool, manualStopStart bool) (string, error) {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return "", err
	}
	if !srcInfo.IsDir() {
		return "", fmt.Errorf("%s is not a directory", src)
	}

	if !manualStopStart {
		daemon, err := service.NewDaemon("immudb", "", "")
		if err != nil {
			return "", err
		}
		if _, err = daemon.Stop(); err != nil {
			return "", err
		}
		defer func() {
			if _, err = daemon.Start(); err != nil {
				fmt.Fprintf(os.Stderr, "error restarting immudb server: %v", err)
			}
		}()
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
		return "", status.Errorf(
			codes.Internal,
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

func restore(src string, dst string, manualStopStart bool) error {
	snapshotPath := src
	_, err := os.Stat(snapshotPath)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
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
		return status.Errorf(
			codes.InvalidArgument,
			"snapshot %s has unsupported format %s; supported formats: .tar.gz, .zip or none (uncompressed)",
			snapshotPath, snapshotExt)
	}

	if !manualStopStart {
		daemon, err := service.NewDaemon("immudb", "", "")
		if err != nil {
			return err
		}
		if _, err = daemon.Stop(); err != nil {
			return err
		}
		defer func() {
			if _, err = daemon.Start(); err != nil {
				fmt.Fprintf(os.Stderr, "error restarting immudb server: %v", err)
			}
		}()
	}

	if extract != nil {
		if err = extract(snapshotPath, dbParentDir); err != nil {
			return status.Errorf(codes.Internal, "%v", err)
		}
	}
	// keep the same db identifier
	if err = fs.CopyFile(
		path.Join(dst, server.IDENTIFIER_FNAME),
		path.Join(extractedSnapshotDir, server.IDENTIFIER_FNAME)); err != nil {
		return status.Errorf(codes.Internal, "%v", err)
	}

	dbDirAutoBackupPath := dst + "_bkp_before_restore_" + now
	if err = os.Rename(dst, dbDirAutoBackupPath); err != nil {
		return status.Errorf(
			codes.Internal,
			"error renaming previous db dir %s to %s during restore: %v",
			dst, dbDirAutoBackupPath, err)
	}
	if err = os.Rename(extractedSnapshotDir, dst); err != nil {
		return status.Errorf(
			codes.Internal,
			"error renaming new tmp snapshot dir %s to db dir %s during restore: %v",
			extractedSnapshotDir, dst, err)
	}

	return nil
}
