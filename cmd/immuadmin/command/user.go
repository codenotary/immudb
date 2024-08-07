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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/cobra"
)

const unsecurePasswordMsg = "A strong password (containing upper and lower case letters, digits and symbols) would be advisable"

func (cl *commandline) user(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "user command",
		Short:             "Issue all user commands",
		Aliases:           []string{"u"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
	}
	userListCmd := &cobra.Command{
		Use:   "list",
		Short: "List all users",

		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.userList(args)
			if err != nil {
				c.QuitToStdErr(err)
			}
			fmt.Fprint(cmd.OutOrStdout(), resp)
			return nil
		},
		Args: cobra.MaximumNArgs(0),
	}
	userCreate := &cobra.Command{
		Use:   "create",
		Short: "Create a new user",
		Long:  "Create a new user inside a database with permissions",
		Example: `immuadmin user create user1 read mydb
immuadmin user create user1 readwrite mydb
immuadmin user create user1 admin mydb`,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.userCreate(cmd, args)
			if err != nil {
				c.QuitToStdErr(err)
			}
			fmt.Fprintln(cmd.OutOrStdout(), resp)
			return nil
		},
		Args: cobra.RangeArgs(2, 3),
	}
	userChangePassword := &cobra.Command{
		Use:     "changepassword",
		Short:   "Change user password",
		Example: "immuadmin user changepassword user1",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			username := args[0]
			var resp string
			var oldpass []byte
			if username == auth.SysAdminUsername {
				oldpass, err = cl.passwordReader.Read("Old password:")
				if err != nil {
					return fmt.Errorf("Error Reading Password")
				}
			}
			if resp, _, err = cl.changeUserPassword(cmd, username, oldpass); err == nil {
				fmt.Fprintln(cmd.OutOrStdout(), resp)
			}
			return err
		},
		Args: cobra.ExactArgs(1),
	}
	userActivate := &cobra.Command{
		Use:   "activate",
		Short: "Activate a user",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			var resp string
			if resp, err = cl.setActiveUser(args, true); err == nil {
				fmt.Fprint(cmd.OutOrStdout(), resp)
			}
			return err
		},
		Args: cobra.ExactArgs(1),
	}
	userDeactivate := &cobra.Command{
		Use:   "deactivate",
		Short: "Deactivate a user",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			var resp string
			if resp, err = cl.setActiveUser(args, false); err == nil {
				fmt.Fprint(cmd.OutOrStdout(), resp)
			}
			return err
		},
		Args: cobra.ExactArgs(1),
	}
	userPermission := &cobra.Command{
		Use:     "permission [grant|revoke] {username} [read|readwrite|admin] {database}",
		Short:   "Set user permission",
		Example: "immuadmin user permission grant user1 readwrite mydb",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if _, err = cl.setUserPermission(args); err == nil {
				fmt.Fprintf(cmd.OutOrStdout(), "Permission changed successfully")
			}
			return err
		},
		Args: cobra.ExactValidArgs(4),
	}
	ccmd.AddCommand(userListCmd)
	ccmd.AddCommand(userCreate)
	ccmd.AddCommand(userChangePassword)
	ccmd.AddCommand(userActivate)
	ccmd.AddCommand(userDeactivate)
	ccmd.AddCommand(userPermission)
	cmd.AddCommand(ccmd)
}

func (cl *commandline) changeUserPassword(cmd *cobra.Command, username string, oldpassword []byte) (string, []byte, error) {
	newpass, err := cl.passwordReader.Read(fmt.Sprintf("Choose a password for %s:", username))
	if err != nil {
		return "", nil, errors.New("Error Reading Password")
	}

	if err := cl.checkPassword(cmd, string(newpass)); err != nil {
		return "", nil, err
	}

	pass2, err := cl.passwordReader.Read("Confirm password:")
	if err != nil {
		return "", nil, errors.New("Error Reading Password")
	}
	if !bytes.Equal(newpass, pass2) {
		return "", nil, errors.New("Passwords don't match")
	}
	if err := cl.immuClient.ChangePassword(cl.context, []byte(username), oldpassword, newpass); err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("%s's password has been changed", username), newpass, nil
}

func (cl *commandline) checkPassword(cmd *cobra.Command, newpass string) error {
	if err := auth.IsStrongPassword(string(newpass)); err == nil {
		return nil
	}

	c.PrintfColorW(cmd.OutOrStdout(), c.Yellow, "%s.\nDo you want to continue with your password instead? [Y/n]\n", unsecurePasswordMsg)

	selected, err := cl.terminalReader.ReadFromTerminalYN("n")
	if err != nil {
		return err
	}

	if selected != "y" {
		return errors.New("unable to change password")
	}
	return nil
}

func (cl *commandline) userList(args []string) (string, error) {
	userlist, err := cl.immuClient.ListUsers(cl.context)
	if err != nil {
		return "", err
	}
	users := userlist.GetUsers()
	usersAndPermissions := make([][]string, 0, len(users))
	maxColWidths := make([]int, 6)
	for _, user := range users {
		row := make([]string, 6)
		permissions := user.GetPermissions()
		row[0] = string(user.GetUser())
		row[1] = fmt.Sprintf("%t", user.GetActive())
		if len(permissions) > 0 {
			row[2] = permissions[0].Database
			row[3] = permissionToString(permissions[0].Permission)
		}
		row[4] = user.Createdby
		row[5] = user.Createdat
		updateMaxLen(maxColWidths, row)
		usersAndPermissions = append(usersAndPermissions, row)
		// extra rows for other dbs and permissions
		if len(permissions) > 1 {
			for i := 1; i < len(permissions); i++ {
				row := make([]string, 6)
				row[2] = permissions[i].Database
				row[3] = permissionToString(permissions[i].Permission)
				usersAndPermissions = append(usersAndPermissions, row)
				updateMaxLen(maxColWidths, row)
			}
		}
	}
	var b bytes.Buffer
	w := bufio.NewWriter(&b)
	c.PrintTable(
		w,
		[]string{
			fmt.Sprintf("% -*s", maxColWidths[0], "User"),
			fmt.Sprintf("% -*s", maxColWidths[1], "Active"),
			fmt.Sprintf("% -*s", maxColWidths[2], "Database"),
			fmt.Sprintf("% -*s", maxColWidths[3], "Permission"),
			fmt.Sprintf("% -*s", maxColWidths[4], "Created By"),
			fmt.Sprintf("% -*s", maxColWidths[5], "Created At"),
		},
		len(usersAndPermissions),
		func(i int) []string { return usersAndPermissions[i] },
		fmt.Sprintf("%d user(s)", len(users)),
	)
	w.Flush()
	return b.String(), nil
}

func updateMaxLen(maxs []int, strs []string) {
	for i, str := range strs {
		if len(str) > maxs[i] {
			maxs[i] = len(str)
		}
	}
}

func permissionToString(permission uint32) string {
	switch permission {
	case auth.PermissionAdmin:
		return "Admin"
	case auth.PermissionSysAdmin:
		return "System Admin"
	case auth.PermissionR:
		return "Read"
	case auth.PermissionRW:
		return "Read/Write"
	default:
		return fmt.Sprintf("unknown: %d", permission)
	}
}

func (cl *commandline) userCreate(cmd *cobra.Command, args []string) (string, error) {
	username := args[0]
	permissionStr := args[1]
	var databasename string
	if len(args) == 3 {
		databasename = args[2]
	}

	// validations
	usernameTaken, err := userExists(cl.context, cl.immuClient, username)
	if err != nil {
		return "", err
	}
	if usernameTaken {
		return "", fmt.Errorf("User %s already exists", username)
	}
	if databasename != "" {
		existingDb, err := dbExists(cl.context, cl.immuClient, databasename)
		if err != nil {
			return "", err
		}
		if !existingDb {
			return "", fmt.Errorf("Database %s does not exist", databasename)
		}
	}
	permission, err := permissionFromString(permissionStr)
	if err != nil {
		return "", err
	}

	pass, err := cl.passwordReader.Read(fmt.Sprintf("Choose a password for %s:", username))
	if err != nil {
		return "", fmt.Errorf("Error Reading Password")
	}

	if err := cl.checkPassword(cmd, string(pass)); err != nil {
		return "", err
	}
	pass2, err := cl.passwordReader.Read("Confirm password:")
	if err != nil {
		return "", fmt.Errorf("Error Reading Password")
	}
	if !bytes.Equal(pass, pass2) {
		return "", fmt.Errorf("Passwords don't match")
	}

	err = cl.immuClient.CreateUser(cl.context, []byte(username), pass, permission, databasename)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Created user %s", username), nil
}

func (cl *commandline) setActiveUser(args []string, active bool) (string, error) {
	username := args[0]
	err := cl.immuClient.SetActiveUser(cl.context, &schema.SetActiveUserRequest{
		Active:   active,
		Username: username,
	})
	if err != nil {
		return "", err
	}
	return "User status changed successfully", nil
}

func (cl *commandline) setUserPermission(args []string) (resp string, err error) {
	var permissionAction schema.PermissionAction
	switch args[0] {
	case "grant":
		permissionAction = schema.PermissionAction_GRANT
	case "revoke":
		permissionAction = schema.PermissionAction_REVOKE
	default:
		return "", fmt.Errorf("wrong permission action. Only grant or revoke are allowed. Provided: %s", args[0])
	}
	username := args[1]
	permission, err := permissionFromString(args[2])
	if err != nil {
		return "", err
	}
	dbname := args[3]
	return "", cl.immuClient.ChangePermission(cl.context, permissionAction, username, dbname, permission)
}

func userExists(
	ctx context.Context,
	immuClient client.ImmuClient,
	username string,
) (bool, error) {
	existingUsers, err := immuClient.ListUsers(ctx)
	if err != nil {
		return false, err
	}
	for _, eu := range existingUsers.GetUsers() {
		if string(eu.GetUser()) == username {
			return true, nil
		}
	}
	return false, nil
}

func dbExists(
	ctx context.Context,
	immuClient client.ImmuClient,
	dbName string,
) (bool, error) {
	existingDBs, err := immuClient.DatabaseList(ctx)
	if err != nil {
		return false, err
	}
	for _, db := range existingDBs.GetDatabases() {
		if db.GetDatabaseName() == dbName {
			return true, nil
		}
	}
	return false, nil
}

func permissionFromString(permissionStr string) (uint32, error) {
	var permission uint32
	switch permissionStr {
	case "read":
		permission = auth.PermissionR
	case "admin":
		permission = auth.PermissionAdmin
	case "readwrite":
		permission = auth.PermissionRW
	default:
		return 0, fmt.Errorf(
			"Permission %s not recognized: allowed permissions are read, readwrite, admin",
			permissionStr)
	}
	return permission, nil
}
