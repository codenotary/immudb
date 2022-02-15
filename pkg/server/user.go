package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/codenotary/immudb/pkg/server/sessions"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/errors"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Login ...
func (s *ImmuServer) Login(ctx context.Context, r *schema.LoginRequest) (*schema.LoginResponse, error) {
	if !s.Options.auth {
		return nil, errors.New(ErrAuthDisabled).WithCode(errors.CodProtocolViolation)
	}

	u, err := s.getValidatedUser(r.User, r.Password)
	if err != nil {
		return nil, errors.Wrap(err, ErrInvalidUsernameOrPassword)
	}

	if !u.Active {
		return nil, errors.New(ErrUserNotActive)
	}

	var token string

	if s.multidbmode {
		//-1 no database yet, must exec the "use" (UseDatabase) command first
		token, err = auth.GenerateToken(*u, -1, s.Options.TokenExpiryTimeMin)
	} else {
		token, err = auth.GenerateToken(*u, defaultDbIndex, s.Options.TokenExpiryTimeMin)
	}
	if err != nil {
		return nil, err
	}

	loginResponse := &schema.LoginResponse{Token: token}
	if u.Username == auth.SysAdminUsername && string(r.GetPassword()) == auth.SysAdminPassword {
		loginResponse.Warning = []byte(auth.WarnDefaultAdminPassword)
	}

	if u.Username == auth.SysAdminUsername {
		u.IsSysAdmin = true
	}

	//add user to loggedin list
	s.addUserToLoginList(u)

	return loginResponse, nil
}

// Logout ...
func (s *ImmuServer) Logout(ctx context.Context, r *empty.Empty) (*empty.Empty, error) {
	if !s.Options.auth {
		return nil, errors.New(ErrAuthDisabled).WithCode(errors.CodProtocolViolation)
	}

	_, user, err := s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	// remove user from loggedin users
	s.removeUserFromLoginList(user.Username)

	// invalidate the token for this user
	_, err = auth.DropTokenKeysForCtx(ctx)

	return new(empty.Empty), err
}

// CreateUser Creates a new user
func (s *ImmuServer) CreateUser(ctx context.Context, r *schema.CreateUserRequest) (*empty.Empty, error) {
	s.Logger.Debugf("CreateUser")

	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	loggedInuser := &auth.User{}
	var err error

	if !s.Options.GetAuth() {
		return nil, fmt.Errorf("this command is available only with authentication on")
	}

	_, loggedInuser, err = s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	if len(r.User) == 0 {
		return nil, fmt.Errorf("username can not be empty")
	}

	if (len(r.Database) == 0) && s.multidbmode {
		return nil, fmt.Errorf("database name can not be empty when there are multiple databases")
	}

	if (len(r.Database) == 0) && !s.multidbmode {
		r.Database = DefaultDBName
	}

	//check if database exists
	if s.dbList.GetId(r.Database) < 0 {
		return nil, fmt.Errorf("database %s does not exist", r.Database)
	}

	//check permission is a known value
	if (r.Permission == auth.PermissionNone) ||
		(r.Permission > auth.PermissionRW && r.Permission < auth.PermissionAdmin) {
		return nil, fmt.Errorf("unrecognized permission")
	}

	//if the requesting user has admin permission on this database
	if (!loggedInuser.IsSysAdmin) &&
		(!loggedInuser.HasPermission(r.Database, auth.PermissionAdmin)) {
		return nil, fmt.Errorf("you do not have permission on this database")
	}

	//do not allow to create another system admin
	if r.Permission == auth.PermissionSysAdmin {
		return nil, fmt.Errorf("can not create another system admin")
	}

	_, err = s.getUser(r.User, true)
	if err == nil {
		return nil, fmt.Errorf("user already exists")
	}

	_, _, err = s.insertNewUser(r.User, r.Password, r.GetPermission(), r.Database, true, loggedInuser.Username)
	if err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

// ListUsers returns a list of users based on the requesting user permissions
func (s *ImmuServer) ListUsers(ctx context.Context, req *empty.Empty) (*schema.UserList, error) {
	s.Logger.Debugf("ListUsers")

	loggedInuser := &auth.User{}
	var dbInd = int64(0)
	var err error
	userlist := &schema.UserList{}

	if !s.Options.GetMaintenance() {
		if !s.Options.GetAuth() {
			return nil, fmt.Errorf("this command is available only with authentication on")
		}

		dbInd, loggedInuser, err = s.getLoggedInUserdataFromCtx(ctx)
		if err != nil {
			return nil, err
		}
	}

	itemList, err := s.sysDB.Scan(&schema.ScanRequest{
		Prefix: []byte{KeyPrefixUser},
		NoWait: true,
	})
	if err != nil {
		s.Logger.Errorf("error getting users: %v", err)
		return nil, err
	}

	if loggedInuser.IsSysAdmin || s.Options.GetMaintenance() {
		// return all users, including the deactivated ones
		for i := 0; i < len(itemList.Entries); i++ {
			itemList.Entries[i].Key = itemList.Entries[i].Key[1:]

			var user auth.User

			err = json.Unmarshal(itemList.Entries[i].Value, &user)
			if err != nil {
				return nil, err
			}

			permissions := []*schema.Permission{}

			for _, val := range user.Permissions {
				permissions = append(permissions, &schema.Permission{
					Database:   val.Database,
					Permission: val.Permission,
				})
			}

			u := schema.User{
				User:        []byte(user.Username),
				Createdat:   user.CreatedAt.String(),
				Createdby:   user.CreatedBy,
				Permissions: permissions,
				Active:      user.Active,
			}

			userlist.Users = append(userlist.Users, &u)
		}

		return userlist, nil

	} else if loggedInuser.WhichPermission(s.dbList.GetByIndex(dbInd).GetName()) == auth.PermissionAdmin {
		// for admin users return only users for the database that is has selected
		selectedDbname := s.dbList.GetByIndex(dbInd).GetName()
		userlist := &schema.UserList{}

		for i := 0; i < len(itemList.Entries); i++ {
			include := false
			itemList.Entries[i].Key = itemList.Entries[i].Key[1:]

			var user auth.User

			err = json.Unmarshal(itemList.Entries[i].Value, &user)
			if err != nil {
				return nil, err
			}

			permissions := []*schema.Permission{}

			for _, val := range user.Permissions {
				//check if this user has any permission for this database
				//include in the reply only if it has any permission for the currently selected database
				if val.Database == selectedDbname {
					include = true
				}

				permissions = append(permissions, &schema.Permission{
					Database:   val.Database,
					Permission: val.Permission,
				})
			}

			if include {
				u := schema.User{
					User:        []byte(user.Username),
					Createdat:   user.CreatedAt.String(),
					Createdby:   user.CreatedBy,
					Permissions: permissions,
					Active:      user.Active,
				}

				userlist.Users = append(userlist.Users, &u)
			}
		}

		return userlist, nil

	} else {
		// any other permission return only its data
		userlist := &schema.UserList{}
		permissions := []*schema.Permission{}

		for _, val := range loggedInuser.Permissions {
			permissions = append(permissions, &schema.Permission{
				Database:   val.Database,
				Permission: val.Permission,
			})
		}

		u := schema.User{
			User:        []byte(loggedInuser.Username),
			Createdat:   loggedInuser.CreatedAt.String(),
			Createdby:   loggedInuser.CreatedBy,
			Permissions: permissions,
			Active:      loggedInuser.Active,
		}

		userlist.Users = append(userlist.Users, &u)

		return userlist, nil
	}
}

// ChangePassword ...
func (s *ImmuServer) ChangePassword(ctx context.Context, r *schema.ChangePasswordRequest) (*empty.Empty, error) {
	s.Logger.Debugf("ChangePassword")

	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	if !s.Options.GetAuth() {
		return nil, fmt.Errorf("this command is available only with authentication on")
	}

	_, user, err := s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	if string(r.User) == auth.SysAdminUsername {
		if err = auth.ComparePasswords(user.HashedPassword, r.OldPassword); err != nil {
			return new(empty.Empty), status.Errorf(codes.PermissionDenied, "old password is incorrect")
		}
	}

	if !user.IsSysAdmin {
		if !user.HasAtLeastOnePermission(auth.PermissionAdmin) {
			return nil, fmt.Errorf("user is not system admin nor admin in any of the databases")
		}
	}

	if len(r.User) == 0 {
		return nil, fmt.Errorf("username can not be empty")
	}

	targetUser, err := s.getUser(r.User, true)
	if err != nil {
		return nil, fmt.Errorf("user %s was not found or it was not created by you", string(r.User))
	}

	//if the user is not sys admin then let's make sure the target was created from this admin
	if !user.IsSysAdmin {
		if user.Username != targetUser.CreatedBy {
			return nil, fmt.Errorf("user %s was not found or it was not created by you", string(r.User))
		}
	}

	_, err = targetUser.SetPassword(r.NewPassword)
	if err != nil {
		return nil, err
	}

	targetUser.CreatedBy = user.Username
	targetUser.CreatedAt = time.Now()
	if err := s.saveUser(targetUser); err != nil {
		return nil, err
	}

	//remove user from loggedin users
	s.removeUserFromLoginList(targetUser.Username)
	// invalidate the token for this user
	auth.DropTokenKeys(targetUser.Username)

	return new(empty.Empty), nil
}

//ChangePermission grant or revoke user permissions on databases
func (s *ImmuServer) ChangePermission(ctx context.Context, r *schema.ChangePermissionRequest) (*empty.Empty, error) {
	s.Logger.Debugf("ChangePermission %+v", r)

	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	//sanitize input
	{
		if len(r.Username) == 0 {
			return nil, status.Errorf(codes.InvalidArgument, "username can not be empty")
		}
		if len(r.Database) == 0 {
			return nil, status.Errorf(codes.InvalidArgument, "database can not be empty")
		}
		if (r.Action != schema.PermissionAction_GRANT) &&
			(r.Action != schema.PermissionAction_REVOKE) {
			return nil, status.Errorf(codes.InvalidArgument, "action not recognized")
		}
		if (r.Permission == auth.PermissionNone) ||
			((r.Permission > auth.PermissionRW) &&
				(r.Permission < auth.PermissionAdmin)) {
			return nil, status.Errorf(codes.InvalidArgument, "unrecognized permission")
		}
	}

	_, user, err := s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	//do not allow to change own permissions, user can lock itsself out
	if r.Username == user.Username {
		return nil, status.Errorf(codes.InvalidArgument, "changing your own permissions is not allowed")
	}

	if r.Username == auth.SysAdminUsername {
		return nil, status.Errorf(codes.InvalidArgument, "changing sysadmin permisions is not allowed")
	}

	if r.Database == SystemDBName && r.Permission == auth.PermissionRW {
		return nil, ErrPermissionDenied
	}

	//check if user exists
	targetUser, err := s.getUser([]byte(r.Username), true)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "user %s not found", string(r.Username))
	}

	//target user should be active
	if !targetUser.Active {
		return nil, status.Errorf(codes.FailedPrecondition, "user %s is not active", string(r.Username))
	}

	//check if requesting user has permission on this database
	if !user.IsSysAdmin {
		if !user.HasPermission(r.Database, auth.PermissionAdmin) {
			return nil, status.Errorf(codes.PermissionDenied, "you do not have permission on this database")
		}
	}

	if r.Action == schema.PermissionAction_REVOKE {
		targetUser.RevokePermission(r.Database)
	} else {
		targetUser.GrantPermission(r.Database, r.Permission)
	}

	targetUser.CreatedBy = user.Username
	targetUser.CreatedAt = time.Now()

	if err := s.saveUser(targetUser); err != nil {
		return nil, err
	}

	//remove user from loggedin users
	s.removeUserFromLoginList(targetUser.Username)

	return new(empty.Empty), nil
}

//SetActiveUser activate or deactivate a user
func (s *ImmuServer) SetActiveUser(ctx context.Context, r *schema.SetActiveUserRequest) (*empty.Empty, error) {
	s.Logger.Debugf("SetActiveUser")

	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	if !s.Options.GetAuth() {
		return nil, fmt.Errorf("this command is available only with authentication on")
	}

	if len(r.Username) == 0 {
		return nil, fmt.Errorf("username can not be empty")
	}

	user := &auth.User{}
	var err error

	_, user, err = s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	if !user.IsSysAdmin {
		if !user.HasAtLeastOnePermission(auth.PermissionAdmin) {
			return nil, fmt.Errorf("user is not system admin nor admin in any of the databases")
		}
	}

	if r.Username == user.Username {
		return nil, fmt.Errorf("changing your own status is not allowed")
	}

	targetUser, err := s.getUser([]byte(r.Username), true)
	if err != nil {
		return nil, fmt.Errorf("user %s not found", r.Username)
	}

	//if the user is not sys admin then let's make sure the target was created from this admin
	if !user.IsSysAdmin && user.Username != targetUser.CreatedBy {
		return nil, fmt.Errorf("%s was not created by you", r.Username)
	}

	targetUser.Active = r.Active
	targetUser.CreatedBy = user.Username
	targetUser.CreatedAt = time.Now()

	if err := s.saveUser(targetUser); err != nil {
		return nil, err
	}

	//remove user from loggedin users
	s.removeUserFromLoginList(targetUser.Username)

	return new(empty.Empty), nil
}

// insertNewUser inserts a new user to the system database and returns username and plain text password
// A new password is generated automatically if passed parameter is empty
// If enforceStrongAuth is true it checks if username and password meet security criteria
func (s *ImmuServer) insertNewUser(username []byte, plainPassword []byte, permission uint32, database string, enforceStrongAuth bool, createdBy string) ([]byte, []byte, error) {
	if enforceStrongAuth {
		if !auth.IsValidUsername(string(username)) {
			return nil, nil, status.Errorf(
				codes.InvalidArgument,
				"username can only contain letters, digits and underscores")
		}
	}

	if enforceStrongAuth {
		if err := auth.IsStrongPassword(string(plainPassword)); err != nil {
			return nil, nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
	}

	userdata := new(auth.User)
	plainpassword, err := userdata.SetPassword(plainPassword)
	if err != nil {
		return nil, nil, err
	}

	userdata.Active = true
	userdata.Username = string(username)
	userdata.Permissions = append(userdata.Permissions, auth.Permission{Permission: permission, Database: database})
	userdata.CreatedBy = createdBy
	userdata.CreatedAt = time.Now()

	if permission == auth.PermissionSysAdmin {
		userdata.IsSysAdmin = true
	}

	if (permission > auth.PermissionRW) && (permission < auth.PermissionAdmin) {
		return nil, nil, fmt.Errorf("unknown permission")
	}

	err = s.saveUser(userdata)

	return username, plainpassword, err
}

func (s *ImmuServer) getValidatedUser(username []byte, password []byte) (*auth.User, error) {
	userdata, err := s.getUser(username, true)
	if err != nil {
		return nil, err
	}

	err = userdata.ComparePasswords(password)
	if err != nil {
		return nil, err
	}

	return userdata, nil
}

// getUser returns userdata (username,hashed password, permission, active) from username
func (s *ImmuServer) getUser(username []byte, includeDeactivated bool) (*auth.User, error) {
	key := make([]byte, 1+len(username))
	key[0] = KeyPrefixUser
	copy(key[1:], username)

	item, err := s.sysDB.Get(&schema.KeyRequest{Key: key})
	if err != nil {
		return nil, err
	}

	var usr auth.User

	err = json.Unmarshal(item.Value, &usr)
	if err != nil {
		return nil, err
	}

	if !includeDeactivated {
		if usr.Active {
			return nil, fmt.Errorf("user not found")
		}
	}

	return &usr, nil
}

func (s *ImmuServer) saveUser(user *auth.User) error {
	userData, err := json.Marshal(user)
	if err != nil {
		return logErr(s.Logger, "error saving user: %v", err)
	}

	userKey := make([]byte, 1+len(user.Username))
	userKey[0] = KeyPrefixUser
	copy(userKey[1:], []byte(user.Username))

	userKV := &schema.KeyValue{Key: userKey, Value: userData}
	_, err = s.sysDB.Set(&schema.SetRequest{KVs: []*schema.KeyValue{userKV}})

	time.Sleep(time.Duration(10) * time.Millisecond)

	return logErr(s.Logger, "error saving user: %v", err)
}

func (s *ImmuServer) removeUserFromLoginList(username string) {
	s.userdata.Lock()
	defer s.userdata.Unlock()

	delete(s.userdata.Userdata, username)
}

func (s *ImmuServer) addUserToLoginList(u *auth.User) {
	s.userdata.Lock()
	defer s.userdata.Unlock()

	s.userdata.Userdata[u.Username] = u
}

func (s *ImmuServer) getLoggedInUserdataFromCtx(ctx context.Context) (int64, *auth.User, error) {
	if sessionID, err := sessions.GetSessionIDFromContext(ctx); err == nil {
		sess, e := s.SessManager.GetSession(sessionID)
		if e != nil {
			return 0, nil, e
		}
		return s.dbList.GetId(sess.GetDatabase().GetName()), sess.GetUser(), nil
	}
	jsUser, err := auth.GetLoggedInUser(ctx)
	if err != nil {
		return -1, nil, err
	}

	u, err := s.getLoggedInUserDataFromUsername(jsUser.Username)
	return jsUser.DatabaseIndex, u, err
}

func (s *ImmuServer) getLoggedInUserDataFromUsername(username string) (*auth.User, error) {
	s.userdata.Lock()
	defer s.userdata.Unlock()

	userdata, ok := s.userdata.Userdata[username]
	if !ok {
		return nil, ErrNotLoggedIn
	}

	return userdata, nil
}
