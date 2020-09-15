# CHANGELOG
All notable changes to this project will be documented in this file. This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
<a name="unreleased"></a>
## [Unreleased]


<a name="v0.8.0"></a>
## [v0.8.0] - 2020-09-15
### Bug Fixes
- fix immudb and immugw version and mangen commands errors Without this change, while immuclient and immuadmin still worked as expected, immudb and immugw version and mangen commands were throwing the following error: ./immugw version Error: flag accessed but not defined: config Usage:   immugw version [flags]
- fix immuclient audit-mode
- **cmd/immuadmin/command:** fix immuadmin dbswitch
- **pkg/client:** setBatch creates structured values
- **pkg/client:** token service manages old token format

### Changes
- update README file ([#487](https://github.com/vchain-us/immudb/issues/487))
- switching README.md end lines to LF
- fix immugw dependency to support new root structure
- update readme, add immudb4j news ([#488](https://github.com/vchain-us/immudb/issues/488))
- **cmd:** add signingKey flag
- **cmd:** remove error suppression in config loader
- **cmd/immutest/command:** remove immugw dependency from immutest
- **pkg:** add kvlist validator ([#498](https://github.com/vchain-us/immudb/issues/498))
- **pkg/server:** log uuid set and get error
- **pkg/server:** log signer initialization in immudb start

### Code Refactoring
- wrap root hash and index in a new structure to support signature
- move immugw in a separate repository
- configs file are loaded in viper preRun method
- **pkg/server:** inject root signer service inside immudb server

### Features
- auditor verifies root signature
- **cmd:** process launcher check if are present another istances. fixes [#168](https://github.com/vchain-us/immudb/issues/168)
- **pkg:** add root signer service
- **pkg/signer:** add ecdsa signer


<a name="v0.7.0"></a>
## [v0.7.0] - 2020-08-10
### Bug Fixes
- userlist returns wrong message when logged in as immudb with single database
- use dedicated logger for store
- fix compilation error in corruption checker test
- race condition in token eviction
- chose defaultdb on user create if not in multiple db mode
- user list showing only the superadmin user even when other user exist
- fix multiple services config uninstall
- skip loading databases from disk when in memory is requested
- if custom port is <= 0 use default port for both immudb and immugw
- fix immugw failing to start with nil pointer dereference since gRPC dial options are inherited (bug was introduced in commit a4477e2e403ab35fc9392e0a3a2d8436a5806901)
- remove os.Exit(0) from disconnect method
- fix DefaultPasswordReader initialization. fixes [#404](https://github.com/vchain-us/immudb/issues/404)
- fix travis build sleep time
- use the correct server logger and use a dedicated logger with warning level for the db store
- **cmd/immuadmin/command:** fix user list output to support multiple databases (with permissions) for the same user
- **pkg/auth:** if new auth token is found in outgoing context it replaced the old one
- **pkg/client:** use database set internally database name
- **pkg/client:** inherit dial options that came from constructor
- **pkg/fs:** don't overwrite copy error on Close malfunction. Sync seals the operation–not Close.
- **pkg/gw:** fix client option construction with missing homedirservice
- **pkg/server:** added os file separator and db root path
- **pkg/server:** avoid recursion on never ending functionality. Further improvements can be done ([#427](https://github.com/vchain-us/immudb/issues/427))
- **pkg/server/server:** change user pass , old password check
- **pkg/service:** restore correct config path
- **pkg/store:** fix count method using a proper NewKeyIterator

### Changes
- refactor immuclient test
- fix tokenService typos
- add use database gw handler
- spread token service usage
- enhance immudb server messages during start
- capitalize immudb stop log message for consistency reasons
- remove permission leftovers and useless messages in client server protocol
- log immudb user messages during start to file if a logfile is specified
- use debug instead of info for some log messages that are not relevant to the user
- versioning token filename
- add auditor single run mode
- fix conflicts while rebasing from master
- remove user commands from immuclient
- add unit tests for zip and tar
- fix test
- improve command ux and fix changepassword test. Closes [#370](https://github.com/vchain-us/immudb/issues/370)
- change insert user to use safeset instead of set
- remove useless quitToStdError and os.exit calls
- remove sleep from tests
- use 0.0.0.0 instead of 127.0.0.1 as default address for both immudb and immugw
- using cobra command std out
- move immuadmin and immuclient service managing to pkg
- add homedir service
- rewrite tests in order to use pkg/server/servertest
- add codecov windows and freebsd ignore paths
- fix typo in UninstallManPages function name
- add coveralls.io stage
- refactor immuadmin service to use immuos abstraction
- add coverall badge
- add filepath abstration, use it in immuadmin backup and enhance coverage for backup test
- add os and filepath abstraction and use it in immuadmin backup command
- fix codecov ignore paths
- remove os wrapper from codecov.yml
- fix go test cover coverall
- fix immuclient tests
- add empty clientTest constructor
- user list client return a printable string
- add unexpectedNotStructuredValue error. fixes [#402](https://github.com/vchain-us/immudb/issues/402)
- add failfast option in test command
- fix contributing.md styling
- remove tests from windows CI
- add go-acc to calculate code coverage and fix go version to 1.13
- refactor immuclient test, place duplicated code in one place
- add an explicit data source on terminal reader
- TestHealthCheckFails if grpc is no fully closed
- add options to tuning corruption checking frequency, iteration and single iteration
- **cmd:** immugw and immudb use process launcher for detach mode
- **cmd:** token is managed as a string. fixes [#453](https://github.com/vchain-us/immudb/issues/453)
- **cmd:** fix typo in command messages
- **cmd:** enhance PrintTable function to support custom table captions and use such captions in immuadmin user and database list commands
- **cmd:** restore error handling in main method
- **cmd/helper:** add doc comment for the PrintTable function
- **cmd/immuadmin:** immuadmin user sub-commands use cobra, tests
- **cmd/immuadmin/command:** remove useless auth check in print tree command
- **cmd/immuadmin/command:** fix text alignment and case
- **cmd/immuadmin/command:** move command line and his command helper method in a single file
- **cmd/immuadmin/command:** automatically login the immuadmin user after forced password change is completed
- **cmd/immuadmin/command:** remove silent errors in immuadmin
- **cmd/immuadmin/command:** move options as dependency of commandline struct
- **cmd/immuadmin/command:** user and database list use table printer
- **cmd/immuclient/command:** remove useless comment
- **cmd/immuclient/immuc:** inject homedir service as dependency
- **cmd/immugw/command:** use general viper.BindPFlags binding instead of a verbose bindFlags solution
- **cmd/immutest/command:** inject homedir service as dependency
- **pkg/client/options:** add options fields and test
- **pkg/client/timestamp:** removed unused ntp timestamp
- **pkg/fs:** utilise filepath directory walk for copy
- **pkg/fs:** traceable copy errors
- **pkg/fs:** create file copy with flags from the start, in write-only mode
- **pkg/server:** add corruption checker random indexes generator  missing dependency
- **pkg/server:** improve tests
- **pkg/server:** mtls test certificates system db as immuserver property improve tests
- **pkg/server:** make DevMode default false and cleanup call to action message shwon right after immudb start
- **pkg/server:** immudb struct implements immudbIf interface, fixes previous tests
- **pkg/server:** add corruption checker random indexes generator dependency
- **pkg/store/sysstore:** remove useless method

### Code Refactoring
- add immuadmin services interfaces and terminal helper
- remove custom errors inside useDatabase and createDatabase services. Fixes [#367](https://github.com/vchain-us/immudb/issues/367)
- handle in idiomatic way errors in changePermission grpc service. Fixes [#368](https://github.com/vchain-us/immudb/issues/368)
- decoupled client options from server gateway constructor
- refactor detach() method in a process launcher service
- decouple manpage methods in a dedicated service
- **cmd:** move database management commands from immuclient to immuadmin. Fixes [#440](https://github.com/vchain-us/immudb/issues/440)
- **cmd/immuadmin/command:** using c.PrintfColorW instead c.PrintfColor to increase cobra.cmd integration for tests
- **cmd/immuadmin/command:** move checkLoggedInAndConnect, connect, disconnect from server to login file
- **cmd/immuadmin/command:** remove useless argument in Init and improve naming conventions

### Features
- add multiple databases support
- **cmd/helper:** add table printer
- **cmd/helper:** add PrintfColorW to decouple writer capabilities
- **cmd/immutest:** allow immutest to run on remote server
- **pkg/client:** add token service


<a name="v0.6.2"></a>
## [v0.6.2] - 2020-06-15
### Bug Fixes
- require auth for admin commands even if auth is disabled on server, do not allow admin user to be deactivated
- base64 decoding of passwords: now it requires the "enc:" prefix as base64 can not be differentiated from plain-text at runtime (e.g. "immu" is a valid base64 encode string)
- only require admin password to be changed if it is "immu"
- fix ldflags on dist binaries and add static compilation infos
- **cmd/immuclient/audit:** fix base64 encoded password not working with immuclient audit-mode
- **immuadmin:** repair password change flow right after first admin login
- **pkg/auth:** make ListUsers require admin permissions
- **pkg/ring:** fixes cache corruption due to a ring buffer elements overwrite  on same internal index
- **pkg/store:** remove useless ringbuffer array
- **pkg/store:** fix uniform cache layers size allocation with small values

### Changes
- add bug and feature request report github template
- fix golint errors
- githubactions add windows and build step
- remove plain-test admin password from log outputs
- add message (in cli help and swagger description) about base64-encoded inputs and outputs of get and set commands
- FreeBSD section in the readme
- fix changelog auto generation repo and releasing template
- **pkg/server:** reduce corruption_checker resources usage

### Features
- expose through REST the following user-related actions: create, get, list, change password, set permission and deactivate
- immuclient freebsd daemon installation
- freebsd service install
- read immudb default admin password from flag, config or env var
- use immu as default admin password instead of randomly generated one
- **immudb:** accept base64 string for admin password in flag/config/env var


<a name="v0.6.1"></a>
## [v0.6.1] - 2020-06-09
### Bug Fixes
- disallow running immuadmin backup with current directory as source
- immuadmin dump hangs indefinitely if token is invalid
- [#283](https://github.com/vchain-us/immudb/issues/283), immudb crash on dump of empty db
- fix corruption checker crash during immudb shoutdown
- choose correct config for immudb, immugw installation
- update env vars in README and Docker files ([#297](https://github.com/vchain-us/immudb/issues/297))
- **cmd/immuadmin:** inform user that manual server restart may be needed after interrupted backup
- **cmd/immuadmin:** validate backup dir before asking password
- **cmd/immuclient:** add version sub-command to immuclient interractive mode
- **cmd/immuclient:** nil pointer when audit-mode used with immudb running as daemon
- **cmd/immutest:** add new line at the end of output message
- **pkg/ring:** return nil on inconsistent access to buffer rings elements
- **pkg/store:** fix visualization of not frozen nodes inside print tree command
- **pkg/store/treestore:** fix overwriting on not freezes nodes

### Changes
- add license to tests ([#288](https://github.com/vchain-us/immudb/issues/288))
- update statement about traditional DBs in README
- remove immugw configs from immudb config file [#302](https://github.com/vchain-us/immudb/issues/302)
- **cmd/immuadmin/command:** improve visualization ui in merkle tree print command
- **cmd/immuadmin/command/service:** syntax error, fail build on windows
- **cmd/immuclient/audit:** code cleanup and renaming
- **pkg/store/treestore:** improve cache invalidation

### Code Refactoring
- handling of failed dump

### Features
- allow the password of immugw auditor to be base64 encoded in the config file ([#296](https://github.com/vchain-us/immudb/issues/296))
- add auth support to immutest CLI
- add server-side logout ([#286](https://github.com/vchain-us/immudb/issues/286))
- **cmd/helper:** add functionalities to print colored output
- **cmd/immuadmin:** add print tree command
- **cmd/immutest:** add env var for tokenfile
- **pkg:** add print tree functionality


<a name="v0.6.0"></a>
## [v0.6.0] - 2020-05-28
### Bug Fixes
- rewrite user management to store user, password and permissions separately
- readme doc, immugw start command
- typos in immugw help
- licence
- modify BUILT_BY flag with user email to keep dist script functionalities in makefile
- race condition while prefixing keys
- various permissions-related issues
- when fetching users, only fetch the latest version
- admin user can change password of regular user without having to know his old password
- use iota for permissions enum
- immugw pid path consistency
- SafeZAdd handler SafeZAdd tests. Fix ReferenceHandler test
- safereference_handler, add tests [#264](https://github.com/vchain-us/immudb/issues/264)
- safeset_handler test
- [#260](https://github.com/vchain-us/immudb/issues/260)
- implementation of user deactivate
- fix immuclient windows build
- fix bug on zadd server method
- **cmd/helper:** fix osx build
- **cmd/immuadmin/command/service:** fix error returned by GetDefaultConfigPath
- **cmd/immuadmin/command/service:** fix immudb data uninstall
- **cmd/immuclient:** Added missing documentations and renamed deprecated structures.
- **cmd/immuclient:** Fixed wrong audit credentials error
- **cmd/immuclient:** Added missing documentations and renamed deprecated structures.
- **cmd/immuclient:** Fixed paths.
- **cmd/immuclient/audit:** fix immuclient service installation
- **cmd/immuclient/service:** fix config import

### Changes
- rename back immugw "trust checker" to "auditor"
- improve help for immugw auditor metrics
- rename audit(or) to trust-check(er)
- use status.Error instead of status.Errorf for static string
- use Sprintf instead of string concat
- extract root service from immugw trust checker
- rename default immudb and immugw loggers
- turn sys keys prefixes into constants
- remove setup release in makefile
- service_name inside release build script  is configurable inside makefile. closes [#159](https://github.com/vchain-us/immudb/issues/159) closes [#239](https://github.com/vchain-us/immudb/issues/239)
- remove ppc and arm target arch from makefile
- add CD releases, certificate sign, vcn sign in makefile dist scripts
- add dist scripts in makefile
- fix typo in README.md
- add changelog
- add getByRawSafeIndex tests
- move corruption checker inside immudb process
- update docker files
- immugw audit publishes -1 if empty db and -2 if error, otherwise 0 (check failed) or 1 (succeeded)
- immugw audit publishes -1 value for result and root indexes in case the audit could not run (i.e. empty database, error etc.)
- change immugw metrics port
- refactoring file cache for immugw auditor
- rename immugw trust-checker to auditor
- move auditor package under client directory
- **cmd:** fix corruption checker flag
- **cmd/helper:** add path os wildcard resolver
- **cmd/helper:** fix config path manager stub on linux
- **cmd/helper:** remove useless var
- **cmd/immuadmin:** path of service files and binaries are os dynamic
- **cmd/immuclient:** add pid file management on windows
- **immuadmin:** improve the very first login message

### Code Refactoring
- refactor safeset_handler_test

### Features
- Audit agent added to immuclient.
- make metrics server start configurable through options to aid tests. MetricsServer must not be started as during tests because prometheus lib panis with: duplicate metrics collector registration attempted.
- add immugw auditor
- invalidate tokens by droping public and private keys for a specific user
- check permissions dynamically
- implement user permissions and admin command to set them
- prefix user keys
- update metrics from immugw auditor
- **cmd/immuclient/command:** add getByRawSafeIndex method
- **immugw:** add GET /lastaudit on metrics server


<a name="v0.6.0-RC2"></a>
## [v0.6.0-RC2] - 2020-05-19
### Bug Fixes
- fix stop, improve trust checker log
- handling immudb no connection error and comments
- **cmd/immuadmin:** old password can not be empty when changing password
- **cmd/immuadmin/command:** remove PID by systemd directive
- **cmd/immuadmin/command:** do not erase data without explicit consensus. closes 165
- **cmd/immuadmin/command/service:** fix [#188](https://github.com/vchain-us/immudb/issues/188)
- **cmd/immuclient:** correct argument index for value in rawsafeset
- **cmd/immutest:** rename immutestapp files to immutest
- **pkg/server:** fix error when unlocking unlocked stores after online db restore
- **pkg/store:** wait for pending writes in store.FlushToDisk

### Changes
- remove online backup and restore features
- add copyrights to makefile. closes [#142](https://github.com/vchain-us/immudb/issues/142)
- update dockerfiles
- fix immugw dockerfile with dir property, update README
- manage dir flag in immutc
- add immutc makefile and remove bm from makeall
- fix useless checks, binding address
- use empty struct for values in map that store admin-only methods and add also Backup and Restore methods
- fix useless viper dependency
- fix travis build
- **cmd:** remove useless exit call
- **cmd/immuadmin:** add todos to use the functions from fs package in immuadmin service helpers
- **cmd/immuadmin:** rename offline backup and restore to reflect their offline nature
- **cmd/immuadmin:** fix typo in todo keyword
- **cmd/immugw:** add dir property with default
- **pkg/client:** fix ByRawSafeIndex method comment
- **pkg/client:** add dir property with default
- **pkg/client:** fix client contructor in tests
- **pkg/gw:** add dir property with default, fix error messages
- **pkg/gw:** remove useless root service dependency
- **pkg/immuadmin:** use ReadFromTerminalYN function to get user confirmation to proceed
- **pkg/store:** fix typo in tamper insertion order index error message
- **server:** do not close the stores during cold Restore
- **server:** check for null before closing stores during backup and return absolute backup path

### Features
- show build time in user timezone in version cmd output
- set version to latest git tag
- added interactive cli to immuclient
- **cmd/immutc:** add trust checker command
- **immuadmin:** add offline backup and restore option with possibility to stop and restart the server manually
- **immuadmin:** add cold backup and restore
- **pkg/api/schema:** add byRawSafeIndex proto definitions and related parts
- **pkg/client:** add byRawSafeIndex client
- **pkg/server:** add byRawSafeIndex server
- **pkg/store:** add byRawSafeIndex methods and relateds parts
- **pkg/tc:** add trust checker core


<a name="v0.6.0-RC1"></a>
## [v0.6.0-RC1] - 2020-05-11
### Bug Fixes
- change user message when new password is identical to the old one
- remove common package
- use grpc interceptors for authentication
- place password input on the same line with the password input label
- store auth tokens in user home dir by default and other auth-related enhancements
- fix structured values integration
- disabling CGO to removes the need for the cross-compile dependencies
- remove useless error. https://github.com/dgraph-io/badger/commit/c6c1e5ec7690b5e5d7b47f6ab913bae6f78df03b
- return correct error in safeZAdd handler
- upadating takama/daemon to fix freebsd compilation. closes [#160](https://github.com/vchain-us/immudb/issues/160)
- split main fails in separate folders
- fix immugw immud services in windows os
- improving config management on linux and improved usage message
- add immugw exec to docker image
- wrap root_service with mutex to avoid dirty read
- fix env vars. Switch to toml
- prevent immuadmin users other than immu to login
- env vars names for immudb port and address in CLIs help
- enhance authentication and user management
- environment variables
- fix bug related to retrieve a versioned element by index
- use arguments instead of flags for user create, change-password and deleted
- fix reading user input for passwords
- return verified item on safeset
- code format for multiple files to comply with Go coding standards
- **/pkg/gw:** manage 0 index value in history
- **/pkg/gw:** manage 0 index value in safeget
- **/pkg/gw:** fix guard
- **cmd:** get and safeget error for non-existing key
- **cmd:** remove short alias for tokenfile flag
- **cmd/immu:** fix backup file opening during restore
- **cmd/immu:** Fix newline at the end of restore/backup success message
- **cmd/immu:** set auth header correctly
- **cmd/immuadmin:** generate admin user if not exists also at 1st login attempt with user immu
- **cmd/immuadmin:** verify old password immediately during change-password flow
- **cmd/immuadmin:** fix uninstall automatic stop
- **cmd/immuadmin/command/service:** fix windows helper import
- **cmd/immuadmin/command/service:** fix group creation
- **cmd/immuadmin/command/service:** fix config files in windows
- **immuclient:** do not connect to immudb server for version or mangen commands
- **immudb/command:** fix command test config file path
- **immupopulate:** do not connect to immudb server for version or mangen commands
- **pkg/api/schema:** fix typos
- **pkg/client:** fix stream closing to complete restore
- **pkg/client:** fix cleaning on client tests
- **pkg/client:** fix root file management
- **pkg/client/cache:** manage concurrent read and write ops
- **pkg/gw:** fix hash calc for reference item
- **pkg/gw:** fix lock release in case of errors
- **pkg/gw:** ensure computed item's matches the proof one for safeset
- **pkg/gw:** fix error handling in safe method overwrite
- **pkg/gw:** manage concurrent safeset and get requests
- **pkg/gw:** improve immugw logging
- **pkg/gw:** use leaf computed from the item
- **pkg/gw:** fix gw stop method
- **pkg/gw:** refactor overwrite safe set and get request in order to use dependencies
- **pkg/server:** correct error checking
- **pkg/server:** improve immudb logging
- **pkg/store:** correct health check error comparison
- **pkg/store:** correct gRPC code for key not found error
- **pkg/store:** badger's errors mapping
- **pkg/store:** truncate set to true for windows
- **pkg/store:** fix [#60](https://github.com/vchain-us/immudb/issues/60).

### Changes
- get rid of password generating library
- update .gitignore
- linux services are managed by immu user
- add missing dep
- update deps
- simplify error during first admin login attempt
- marshal options to JSON in String implementations
- grpc-gateway code generation
- Add swagger generation command
- switch to 2.0.3 go_package patched badger version
- simplify .gitignore
- import badger protobuffer schema
- rename immutestapp to immutest
- Update makefile in order to use badeger on codenotary fork
- fix compilation on OS X
- Switch services default ports
- improve configuration features on services management
- prefix version-related variable holding common ldflags so that it matches the convention used for the other version-related variables
- add pid params in config
- fix typo in dump command help
- remove default version value from immu* commands
- change default output folder for  man pages generation
- rename immupopulate to immutestapp
- remove app names from ldflags in Makefile, update immudb description in help
- Switch config format to toml
- Fix namings conventions on immud command config properties
- Remove man pages
- Fix coding style
- move token file name into options and some cleanup
- Print immud running infos properly
- merge code changes related to histograms and detached server options
- refactor immuadmin and stats command to allow dependency injection of immu client
- change label from "latency" to "duration" in immuadmin statistics
- remove types.go from immuadmin statistics cmd
- rename functions that update metrics
- integrate latest changes from master branch
- update termui transitive dependencies
- move immuadmin metrics struct to dedicated file
- rewrite immuadmin client to align it with latest refactorings
- fix project name in CONTRIBUTING.md
- Suppression of ErrObsoleteDataFormat in order to reduce breaking changes
- fix typo in raw command usage help
- rename backup to dump, and disable restore
- info if starting server with empty database
- use exact number of args 2 for set and safeset
- Set correct data folder and show usage in config. closes [#37](https://github.com/vchain-us/immudb/issues/37)
- instructions after make
- update default dbname in server config
- remove immuclient from default make target
- protect default data folder from being inserted in repo
- change config path in server default options
- change default immudb data folder name from "immudb" to "immudata"
- rename binaries and configs
- rename test config file
- remove codenotary badger fork requirement
- rename command "consistency" to "verify"
- remove codenotary badger fork requirement
- change auth header to string
- rename command "verify" to "check-consistency"
- simplify auth options in immu client
- improve login help and cleanup irelevant TODO comment
- add host and version to swagger json
- move server password generation to server start
- get rid of locks on immuclient during login and user during set password
- use all lowercase for immudb everywhere it is mentioned to the user
- Switch config format to ini
- **cmd:** add env vars to commands help and man
- **cmd:** addutility to manage a y/n dialog
- **cmd:** enhance unauthenticated message
- **cmd/immu:** Add reference in command line
- **cmd/immuadmin:** set titles to green and use grid width instead of terminal width to determine plots data length
- **cmd/immuadmin:** remove duplicate dependency
- **cmd/immuadmin:** remove log files in uninstall
- **cmd/immuadmin:** improve code organization and help messages
- **cmd/immuadmin:** remove unused varialble in user command
- **cmd/immuadmin:** extract to functions the init and update of plots in visual stats
- **cmd/immuadmin:** fix examples alignment in user help
- **cmd/immuadmin:** remove ValidArgsFunction from user sub-command
- **cmd/immuadmin:** improved immuadmin service ux
- **cmd/immuadmin:** fix build on freebsd
- **cmd/immuadmin/commands:** fix typo in error message and remove useless options
- **cmd/immuadmin/commands:** fix empty imput and improve immugw install ux
- **cmd/immugw:** overwrite safeZAdd default handler
- **cmd/immugw:** Use default options values
- **cmd/immugw:** overwrite safeReference default handler
- **immuclient:** move pre and post run callbacks to sub-commands
- **pkg/auth:** improve local client detection
- **pkg/client:** add reference client command
- **pkg/client:** add ZAdd and ZScan client methods
- **pkg/gw:** fix default config path for immugw
- **pkg/gw:** refactor handlers in order to use cache adapters
- **pkg/gw:** remove useless check on path
- **pkg/gw:** manage panic into http error
- **pkg/server:** return descriptive error if login gets called when auth is disabled
- **pkg/server:** keep generated keys (used to sign auth token) only in memory
- **pkg/store:** switch to gRPC errors with codes

### Code Refactoring
- refactor  packages to expose commands
- remove immuclient initialization from root level command
- Removed needless allocations and function calls, Rewrote Immuclient package layout
- config is managed properly with cobra and viper combo. closes [#44](https://github.com/vchain-us/immudb/issues/44)
- Structured immugw and handling SIGTERM. closes [#33](https://github.com/vchain-us/immudb/issues/33)
- pkg/tree got ported over to its own external repo codenotary/merkletree
- **pkg/store:** prefix errors with Err

### Features
- add safeget, safeset, safereference and safezadd to the CLI client
- add mtls to immud
- add version to all commands
- Add config file. Closes [#36](https://github.com/vchain-us/immudb/issues/36) closes [#37](https://github.com/vchain-us/immudb/issues/37)
- add mtls to immugw
- add mtls certificates generation script
- always use the default bcrypt cost when hashing passwords
- implement user management
- Add capabilities to run commands in background. Closes [#136](https://github.com/vchain-us/immudb/issues/136) closes [#106](https://github.com/vchain-us/immudb/issues/106)
- hide some of the widgets in immuadmin statistics view if the server does not provide histograms
- add --no-histograms option to server
- complete implementation of visual statistics in immuadmin
- change client "last query at" label
- add "client last active at" metric
- add uptime to metrics
- add immuadmin-related rules to makefile
- create a new build process [#41](https://github.com/vchain-us/immudb/issues/41)
- add text and visual display options to immuadmin statistics
- Add multiroot management, Add client mtls, client refactor. closes [#50](https://github.com/vchain-us/immudb/issues/50) closes [#80](https://github.com/vchain-us/immudb/issues/80)
- Add config file to immu
- improve metrics
- add immuadmin client (WiP)
- add Prometheus-based metrics
- Add raw safeset and safeget method
- Add IScan and improve ScanByIndex command. Closes [#91](https://github.com/vchain-us/immudb/issues/91)
- add insertion order index and tests. closes [#39](https://github.com/vchain-us/immudb/issues/39)
- add current command. Closes [#88](https://github.com/vchain-us/immudb/issues/88)
- Add structured values components
- structured value
- close immuadmin visual statistics also on <Escape>
- add config item to toggle token-based auth on or off
- add token based authentication
- add number of RCs per client metric
- Add config and pid file to immugw
- **cmd:** make unauthenticated message user-friendly
- **cmd/immu:** fix CLI output of safe commands
- **cmd/immu:** add backup and restore commands
- **cmd/immu:** Add ZAdd and ZScan command line methods
- **cmd/immu:** add safeget, safeset, safereference and safezadd commands to immu CLI
- **cmd/immu:** enhance backup and restore commands by writing/reading proto message size to/from the backup file
- **cmd/immuadmin:** add configuration management in service command
- **cmd/immuadmin:** add services management subcommand
- **cmd/immud:** Add pid file parameter
- **cmd/immugw:** add immugw command
- **cmd/immugw:** add logger
- **cmd/immupopulate:** add immupopulate command
- **immuadmin:** show line charts instead of piecharts
- **immuadmin:** add dump command
- **pkg/api/schema:** add autogenerated grpc gw code
- **pkg/api/schema:** add backup and restore protobuffer objects
- **pkg/api/schema:** add safeget set patterns export
- **pkg/api/schema:** add reference and safeReference grpc messages
- **pkg/api/schema:** add zadd, safezadd and zscan grpc messages
- **pkg/client:** add backup and restore and automated tests for them
- **pkg/client:** move keys reading outside ImmuClient and also pass it context from outside
- **pkg/client:** use dedicated structs VerifiedItem and VerifiedIndex as safe functions return type
- **pkg/client:** add root cache service
- **pkg/client:** add new RootService field to ImmuClient, initialize it in connectWithRetry and use it in Safe* functions
- **pkg/client/cache:** add cache file adapter
- **pkg/gw:** add safeset get overwrites
- **pkg/gw:** add safeZAdd overwrite
- **pkg/gw:** add reference and safeReference rest endpoint
- **pkg/logger:** Add file logger
- **pkg/server:** Add backup and restore handlers
- **pkg/server:** add reference and safeReference handlers
- **pkg/server:** Add pid file management
- **pkg/server:** add ZAdd, safeZAdd and zscan handlers
- **pkg/store:** add ZAdd, safeZAdd and ZScan methods and tests
- **pkg/store:** add reference and safeReference methods and tests
- **pkg/store:** Add store backup and restore methods and tests


<a name="v0.0.0-20200206"></a>
## v0.0.0-20200206 - 2020-02-06
### Bug Fixes
- client dial retry and health-check retry counting
- missing dependencies
- address parsing
- apply badger options in bm
- client default options
- typo
- pin go 1.13 for CI
- nil out topic on server shutdown
- get-batch for missing keys
- server re-start
- protobuf codegen
- rpc bm startup
- **bm:** allow GC
- **bm/rpc:** await server to be up
- **client:** missing default options
- **db:** correct treestore data race
- **db:** correct cache pre-allocation
- **db:** workaround for tree indexes when reloading
- **db:** cannot call treestore flush on empty tree
- **db:** correct tree cache positions calculation at boostrap
- **db:** include key as digest input
- **db:** correct tree width calculation at startup
- **db:** default option for sync write
- **db/treestore:** revert debug if condition
- **pkg/server:** correct nil pointer dereferences
- **pkg/server:** correct nil pointer dereference on logging
- **pkg/store:** correct inclusion proof call
- **pkg/store:** correct key prefix guard
- **pkg/store:** add miss condition to limit scan result
- **server:** avoid premature termination
- **tools/bm:** stop on error and correct max batch size
- **tree:** benchmark timers
- **tree:** `at` cannot be equal to width
- **tree:** handle missing node in mem store
- **tree:** correct root of an empty tree

### Changes
- server refactoring
- rename module
- editorconfig
- gitignore
- gitignore immu binaries
- gitignore vendor
- bumps badger to v2.0.1
- update .gitignore
- remove stale files
- update copyright notice
- gitignore bm binary
- gitignore bm binary
- change default data dir to "immustore"
- gitignore working db directory
- nicer error reporting on client failure
- immu client cleanup
- client option type
- docker cleanup
- server options refactoring
- trigger CI on PRs
- move server options to type
- bump badger to latest v2
- remove dead client code
- license headers
- server options refactoring
- client connection wording
- license headers
- api cleanup
- logging cleanup + thresholds
- more load on rpc bm
- improved logging
- simplified client interface
- extract client health check error
- client error refactoring
- missing license headers
- refactor rpc bms
- add license headers
- refactor server logging
- move client options to type
- strip bm binary
- make all target
- clean bm binary
- improved server interface
- rpc bm cleanup
- minor cleanup in bench.py
- new db constructor with options
- **api:** add index in hash construction
- **bm:** fine tuning
- **bm/rpc:** print error
- **client:** add ByIndex rpc
- **cmd/immu:** align commands to new APIs
- **db:** return item index
- **db:** correct logging messages
- **db:** default options
- **db:** treestore improvements
- **db:** switch to unbalanced tree test
- **db:** add default logger
- **db:** treestore entries discarding and ordering
- **db:** fine tuning
- **immu:** improve printing
- **immu:** improved membership verification
- **logger:** expose logging level
- **pkg/api:** add Hash() method for Item
- **pkg/api/schema:** get new root from proof
- **pkg/api/schema:** relax Proof verify when no prev root
- **pkg/api/schema:** refactor bundled proof proto message
- **pkg/api/schema:** add Root message and CurrentRoot RPC
- **pkg/db:** split data and tree storage
- **pkg/store:** code improvements
- **pkg/store:** add error for invalid root index
- **pkg/tree:** add verification for RFC 6962 examples
- **pkg/tree:** improve comment
- **schema:** add index
- **server:** add ByIndex rpc
- **tools:** add nimmu hacking tool
- **tools:** benchmark
- **tools:** armonize comparison settings
- **tools:** no logging needed for nimmu
- **tools:** add makefile target for comparison
- **tools:** benchmark
- **tools/bm:** correct method signature to accomodate indexes
- **tools/nimmu:** improve input and output
- **tree:** correct returned value for undefined ranges
- **tree:** clean up map store
- **tree:** correct MTH test var naming
- **tree:** reduce testPaths
- **tree:** remove unnecessary int conversion
- **tree:** add map store test
- **tree:** add print tree helper for debugging
- **tree:** add map backed mem store for testing
- **tree:** add IsFrozen helper

### Code Refactoring
- change APIs according to the new schema
- reviewed schema package
- rname "membership" to "inclusion"
- use []byte keys
- "db" pkg renamed to "store"
- logging prefixes and miscellany renamed according to immustore
- env vars prefix renamed to IMMU_
- rename module to immustore
- **db:** new storing strategy
- **db:** use Item on ByIndex API
- **db:** use ring buffer for caching
- **pkg/tree:** improve coverage and clean up
- **server:** metrics prefix renamed to immud_
- **tree:** AppendHash with side-effect
- **tree:** testing code improvement
- **tree:** switch to unbalanced tree
- **tree:** reduce lookups
- **tree:** remove unnecessary Tree struct and correct int sizes
- **tree:** simplify Storer interface
- **tree:** trival optimization

### Features
- extract rpc bm constructor
- initial draft for storing the tree alongside the data
- immu client
- Makefile
- basic protobuf schema
- poc transport format
- poc grpc server
- poc grpc client
- poc server wiring
- poc client wiring
- topic wiring
- poc topic get
- expose metrics
- server topic get wiring
- poc cli args
- client-side performance logs
- transport schema cleanup
- docker
- server options
- client options
- server options for dbname
- immud command line args
- client cli args
- logging infrastructure
- set stdin support
- add healtcheck command
- client type
- treestore with caching
- make bm
- batch-set
- no healthcheck/dial retry wait-period when set to 0
- bm tooling
- bm improvements (rpc-style and in-process api calls)
- scylla comparison
- immud health-check
- batch get requests
- pretty-print client options
- client connect and wait for health-check
- stateful client connection
- return item index for get and set ops
- expose dial and health-check retry configuration
- apply env options for server
- apply env options for client
- server options from environment
- client options from environment
- client connect with closure
- client errors extracted
- named logger
- bm memstats
- CI action
- server health-check
- db health-check
- set-batch bm
- client set-batch
- key reader
- initial project skeleton
- **api:** add item and item list data structure
- **api:** add membership proof struct
- **client:** membership command
- **client:** add history command
- **cmd/immu:** scan and count commands
- **db:** add membership proof API
- **db:** async commit option
- **db:** add history API to get all versions of the same key
- **db:** get item by index
- **pkg/api/schema:** SafeGet
- **pkg/api/schema:** consistency proof API
- **pkg/api/schema:** add SafeSet to schema.proto
- **pkg/api/schema:** ScanOptions
- **pkg/api/schema:** added Scan and Count RPC
- **pkg/client:** Scan and Count API
- **pkg/client:** consistecy proof
- **pkg/server:** SafeSet RPC
- **pkg/server:** SafeGet RPC
- **pkg/server:** consistency proof RPC
- **pkg/server:** CurrentRoot RPC
- **pkg/server:** Scan and Count API
- **pkg/store:** SafeGet API
- **pkg/store:** count API
- **pkg/store:** consistency proof
- **pkg/store:** SafeSet
- **pkg/store:** CurrentRoot API
- **pkg/store:** range scan API
- **pkg/tree:** consistency proof
- **pkg/tree:** Path to slice conversion
- **pkg/tree:** consistency proof verification
- **pkg/tree:** test case for RFC 6962 examples
- **ring:** ring buffer
- **schema:** add message for membership proof
- **server:** add membership rpc
- **server:** support for history API
- **tree:** audit path construction
- **tree:** membership proof verification
- **tree:** Merkle Consistency Proof reference impl
- **tree:** Merkle audit path reference impl
- **tree:** Storer interface
- **tree:** draft implementation
- **tree:** MTH reference impl


[Unreleased]: https://github.com/vchain-us/immudb/compare/v0.8.0...HEAD
[v0.8.0]: https://github.com/vchain-us/immudb/compare/v0.7.0...v0.8.0
[v0.7.0]: https://github.com/vchain-us/immudb/compare/v0.6.2...v0.7.0
[v0.6.2]: https://github.com/vchain-us/immudb/compare/v0.6.1...v0.6.2
[v0.6.1]: https://github.com/vchain-us/immudb/compare/v0.6.0...v0.6.1
[v0.6.0]: https://github.com/vchain-us/immudb/compare/v0.6.0-RC2...v0.6.0
[v0.6.0-RC2]: https://github.com/vchain-us/immudb/compare/v0.6.0-RC1...v0.6.0-RC2
[v0.6.0-RC1]: https://github.com/vchain-us/immudb/compare/v0.0.0-20200206...v0.6.0-RC1
