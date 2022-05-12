
# Hot backup and restore

Hot backup/restore feature allows to backup and restore immudb database without stopping immudb engine. Database remains accessible during backup process. It is possibly to perform full or incremental/differential backup and restore.

Both backup and restore functions can use streams or files as a source/destination.

Backup file is not compressed, assuming user may use any suitable method (see examples for bzip2 compression).

## Backup

```
immuadmin hot-backup <database> [-o <file> [--append]] [--start-tx]
```

### Full backup

To run full database backup, execute `immuadmin hot-backup <database>` command, specifying the optional backup file name with `-o` options. If `-o` option is not specified, output is sent to `stdout`.

If backup file is specified with `-o` option, the file is created. If file already exists, backup process fails.

### Incremental backup

When backup database up to the existing file, `immuadmin` tools finds the last backed up database transaction in file, verifies its checksum and appends only database changes, made after this transaction. `immuadmin` requires user to specify `--append` command line option to append to existing file.

When backup up to the stream, `immuadmin` doesn't have information about last backup up transaction, however user can specify the ID of the transaction to start from with `--start-tx` command line option. It allows user to implement incremental/differential backup strategy, using streams.

## Restore
```
immuadmin hot-restore <database> [-i <file>] [--append] [--force] [--force-replica]
```

### Full restore

To run full restore, execute `immuadmin hot-restore <database>` command, specifying the optional backup file name with `-o` options. If `-o` option is not specified, input data is read from `stdin`.

If database already exists, restore process fails.

### Incremental restore

If database already exists, it is possible to append new data from backup file to the database. In this case user has to specify `--append` flag.

#### Transaction overlap/gap handling

`immuadmin` tries to verify that backup file and database where data are being restored to have the same origin. To do this `immuadmin` finds the last transaction in source database and the same transaction in the backup file, and check transaction signatures. If transactions don't match, restore isn't possible.

When there is no overlap between transactions in database and file, transaction verification is not possible. However, if there is no gap between transactions, `immuadmin` allows to bypass verification with `--force` command line option. If there is a gap between last transaction in database and first transaction in file, restore isn't possible.

### Transaction verification

During restore process `immuadmin` checks if checksum, reported by database after restoration of the transaction, matches the one stored in the file during backup process. It allows to detect backup file accidental or malicious corruption.

### Replica flag handling

It is possible to restore data only to the replica database. During full restore database automatically created as replica (replica flag is switched off after restore), but for incremental restore `immuadmin` assumes database is already in replica mode (user can use `immuadmin database update <database> --replication-enabled` command to switch on replica mode).

However, it is possible to automatically switch on and off replica mode for incremental backup using `--force-replica` command line option.

### Verifying backup file

```
immuadmin hot-restore --verify [-i <file>]
```
It is possible to verify backup file/stream using `immuadmin hot-restore --verify` command. It only checks the correctness of database file, e.g. file format and correct sequence of transactions in file. The only way to detect data corruption is to restore data.

## Examples

Full backup to file:
```
immuadmin hot-backup foo -o foo.backup
immuadmin hot-backup foo > foo.backup
```

Incremental backup to file:
```
immuadmin hot-backup foo -o foo.backup --append
```
Incremental backup with `bzip2` compression:
```
immuadmin hot-backup foo --start-tx 123 | bzip2 > foo.bz2
```
Full restore
```
immuadmin hot-restore bar -i foo.backup
immuadmin hot-restore bar < foo.backup
```
Full restore from `bzip2`-compressed file
```
bunzip2 foo.bz2 -c | immuadmin hot-restore bar
```
Incremental restore with automatic switching of replica mode
```
immuadmin hot-restore bar -i foo.backup --append --force-replica
```

Copy database:
```
immuadmin hot-backup foo | immuadmin hot-restore bar
```
