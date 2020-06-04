package db

import (
	"strconv"
	"time"
)

type DbOptions struct {
	dbDir    string
	DbName   string
	sysDbDir string
}

// DefaultOption Initialise Db Optionts to default values
func DefaultOption() *DbOptions {
	ID := strconv.FormatInt(time.Now().UnixNano(), 10)
	return &DbOptions{
		dbDir:    "immudb",
		DbName:   "db_" + ID,
		sysDbDir: "immudbsys",
	}
}

// WithDbName sets dbName
func (o *DbOptions) WithDbName(dbName string) *DbOptions {
	ID := strconv.FormatInt(time.Now().UnixNano(), 10)
	o.DbName = dbName + "_" + ID
	return o
}

// GetDbDir Returns Database Directory name
func (o *DbOptions) GetDbDir() string {
	return o.dbDir
}

// GetSysDbDir Returns System Database Directory name
func (o *DbOptions) GetSysDbDir() string {
	return o.sysDbDir
}
