package db

import (
	"strings"
	"testing"
)

func TestDefaultOptions(t *testing.T) {
	op := DefaultOption()
	if !strings.Contains(op.DbName, "db_") {
		t.Errorf("default db dir non as per convention")
	}
	if op.GetDbDir() != "immudb" {
		t.Errorf("default db name non as per convention")
	}
	if op.GetSysDbDir() != "immudbsys" {
		t.Errorf("default sysdb name non as per convention")
	}
}

func TestSetOptions(t *testing.T) {
	DbName := "Charles_Aznavour"
	op := DefaultOption().WithDbName(DbName)
	if !strings.Contains(op.DbName, DbName) {
		t.Errorf("db name not set correctly , expected %s got %s", DbName, op.DbName)
	}
}
