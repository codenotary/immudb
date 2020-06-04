package db

import (
	"os"
	"path"
	"testing"
)

func TestDefaultDbCreation(t *testing.T) {
	options := DefaultOption()
	_, err := NewDb(options)
	if err != nil {
		t.Errorf("Error creating Db instance %s", err)
	}
	defer os.RemoveAll(options.DbName)

	if _, err = os.Stat(options.DbName); os.IsNotExist(err) {
		t.Errorf("Db dir not created")
	}

	_, err = os.Stat(path.Join(options.DbName, options.GetDbDir()))
	if os.IsNotExist(err) {
		t.Errorf("Data dir not created")
	}

	_, err = os.Stat(path.Join(options.DbName, options.GetSysDbDir()))
	if os.IsNotExist(err) {
		t.Errorf("Sys dir not created")
	}
}
func TestDbCreation(t *testing.T) {
	options := DefaultOption().WithDbName("EdithPiaf")
	_, err := NewDb(options)
	if err != nil {
		t.Errorf("Error creating Db instance %s", err)
	}
	defer os.RemoveAll(options.DbName)

	if _, err = os.Stat(options.DbName); os.IsNotExist(err) {
		t.Errorf("Db dir not created")
	}

	_, err = os.Stat(path.Join(options.DbName, options.GetDbDir()))
	if os.IsNotExist(err) {
		t.Errorf("Data dir not created")
	}

	_, err = os.Stat(path.Join(options.DbName, options.GetSysDbDir()))
	if os.IsNotExist(err) {
		t.Errorf("Sys dir not created")
	}
}
