package immuadmin

import (
	"bytes"
	"io/ioutil"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabaseList(t *testing.T) {
	_, cmd := newTestCommandLine(t)
	// Set arguments to execute the database list command.
	cmd.SetArgs([]string{"database", "list"})

	// Set a buffer to read the command output.
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	// Execute command and verify the output contains the name of the
	// database created by default.
	err := cmd.Execute()
	assert.NoError(t, err, "Executing database list command should not fail.")

	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	assert.Contains(t, string(out), "defaultdb")
}

func TestDatabaseCreate(t *testing.T) {
	_, cmd := newTestCommandLine(t)
	// Set arguments to create a new database.
	cmd.SetArgs([]string{"database", "create", "mynewdb"})

	// Set a buffer to read the command output.
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	// Execute command and verify the output contains a success message.
	err := cmd.Execute()
	assert.NoError(t, err, "Executing database create command should not fail.")

	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	assert.Regexp(t, regexp.MustCompile("database 'mynewdb' .* successfully created"), string(out))
}
