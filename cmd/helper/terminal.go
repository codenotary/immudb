package helper

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"golang.org/x/crypto/ssh/terminal"
)

type terminalReader struct {
	r io.Reader
}

type TerminalReader interface {
	ReadFromTerminalYN(def string) (selected string, err error)
}

func NewTerminalReader(r io.Reader) *terminalReader {
	return &terminalReader{r}
}

// ReadFromTerminalYN read terminal user input from a Yes No dialog. It returns y and n only with an explicit Yy or Nn input. If no input is submitted it returns default value. If the input is different from the expected one empty string is returned.
func (t *terminalReader) ReadFromTerminalYN(def string) (selected string, err error) {
	var u string
	var n int
	if n, err = fmt.Fscanln(t.r, &u); err != nil && err != io.EOF && err.Error() != "unexpected newline" {
		return "", err
	}
	if n <= 0 {
		u = def
	}
	u = strings.TrimSpace(strings.ToLower(u))
	if u == "y" {
		return "y", nil
	}
	if u == "n" {
		return "n", nil
	}
	return "", nil
}

// PasswordReader ...
type PasswordReader interface {
	Read(string) ([]byte, error)
}
type stdinPasswordReader struct {
	trp TerminalReadPw
}

// DefaultPasswordReader ...
var DefaultPasswordReader PasswordReader = stdinPasswordReader{trp: terminalReadPw{}}

func (pr stdinPasswordReader) Read(msg string) ([]byte, error) {
	fi, _ := os.Stdin.Stat()
	// pipe?
	if (fi.Mode() & os.ModeCharDevice) == 0 {
		pass, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return nil, err
		}
		return pass, nil
	} else {
		// terminal
		fmt.Print(msg)
		pass, err := pr.trp.ReadPassword(int(os.Stdin.Fd()))
		fmt.Println()
		if err != nil {
			return nil, err
		}
		return pass, nil
	}
}

type terminalReadPw struct{}
type TerminalReadPw interface {
	ReadPassword(fd int) ([]byte, error)
}

func (trp terminalReadPw) ReadPassword(fd int) ([]byte, error) {
	return terminal.ReadPassword(fd)
}
