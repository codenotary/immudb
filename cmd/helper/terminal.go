package helper

import (
	"fmt"
	"io"
	"strings"
)

type terminalReader struct{}

type TerminalReader interface {
	ReadFromTerminalYN(def string) (selected string, err error)
}

func NewTerminalReader() *terminalReader {
	return &terminalReader{}
}

// ReadFromTerminalYN read terminal user input from a Yes No dialog. It returns y and n only with an explicit Yy or Nn input. If no input is submitted it returns default value. If the input is different from the expected one empty string is returned.
func (t *terminalReader) ReadFromTerminalYN(def string) (selected string, err error) {
	var u string
	var n int
	if n, err = fmt.Scanln(&u); err != nil && err != io.EOF && err.Error() != "unexpected newline" {
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
