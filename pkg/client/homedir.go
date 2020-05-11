package client

import (
	"io/ioutil"
	"os"
	"strings"

	"github.com/mitchellh/go-homedir"
)

func WriteFileToUserHomeDir(content []byte, pathToFile string) error {
	p := pathToFile
	if !strings.Contains(pathToFile, "/") && !strings.Contains(pathToFile, "\\") {
		hd, err := homedir.Dir()
		if err == nil {
			p = hd + string(os.PathSeparator) + p
			if err := ioutil.WriteFile(p, content, 0644); err == nil {
				return nil
			}
		}
	}
	return ioutil.WriteFile(p, content, 0644)
}
func FileExistsInUserHomeDir(pathToFile string) (bool, error) {
	if !strings.Contains(pathToFile, "/") && !strings.Contains(pathToFile, "\\") {
		hd, err := homedir.Dir()
		if err == nil {
			p := hd + string(os.PathSeparator) + pathToFile
			if _, err := os.Stat(p); err == nil {
				return true, nil
			}
		}
	}
	if _, err := os.Stat(pathToFile); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
func ReadFileFromUserHomeDir(pathToFile string) (string, error) {
	if !strings.Contains(pathToFile, "/") && !strings.Contains(pathToFile, "\\") {
		hd, err := homedir.Dir()
		if err == nil {
			p := hd + string(os.PathSeparator) + pathToFile
			if _, err := os.Stat(p); err == nil {
				contentBytes, err := ioutil.ReadFile(p)
				if err == nil {
					return string(contentBytes), nil
				}
			}
		}
	}
	contentBytes, err := ioutil.ReadFile(pathToFile)
	if err != nil {
		return "", err
	}
	return string(contentBytes), nil
}
func DeleteFileFromUserHomeDir(pathToFile string) {
	if !strings.Contains(pathToFile, "/") && !strings.Contains(pathToFile, "\\") {
		hd, err := homedir.Dir()
		if err == nil {
			p := hd + string(os.PathSeparator) + pathToFile
			os.Remove(p)
		}
	}
	os.Remove(pathToFile)
}
