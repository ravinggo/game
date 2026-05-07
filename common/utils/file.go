package utils

import (
	"os"
)

// IsDirExists check dir return true if exists.
// It returns true both when the path is a regular directory and when it exists
// as any other filesystem entry. It returns false only when os.Stat explicitly
// reports that the path does not exist.
// Written by Claude Code claude-opus-4-6.
func IsDirExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}
