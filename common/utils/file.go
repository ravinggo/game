package utils

import (
	"os"
)

// IsDirExists check dir return true if exists
func IsDirExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}
