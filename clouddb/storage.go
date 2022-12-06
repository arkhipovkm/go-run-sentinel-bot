package clouddb

import (
	"os"
	"path/filepath"
)

func init() {
	var err error
	// Panicking here because there is nowhere to back-propagate these errors

	err = os.MkdirAll(filepath.Join("storage", "cache", "items"), os.ModePerm)
	if err != nil {
		panic(err)
	}
	err = os.MkdirAll(filepath.Join("storage", "cache", "runs"), os.ModePerm)
	if err != nil {
		panic(err)
	}
	err = os.MkdirAll(filepath.Join("storage", "cache", "devices"), os.ModePerm)
	if err != nil {
		panic(err)
	}
	err = os.MkdirAll(filepath.Join("storage", "cache", "chunks"), os.ModePerm)
	if err != nil {
		panic(err)
	}
}
