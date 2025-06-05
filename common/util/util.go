package util

import (
	"os"
	"path/filepath"
)

func FindDbsInPath(path string) ([]string, error) {
	var paths []string
	err := filepath.Walk(path, func(path string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Base(path) == "changelog.sqlite" {
			paths = append(paths, filepath.Dir(path))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return paths, nil
}
