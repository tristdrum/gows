package storage

import "errors"

var ErrNotFound = errors.New("not found")

type StorageDisabledError struct {
	Name string
}

func (e StorageDisabledError) Error() string {
	if e.Name == "" {
		return "storage disabled"
	}
	return "storage disabled: " + e.Name
}

func StorageDisabled(name string) StorageDisabledError {
	return StorageDisabledError{Name: name}
}
