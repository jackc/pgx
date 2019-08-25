// +build !go1.13

package errors

import (
	errors "golang.org/x/xerrors"
)

// New  wrap errors.New
func New(text string) error {
	return errors.New(text)
}

// Errorf  wrap xerrors.Errorf
func Errorf(format string, a ...interface{}) error {
	return errors.Errorf(format, a...)
}

// As wrap xerrors.As
func As(err error, target interface{}) bool {
	return errors.As(err, target)
}

// Is wrap xerrors.As
func Is(err error, target error) bool {
	return errors.Is(err, target)
}
