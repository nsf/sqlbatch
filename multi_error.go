package sqlbatch

import (
	"errors"
	"strings"
)

type MultiError struct {
	Errors []error
}

func (m *MultiError) Error() string {
	var sb strings.Builder
	for i, e := range m.Errors {
		if i != 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(e.Error())
	}
	return sb.String()
}

func (m *MultiError) Is(target error) bool {
	for _, e := range m.Errors {
		if !errors.Is(e, target) {
			return false
		}
	}
	return true
}
