package sqlbatch

import (
	"context"
	"errors"
	"testing"
)

func TestMultiErrorIs(t *testing.T) {
	var e1 error = &MultiError{Errors: []error{context.Canceled, context.Canceled}}
	var e2 error = &MultiError{Errors: []error{context.Canceled, errors.New("hello")}}
	if !errors.Is(e1, context.Canceled) {
		t.Errorf("e1 must be reported as context.Canceled")
	}
	if errors.Is(e2, context.Canceled) {
		t.Errorf("e2 must not be reported as context.Canceled")
	}
}
