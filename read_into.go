package sqlbatch

import (
	"reflect"
	"unsafe"
)

type readInto struct {
	val       reflect.Value  // pointer with type info
	ptr       unsafe.Pointer // raw pointer
	si        *StructInfo
	slice     bool
	errp      *error
	primitive bool // is primitive type? (fallback to reflect API)
	stmt      string
}
