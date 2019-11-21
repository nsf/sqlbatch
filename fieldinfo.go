package sqlbatch

import (
	"reflect"
	"strings"
	"unsafe"
)

// Set currently only used by time.Time for setting the time value implicitly
// GetPtr is used when scanning Row result into struct field
// Write is also used for expression formatting
// Conv is used for expression formatting
type FieldInterface struct {
	Set    func(structPtr unsafe.Pointer, iface interface{})
	GetPtr func(structPtr unsafe.Pointer, ifacePtr *interface{})
	Write  func(structPtr unsafe.Pointer, b *strings.Builder)
	Conv   func(iface interface{}, b *strings.Builder)
}

type FieldInfoFlag uint32

const (
	FieldInfoIsCreated FieldInfoFlag = 1 << iota
	FieldInfoIsUpdated
	FieldInfoIsPrimaryKey
	FieldInfoIsNull
)

type FieldInfo struct {
	flags      FieldInfoFlag
	Name       string
	QuotedName string
	Interface  FieldInterface
	Group      string
	Type       reflect.Type
}

func (f *FieldInfo) IsPrimaryKey() bool { return f.flags&FieldInfoIsPrimaryKey != 0 }
func (f *FieldInfo) IsCreated() bool    { return f.flags&FieldInfoIsCreated != 0 }
func (f *FieldInfo) IsUpdated() bool    { return f.flags&FieldInfoIsUpdated != 0 }
func (f *FieldInfo) IsNull() bool       { return f.flags&FieldInfoIsNull != 0 }
