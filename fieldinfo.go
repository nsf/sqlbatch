package sqlbatch

import (
	"reflect"
	"strings"
	"unsafe"
)

type FieldInterface struct {
	Conv   func(iface interface{}, b *strings.Builder)
	Get    func(structPtr unsafe.Pointer, ifacePtr *interface{})
	GetPtr func(structPtr unsafe.Pointer, ifacePtr *interface{})
	Set    func(structPtr unsafe.Pointer, iface interface{})
	Write  func(structPtr unsafe.Pointer, b *strings.Builder)
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
