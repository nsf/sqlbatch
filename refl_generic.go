package sqlbatch

import (
	"reflect"
	"strings"
	"unsafe"
)

func makeInterfacePtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*interface{})(p)
	}
}

func makeInterfaceWriter(offset uintptr, custom FieldInterfaceResolver) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		iface := *(*interface{})(p)
		t := GetTypeInfo(reflect.TypeOf(iface), custom)
		t.Conv(iface, b)
	}
}

func makeInterfaceConverter(custom FieldInterfaceResolver) func(iface interface{}, b *strings.Builder) {
	return func(iface interface{}, b *strings.Builder) {
		t := GetTypeInfo(reflect.TypeOf(iface), custom)
		t.Conv(iface, b)
	}
}
