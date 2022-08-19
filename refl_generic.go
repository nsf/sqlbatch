package sqlbatch

import (
	"reflect"
	"strings"
	"unsafe"
)

func makeInterfacePtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *any) {
	return func(structPtr unsafe.Pointer, ifacePtr *any) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*any)(p)
	}
}

func makeInterfaceWriter(offset uintptr, custom FieldInterfaceResolver) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		iface := *(*any)(p)
		t := GetTypeInfo(reflect.TypeOf(iface), custom)
		t.Conv(iface, b)
	}
}

func makeInterfaceConverter(custom FieldInterfaceResolver) func(iface any, b *strings.Builder) {
	return func(iface any, b *strings.Builder) {
		t := GetTypeInfo(reflect.TypeOf(iface), custom)
		t.Conv(iface, b)
	}
}
