package sqlbatch

import (
	"reflect"
	"strings"
	"unsafe"
)

func makeGenericWriter(offset uintptr, t reflect.Type) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		reflect.NewAt(t, p).Interface().(GenericField).SqlbatchWrite(b)
	}
}

func makeGenericSetter(offset uintptr, t reflect.Type) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		reflect.NewAt(t, p).Interface().(GenericField).SqlbatchSet(ifacePtr)
	}
}

func makeGenericPtrGetter(offset uintptr, t reflect.Type) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		reflect.NewAt(t, p).Interface().(GenericField).SqlbatchGetPtr(ifacePtr)
	}
}

func makeGenericGetter(offset uintptr, t reflect.Type) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		reflect.NewAt(t, p).Interface().(GenericField).SqlbatchGet(ifacePtr)
	}
}

func genericConverter(iface interface{}, b *strings.Builder) {
	val := iface.(GenericFieldConv)
	val.SqlbatchConv(b)
}
