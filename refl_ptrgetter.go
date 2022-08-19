package sqlbatch

import (
	"unsafe"
)

func MakePtrGetter[T any](offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *any) {
	return func(structPtr unsafe.Pointer, ifacePtr *any) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*T)(p)
	}
}
