package sqlbatch

import (
	"unsafe"
)

func MakeSetter[T any](offset uintptr) func(structPtr unsafe.Pointer, ifacePtr any) {
	return func(structPtr unsafe.Pointer, ifacePtr any) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*T)(p) = ifacePtr.(T)
	}
}
