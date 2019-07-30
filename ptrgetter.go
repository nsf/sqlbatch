package sqlbatch

import (
	"database/sql"
	"github.com/lib/pq"
	"time"
	"unsafe"
)

//--------------------------------------------------------------------------

func makeNullBoolPtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*sql.NullBool)(p)
	}
}

func makeNullFloat64PtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*sql.NullFloat64)(p)
	}
}

func makeNullInt64PtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*sql.NullInt64)(p)
	}
}

func makeNullStringPtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*sql.NullString)(p)
	}
}

func makeNullTimePtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*pq.NullTime)(p)
	}
}

//--------------------------------------------------------------------------

func makeTimePtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*time.Time)(p)
	}
}

func makeByteSlicePtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*[]byte)(p)
	}
}

//--------------------------------------------------------------------------

func makeBoolPtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*bool)(p)
	}
}

func makeStringPtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*string)(p)
	}
}

//--------------------------------------------------------------------------

func makeFloat32PtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*float32)(p)
	}
}

func makeFloat64PtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*float64)(p)
	}
}

//--------------------------------------------------------------------------

func makeIntPtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*int)(p)
	}
}

func makeInt8PtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*int8)(p)
	}
}

func makeInt16PtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*int16)(p)
	}
}

func makeInt32PtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*int32)(p)
	}
}

func makeInt64PtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*int64)(p)
	}
}

//--------------------------------------------------------------------------

func makeUintPtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*uint)(p)
	}
}

func makeUint8PtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*uint8)(p)
	}
}

func makeUint16PtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*uint16)(p)
	}
}

func makeUint32PtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*uint32)(p)
	}
}

func makeUint64PtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*uint64)(p)
	}
}

//--------------------------------------------------------------------------

func makeInt64SlicePtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*[]int64)(p)
	}
}

func makeStringSlicePtrGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = (*[]string)(p)
	}
}
