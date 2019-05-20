package sqlbatch

import (
	"database/sql"
	"github.com/lib/pq"
	"time"
	"unsafe"
)

//--------------------------------------------------------------------------

func makeNullBoolGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*sql.NullBool)(p)
	}
}

func makeNullFloat64Getter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*sql.NullFloat64)(p)
	}
}

func makeNullInt64Getter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*sql.NullInt64)(p)
	}
}

func makeNullStringGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*sql.NullString)(p)
	}
}

func makeNullTimeGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*pq.NullTime)(p)
	}
}

//--------------------------------------------------------------------------

func makeTimeGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*time.Time)(p)
	}
}

func makeByteSliceGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*[]byte)(p)
	}
}

//--------------------------------------------------------------------------

func makeBoolGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*bool)(p)
	}
}

func makeStringGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*string)(p)
	}
}

//--------------------------------------------------------------------------

func makeFloat32Getter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*float32)(p)
	}
}

func makeFloat64Getter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*float64)(p)
	}
}

//--------------------------------------------------------------------------

func makeIntGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*int)(p)
	}
}

func makeInt8Getter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*int8)(p)
	}
}

func makeInt16Getter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*int16)(p)
	}
}

func makeInt32Getter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*int32)(p)
	}
}

func makeInt64Getter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*int64)(p)
	}
}

//--------------------------------------------------------------------------

func makeUintGetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*uint)(p)
	}
}

func makeUint8Getter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*uint8)(p)
	}
}

func makeUint16Getter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*uint16)(p)
	}
}

func makeUint32Getter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*uint32)(p)
	}
}

func makeUint64Getter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*uint64)(p)
	}
}
