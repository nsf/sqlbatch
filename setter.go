package sqlbatch

import (
	"database/sql"
	"github.com/lib/pq"
	"time"
	"unsafe"
)

//--------------------------------------------------------------------------

func makeNullBoolSetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*sql.NullBool)(p) = ifacePtr.(sql.NullBool)
	}
}

func makeNullFloat64Setter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*sql.NullFloat64)(p) = ifacePtr.(sql.NullFloat64)
	}
}

func makeNullInt64Setter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*sql.NullInt64)(p) = ifacePtr.(sql.NullInt64)
	}
}

func makeNullStringSetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*sql.NullString)(p) = ifacePtr.(sql.NullString)
	}
}

func makeNullTimeSetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*pq.NullTime)(p) = ifacePtr.(pq.NullTime)
	}
}

//--------------------------------------------------------------------------

func makeTimeSetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*time.Time)(p) = ifacePtr.(time.Time)
	}
}

func makeByteSliceSetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*[]byte)(p) = ifacePtr.([]byte)
	}
}

//--------------------------------------------------------------------------

func makeBoolSetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*bool)(p) = ifacePtr.(bool)
	}
}

func makeStringSetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*string)(p) = ifacePtr.(string)
	}
}

//--------------------------------------------------------------------------

func makeFloat32Setter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*float32)(p) = ifacePtr.(float32)
	}
}

func makeFloat64Setter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*float64)(p) = ifacePtr.(float64)
	}
}

//--------------------------------------------------------------------------

func makeIntSetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*int)(p) = ifacePtr.(int)
	}
}

func makeInt8Setter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*int8)(p) = ifacePtr.(int8)
	}
}

func makeInt16Setter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*int16)(p) = ifacePtr.(int16)
	}
}

func makeInt32Setter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*int32)(p) = ifacePtr.(int32)
	}
}

func makeInt64Setter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*int64)(p) = ifacePtr.(int64)
	}
}

//--------------------------------------------------------------------------

func makeUintSetter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*uint)(p) = ifacePtr.(uint)
	}
}

func makeUint8Setter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*uint8)(p) = ifacePtr.(uint8)
	}
}

func makeUint16Setter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*uint16)(p) = ifacePtr.(uint16)
	}
}

func makeUint32Setter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*uint32)(p) = ifacePtr.(uint32)
	}
}

func makeUint64Setter(offset uintptr) func(structPtr unsafe.Pointer, ifacePtr interface{}) {
	return func(structPtr unsafe.Pointer, ifacePtr interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*(*uint64)(p) = ifacePtr.(uint64)
	}
}
