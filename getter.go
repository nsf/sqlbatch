package sqlbatch

import (
	"database/sql"
	"github.com/lib/pq"
	"reflect"
	"time"
	"unsafe"
)

// when this function is invoked, t.Kind() is reflect.Struct
type StructFieldGetterFuncResolver func(t reflect.Type, offset uintptr) (StructFieldGetterFunc, bool)

func resolveCustomGetter(t reflect.Type, offset uintptr, custom StructFieldGetterFuncResolver) (StructFieldGetterFunc, bool) {
	if custom == nil {
		return nil, false
	} else {
		return custom(t, offset)
	}
}

func MakeStructFieldGetterFuncForField(t reflect.StructField, offset uintptr, custom StructFieldGetterFuncResolver) StructFieldGetterFunc {
	o := t.Offset + offset
	switch t.Type.Kind() {
	case reflect.Bool:
		return makeBoolGetter(o)
	case reflect.Int:
		return makeIntGetter(o)
	case reflect.Int8:
		return makeInt8Getter(o)
	case reflect.Int16:
		return makeInt16Getter(o)
	case reflect.Int32:
		return makeInt32Getter(o)
	case reflect.Int64:
		return makeInt64Getter(o)
	case reflect.Uint:
		return makeUintGetter(o)
	case reflect.Uint8:
		return makeUint8Getter(o)
	case reflect.Uint16:
		return makeUint16Getter(o)
	case reflect.Uint32:
		return makeUint32Getter(o)
	case reflect.Uint64:
		return makeUint64Getter(o)
	case reflect.String:
		return makeStringGetter(o)
	case reflect.Float32:
		return makeFloat32Getter(o)
	case reflect.Float64:
		return makeFloat64Getter(o)
	case reflect.Slice:
		if t.Type.Elem().Kind() == reflect.Uint8 { // byte slice
			return makeByteSliceGetter(o)
		}
	case reflect.Struct:
		if getter, ok := resolveCustomGetter(t.Type, o, custom); ok {
			return getter
		} else if t.Type == reflect.TypeOf(time.Time{}) {
			return makeTimeGetter(o)
		} else if t.Type == reflect.TypeOf(sql.NullBool{}) {
			return makeNullBoolGetter(o)
		} else if t.Type == reflect.TypeOf(sql.NullFloat64{}) {
			return makeNullFloat64Getter(o)
		} else if t.Type == reflect.TypeOf(sql.NullInt64{}) {
			return makeNullInt64Getter(o)
		} else if t.Type == reflect.TypeOf(sql.NullString{}) {
			return makeNullStringGetter(o)
		} else if t.Type == reflect.TypeOf(pq.NullTime{}) {
			return makeNullTimeGetter(o)
		}
	}
	panic("unsupported field type: " + t.Type.String())
}

//--------------------------------------------------------------------------

func makeNullBoolGetter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*sql.NullBool)(p)
	}
}

func makeNullFloat64Getter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*sql.NullFloat64)(p)
	}
}

func makeNullInt64Getter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*sql.NullInt64)(p)
	}
}

func makeNullStringGetter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*sql.NullString)(p)
	}
}

func makeNullTimeGetter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*pq.NullTime)(p)
	}
}

//--------------------------------------------------------------------------

func makeTimeGetter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*time.Time)(p)
	}
}

func makeByteSliceGetter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*[]byte)(p)
	}
}

//--------------------------------------------------------------------------

func makeBoolGetter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*bool)(p)
	}
}

func makeStringGetter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*string)(p)
	}
}

//--------------------------------------------------------------------------

func makeFloat32Getter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*float32)(p)
	}
}

func makeFloat64Getter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*float64)(p)
	}
}

//--------------------------------------------------------------------------

func makeIntGetter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*int)(p)
	}
}

func makeInt8Getter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*int8)(p)
	}
}

func makeInt16Getter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*int16)(p)
	}
}

func makeInt32Getter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*int32)(p)
	}
}

func makeInt64Getter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*int64)(p)
	}
}

//--------------------------------------------------------------------------

func makeUintGetter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*uint)(p)
	}
}

func makeUint8Getter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*uint8)(p)
	}
}

func makeUint16Getter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*uint16)(p)
	}
}

func makeUint32Getter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*uint32)(p)
	}
}

func makeUint64Getter(offset uintptr) StructFieldGetterFunc {
	return func(structPtr unsafe.Pointer, ifacePtr *interface{}) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		*ifacePtr = *(*uint64)(p)
	}
}
