package sqlbatch

import (
	"database/sql"
	"github.com/lib/pq"
	"github.com/nsf/sqlbatch/util"
	"strings"
	"time"
	"unsafe"
)

// Here we describe how to format all driver.Value variants.

func makeNullBoolWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*sql.NullBool)(p)
		util.AppendBool(b, val.Bool, !val.Valid)
	}
}

func makeNullFloat64Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*sql.NullFloat64)(p)
		util.AppendFloat64(b, val.Float64, !val.Valid)
	}
}

func makeNullInt64Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*sql.NullInt64)(p)
		util.AppendInt64(b, val.Int64, !val.Valid)
	}
}

func makeNullStringWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*sql.NullString)(p)
		util.AppendString(b, val.String, !val.Valid)
	}
}

func makeNullTimeWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*pq.NullTime)(p)
		util.AppendTime(b, val.Time, !val.Valid)
	}
}

//--------------------------------------------------------------------------

func makeTimeWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*time.Time)(p)
		util.AppendTime(b, val, false)
	}
}

func makeByteSliceWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*[]byte)(p)
		util.AppendByteSlice(b, val, false)
	}
}

//--------------------------------------------------------------------------

func makeBoolWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*bool)(p)
		util.AppendBool(b, val, false)
	}
}

func makeStringWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*string)(p)
		util.AppendString(b, val, false)
	}
}

//--------------------------------------------------------------------------

func makeFloat32Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*float32)(p)
		util.AppendFloat64(b, float64(val), false)
	}
}

func makeFloat64Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*float64)(p)
		util.AppendFloat64(b, val, false)
	}
}

//--------------------------------------------------------------------------

func makeIntWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*int)(p)
		util.AppendInt64(b, int64(val), false)
	}
}

func makeInt8Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*int8)(p)
		util.AppendInt64(b, int64(val), false)
	}
}

func makeInt16Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*int16)(p)
		util.AppendInt64(b, int64(val), false)
	}
}

func makeInt32Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*int32)(p)
		util.AppendInt64(b, int64(val), false)
	}
}

func makeInt64Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*int64)(p)
		util.AppendInt64(b, val, false)
	}
}

//--------------------------------------------------------------------------

func makeUintWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*uint)(p)
		util.AppendUint64(b, uint64(val), false)
	}
}

func makeUint8Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*uint8)(p)
		util.AppendUint64(b, uint64(val), false)
	}
}

func makeUint16Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*uint16)(p)
		util.AppendUint64(b, uint64(val), false)
	}
}

func makeUint32Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*uint32)(p)
		util.AppendUint64(b, uint64(val), false)
	}
}

func makeUint64Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*uint64)(p)
		util.AppendUint64(b, uint64(val), false)
	}
}

//--------------------------------------------------------------------------

func makeInt64SliceWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*[]int64)(p)
		for i, v := range val {
			util.AppendInt64(b, v, false)
			if i != len(val)-1 {
				b.WriteString(", ")
			}
		}
	}
}

func makeStringSliceWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*[]string)(p)
		for i, v := range val {
			util.AppendString(b, v, false)
			if i != len(val)-1 {
				b.WriteString(", ")
			}
		}
	}
}
