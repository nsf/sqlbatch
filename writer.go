package sqlbatch

import (
	"database/sql"
	"encoding/hex"
	"github.com/lib/pq"
	"math"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// Here we describe how to format all driver.Value variants.

func makeNullBoolWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*sql.NullBool)(p)
		appendBool(b, val.Bool, !val.Valid)
	}
}

func makeNullFloat64Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*sql.NullFloat64)(p)
		appendFloat64(b, val.Float64, !val.Valid)
	}
}

func makeNullInt64Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*sql.NullInt64)(p)
		appendInt64(b, val.Int64, !val.Valid)
	}
}

func makeNullStringWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*sql.NullString)(p)
		appendString(b, val.String, !val.Valid)
	}
}

func makeNullTimeWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*pq.NullTime)(p)
		appendTime(b, val.Time, !val.Valid)
	}
}

//--------------------------------------------------------------------------

func makeTimeWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*time.Time)(p)
		appendTime(b, val, false)
	}
}

func makeByteSliceWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*[]byte)(p)
		appendByteSlice(b, val, false)
	}
}

//--------------------------------------------------------------------------

func makeBoolWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*bool)(p)
		appendBool(b, val, false)
	}
}

func makeStringWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*string)(p)
		appendString(b, val, false)
	}
}

//--------------------------------------------------------------------------

func makeFloat32Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*float32)(p)
		appendFloat64(b, float64(val), false)
	}
}

func makeFloat64Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*float64)(p)
		appendFloat64(b, val, false)
	}
}

//--------------------------------------------------------------------------

func makeIntWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*int)(p)
		appendInt64(b, int64(val), false)
	}
}

func makeInt8Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*int8)(p)
		appendInt64(b, int64(val), false)
	}
}

func makeInt16Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*int16)(p)
		appendInt64(b, int64(val), false)
	}
}

func makeInt32Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*int32)(p)
		appendInt64(b, int64(val), false)
	}
}

func makeInt64Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*int64)(p)
		appendInt64(b, val, false)
	}
}

//--------------------------------------------------------------------------

func makeUintWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*uint)(p)
		appendInt64(b, int64(val), false)
	}
}

func makeUint8Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*uint8)(p)
		appendInt64(b, int64(val), false)
	}
}

func makeUint16Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*uint16)(p)
		appendInt64(b, int64(val), false)
	}
}

func makeUint32Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*uint32)(p)
		appendInt64(b, int64(val), false)
	}
}

func makeUint64Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*uint64)(p)
		appendInt64(b, int64(val), false)
	}
}

//--------------------------------------------------------------------------

// time.Time
func appendTime(b *strings.Builder, t time.Time, isNull bool) {
	if isNull {
		b.WriteString("NULL")
	} else {
		b.WriteString("TIMESTAMP '")
		b.WriteString(t.UTC().Format("2006-01-02 15:04:05.999999"))
		b.WriteString("'")
	}
}

// int64
func appendInt64(b *strings.Builder, v int64, isNull bool) {
	if isNull {
		b.WriteString("NULL")
	} else {
		b.WriteString(strconv.FormatInt(v, 10))
	}
}

// float64
func appendFloat64(b *strings.Builder, v float64, isNull bool) {
	if isNull {
		b.WriteString("NULL")
	} else {
		switch {
		case math.IsNaN(v):
			b.WriteString("nan")
		case math.IsInf(v, 1):
			b.WriteString("inf")
		case math.IsInf(v, -1):
			b.WriteString("-inf")
		default:
			b.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
		}
	}
}

// bool
func appendBool(b *strings.Builder, v bool, isNull bool) {
	if isNull {
		b.WriteString("NULL")
	} else {
		if v {
			b.WriteString("TRUE")
		} else {
			b.WriteString("FALSE")
		}
	}
}

// []byte
func appendByteSlice(b *strings.Builder, v []byte, isNull bool) {
	if isNull {
		b.WriteString("NULL")
	} else {
		b.WriteString(`'\x`)
		b.WriteString(hex.EncodeToString(v))
		b.WriteString(`'`)
	}
}

// string
func appendString(b *strings.Builder, v string, isNull bool) {
	if isNull {
		b.WriteString("NULL")
	} else {
		b.WriteString(`'`)
		for i := 0; i < len(v); i++ {
			c := v[i]
			if c == 0 { // zero bytes are not supported, skip it
				continue
			} else if c == '\'' {
				b.WriteString(`''`)
			} else {
				b.WriteByte(c)
			}
		}
		b.WriteString(`'`)
	}
}
