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
		AppendBool(b, val.Bool, !val.Valid)
	}
}

func makeNullFloat64Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*sql.NullFloat64)(p)
		AppendFloat64(b, val.Float64, !val.Valid)
	}
}

func makeNullInt64Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*sql.NullInt64)(p)
		AppendInt64(b, val.Int64, !val.Valid)
	}
}

func makeNullStringWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*sql.NullString)(p)
		AppendString(b, val.String, !val.Valid)
	}
}

func makeNullTimeWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*pq.NullTime)(p)
		AppendTime(b, val.Time, !val.Valid)
	}
}

//--------------------------------------------------------------------------

func makeTimeWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*time.Time)(p)
		AppendTime(b, val, false)
	}
}

func makeByteSliceWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*[]byte)(p)
		AppendByteSlice(b, val, false)
	}
}

//--------------------------------------------------------------------------

func makeBoolWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*bool)(p)
		AppendBool(b, val, false)
	}
}

func makeStringWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*string)(p)
		AppendString(b, val, false)
	}
}

//--------------------------------------------------------------------------

func makeFloat32Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*float32)(p)
		AppendFloat64(b, float64(val), false)
	}
}

func makeFloat64Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*float64)(p)
		AppendFloat64(b, val, false)
	}
}

//--------------------------------------------------------------------------

func makeIntWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*int)(p)
		AppendInt64(b, int64(val), false)
	}
}

func makeInt8Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*int8)(p)
		AppendInt64(b, int64(val), false)
	}
}

func makeInt16Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*int16)(p)
		AppendInt64(b, int64(val), false)
	}
}

func makeInt32Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*int32)(p)
		AppendInt64(b, int64(val), false)
	}
}

func makeInt64Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*int64)(p)
		AppendInt64(b, val, false)
	}
}

//--------------------------------------------------------------------------

func makeUintWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*uint)(p)
		AppendUint64(b, uint64(val), false)
	}
}

func makeUint8Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*uint8)(p)
		AppendUint64(b, uint64(val), false)
	}
}

func makeUint16Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*uint16)(p)
		AppendUint64(b, uint64(val), false)
	}
}

func makeUint32Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*uint32)(p)
		AppendUint64(b, uint64(val), false)
	}
}

func makeUint64Writer(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*uint64)(p)
		AppendUint64(b, uint64(val), false)
	}
}

//--------------------------------------------------------------------------

func makeInt64SliceWriter(offset uintptr) func(structPtr unsafe.Pointer, b *strings.Builder) {
	return func(structPtr unsafe.Pointer, b *strings.Builder) {
		p := unsafe.Pointer(uintptr(structPtr) + offset)
		val := *(*[]int64)(p)
		for i, v := range val {
			AppendInt64(b, v, false)
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
			AppendString(b, v, false)
			if i != len(val)-1 {
				b.WriteString(", ")
			}
		}
	}
}

//--------------------------------------------------------------------------

// time.Time
func AppendTime(b *strings.Builder, t time.Time, isNull bool) {
	if isNull {
		b.WriteString("NULL")
	} else {
		b.WriteString("TIMESTAMP '")
		b.WriteString(t.UTC().Format("2006-01-02 15:04:05.999999"))
		b.WriteString("'")
	}
}

// int64
func AppendInt64(b *strings.Builder, v int64, isNull bool) {
	if isNull {
		b.WriteString("NULL")
	} else {
		b.WriteString(strconv.FormatInt(v, 10))
	}
}

// uint64
func AppendUint64(b *strings.Builder, v uint64, isNull bool) {
	if isNull {
		b.WriteString("NULL")
	} else {
		b.WriteString(strconv.FormatUint(v, 10))
	}
}

// float64
func AppendFloat64(b *strings.Builder, v float64, isNull bool) {
	if isNull {
		b.WriteString("NULL")
	} else {
		switch {
		case math.IsNaN(v):
			b.WriteString("'NaN'::FLOAT")
		case math.IsInf(v, 1):
			b.WriteString("'Inf'::FLOAT")
		case math.IsInf(v, -1):
			b.WriteString("'-Inf'::FLOAT")
		default:
			b.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
		}
	}
}

// bool
func AppendBool(b *strings.Builder, v bool, isNull bool) {
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
func AppendByteSlice(b *strings.Builder, v []byte, isNull bool) {
	if isNull {
		b.WriteString("NULL")
	} else {
		b.WriteString(`'\x`)
		b.WriteString(hex.EncodeToString(v))
		b.WriteString(`'`)
	}
}

// string
func AppendString(b *strings.Builder, v string, isNull bool) {
	if isNull {
		b.WriteString("NULL")
	} else {
		b.WriteString(`'`)
		for _, r := range v {
			if r == 0 {
				continue
			} else if r == '\'' {
				b.WriteString(`''`)
			} else {
				b.WriteRune(r)
			}
		}
		b.WriteString(`'`)
	}
}
