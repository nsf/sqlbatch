package sqlbatch

import (
	"database/sql"
	"github.com/lib/pq"
	"strings"
	"time"
)

func nullBoolConverter(iface interface{}, b *strings.Builder) {
	val := iface.(sql.NullBool)
	appendBool(b, val.Bool, !val.Valid)
}

func nullFloat64Converter(iface interface{}, b *strings.Builder) {
	val := iface.(sql.NullFloat64)
	appendFloat64(b, val.Float64, !val.Valid)
}

func nullInt64Converter(iface interface{}, b *strings.Builder) {
	val := iface.(sql.NullInt64)
	appendInt64(b, val.Int64, !val.Valid)
}

func nullStringConverter(iface interface{}, b *strings.Builder) {
	val := iface.(sql.NullString)
	appendString(b, val.String, !val.Valid)
}

func nullTimeConverter(iface interface{}, b *strings.Builder) {
	val := iface.(pq.NullTime)
	appendTime(b, val.Time, !val.Valid)
}

//--------------------------------------------------------------------------

func timeConverter(iface interface{}, b *strings.Builder) {
	val := iface.(time.Time)
	appendTime(b, val, false)
}

func byteSliceConverter(iface interface{}, b *strings.Builder) {
	val := iface.([]byte)
	appendByteSlice(b, val, false)
}

//--------------------------------------------------------------------------

func boolConverter(iface interface{}, b *strings.Builder) {
	val := iface.(bool)
	appendBool(b, val, false)
}

func stringConverter(iface interface{}, b *strings.Builder) {
	val := iface.(string)
	appendString(b, val, false)
}

//--------------------------------------------------------------------------

func float32Converter(iface interface{}, b *strings.Builder) {
	val := iface.(float32)
	appendFloat64(b, float64(val), false)
}

func float64Converter(iface interface{}, b *strings.Builder) {
	val := iface.(float64)
	appendFloat64(b, val, false)
}

//--------------------------------------------------------------------------

func intConverter(iface interface{}, b *strings.Builder) {
	val := iface.(int)
	appendInt64(b, int64(val), false)
}

func int8Converter(iface interface{}, b *strings.Builder) {
	val := iface.(int8)
	appendInt64(b, int64(val), false)
}

func int16Converter(iface interface{}, b *strings.Builder) {
	val := iface.(int16)
	appendInt64(b, int64(val), false)
}

func int32Converter(iface interface{}, b *strings.Builder) {
	val := iface.(int32)
	appendInt64(b, int64(val), false)
}

func int64Converter(iface interface{}, b *strings.Builder) {
	val := iface.(int64)
	appendInt64(b, val, false)
}

//--------------------------------------------------------------------------

func uintConverter(iface interface{}, b *strings.Builder) {
	val := iface.(uint)
	appendUint64(b, uint64(val), false)
}

func uint8Converter(iface interface{}, b *strings.Builder) {
	val := iface.(uint8)
	appendUint64(b, uint64(val), false)
}

func uint16Converter(iface interface{}, b *strings.Builder) {
	val := iface.(uint16)
	appendUint64(b, uint64(val), false)
}

func uint32Converter(iface interface{}, b *strings.Builder) {
	val := iface.(uint32)
	appendUint64(b, uint64(val), false)
}

func uint64Converter(iface interface{}, b *strings.Builder) {
	val := iface.(uint64)
	appendUint64(b, uint64(val), false)
}

//--------------------------------------------------------------------------

func int64SliceConverter(iface interface{}, b *strings.Builder) {
	val := iface.([]int64)
	for i, v := range val {
		appendInt64(b, v, false)
		if i != len(val)-1 {
			b.WriteString(", ")
		}
	}
}

func stringSliceConverter(iface interface{}, b *strings.Builder) {
	val := iface.([]string)
	for i, v := range val {
		appendString(b, v, false)
		if i != len(val)-1 {
			b.WriteString(", ")
		}
	}
}
