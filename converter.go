package sqlbatch

import (
	"database/sql"
	"github.com/lib/pq"
	"strings"
	"time"
)

func nullBoolConverter(iface interface{}, b *strings.Builder) {
	val := iface.(sql.NullBool)
	AppendBool(b, val.Bool, !val.Valid)
}

func nullFloat64Converter(iface interface{}, b *strings.Builder) {
	val := iface.(sql.NullFloat64)
	AppendFloat64(b, val.Float64, !val.Valid)
}

func nullInt64Converter(iface interface{}, b *strings.Builder) {
	val := iface.(sql.NullInt64)
	AppendInt64(b, val.Int64, !val.Valid)
}

func nullStringConverter(iface interface{}, b *strings.Builder) {
	val := iface.(sql.NullString)
	AppendString(b, val.String, !val.Valid)
}

func nullTimeConverter(iface interface{}, b *strings.Builder) {
	val := iface.(pq.NullTime)
	AppendTime(b, val.Time, !val.Valid)
}

//--------------------------------------------------------------------------

func timeConverter(iface interface{}, b *strings.Builder) {
	val := iface.(time.Time)
	AppendTime(b, val, false)
}

func byteSliceConverter(iface interface{}, b *strings.Builder) {
	val := iface.([]byte)
	AppendByteSlice(b, val, false)
}

//--------------------------------------------------------------------------

func boolConverter(iface interface{}, b *strings.Builder) {
	val := iface.(bool)
	AppendBool(b, val, false)
}

func stringConverter(iface interface{}, b *strings.Builder) {
	val := iface.(string)
	AppendString(b, val, false)
}

//--------------------------------------------------------------------------

func float32Converter(iface interface{}, b *strings.Builder) {
	val := iface.(float32)
	AppendFloat64(b, float64(val), false)
}

func float64Converter(iface interface{}, b *strings.Builder) {
	val := iface.(float64)
	AppendFloat64(b, val, false)
}

//--------------------------------------------------------------------------

func intConverter(iface interface{}, b *strings.Builder) {
	val := iface.(int)
	AppendInt64(b, int64(val), false)
}

func int8Converter(iface interface{}, b *strings.Builder) {
	val := iface.(int8)
	AppendInt64(b, int64(val), false)
}

func int16Converter(iface interface{}, b *strings.Builder) {
	val := iface.(int16)
	AppendInt64(b, int64(val), false)
}

func int32Converter(iface interface{}, b *strings.Builder) {
	val := iface.(int32)
	AppendInt64(b, int64(val), false)
}

func int64Converter(iface interface{}, b *strings.Builder) {
	val := iface.(int64)
	AppendInt64(b, val, false)
}

//--------------------------------------------------------------------------

func uintConverter(iface interface{}, b *strings.Builder) {
	val := iface.(uint)
	AppendUint64(b, uint64(val), false)
}

func uint8Converter(iface interface{}, b *strings.Builder) {
	val := iface.(uint8)
	AppendUint64(b, uint64(val), false)
}

func uint16Converter(iface interface{}, b *strings.Builder) {
	val := iface.(uint16)
	AppendUint64(b, uint64(val), false)
}

func uint32Converter(iface interface{}, b *strings.Builder) {
	val := iface.(uint32)
	AppendUint64(b, uint64(val), false)
}

func uint64Converter(iface interface{}, b *strings.Builder) {
	val := iface.(uint64)
	AppendUint64(b, uint64(val), false)
}

//--------------------------------------------------------------------------

func int64SliceConverter(iface interface{}, b *strings.Builder) {
	val := iface.([]int64)
	for i, v := range val {
		AppendInt64(b, v, false)
		if i != len(val)-1 {
			b.WriteString(", ")
		}
	}
}

func stringSliceConverter(iface interface{}, b *strings.Builder) {
	val := iface.([]string)
	for i, v := range val {
		AppendString(b, v, false)
		if i != len(val)-1 {
			b.WriteString(", ")
		}
	}
}
