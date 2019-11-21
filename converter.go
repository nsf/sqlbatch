package sqlbatch

import (
	"database/sql"
	"github.com/lib/pq"
	"github.com/nsf/sqlbatch/util"
	"strings"
	"time"
)

func nullBoolConverter(iface interface{}, b *strings.Builder) {
	val := iface.(sql.NullBool)
	util.AppendBool(b, val.Bool, !val.Valid)
}

func nullFloat64Converter(iface interface{}, b *strings.Builder) {
	val := iface.(sql.NullFloat64)
	util.AppendFloat64(b, val.Float64, !val.Valid)
}

func nullInt64Converter(iface interface{}, b *strings.Builder) {
	val := iface.(sql.NullInt64)
	util.AppendInt64(b, val.Int64, !val.Valid)
}

func nullStringConverter(iface interface{}, b *strings.Builder) {
	val := iface.(sql.NullString)
	util.AppendString(b, val.String, !val.Valid)
}

func nullTimeConverter(iface interface{}, b *strings.Builder) {
	val := iface.(pq.NullTime)
	util.AppendTime(b, val.Time, !val.Valid)
}

//--------------------------------------------------------------------------

func timeConverter(iface interface{}, b *strings.Builder) {
	val := iface.(time.Time)
	util.AppendTime(b, val, false)
}

func byteSliceConverter(iface interface{}, b *strings.Builder) {
	val := iface.([]byte)
	util.AppendByteSlice(b, val, false)
}

//--------------------------------------------------------------------------

func boolConverter(iface interface{}, b *strings.Builder) {
	val := iface.(bool)
	util.AppendBool(b, val, false)
}

func stringConverter(iface interface{}, b *strings.Builder) {
	val := iface.(string)
	util.AppendString(b, val, false)
}

//--------------------------------------------------------------------------

func float32Converter(iface interface{}, b *strings.Builder) {
	val := iface.(float32)
	util.AppendFloat64(b, float64(val), false)
}

func float64Converter(iface interface{}, b *strings.Builder) {
	val := iface.(float64)
	util.AppendFloat64(b, val, false)
}

//--------------------------------------------------------------------------

func intConverter(iface interface{}, b *strings.Builder) {
	val := iface.(int)
	util.AppendInt64(b, int64(val), false)
}

func int8Converter(iface interface{}, b *strings.Builder) {
	val := iface.(int8)
	util.AppendInt64(b, int64(val), false)
}

func int16Converter(iface interface{}, b *strings.Builder) {
	val := iface.(int16)
	util.AppendInt64(b, int64(val), false)
}

func int32Converter(iface interface{}, b *strings.Builder) {
	val := iface.(int32)
	util.AppendInt64(b, int64(val), false)
}

func int64Converter(iface interface{}, b *strings.Builder) {
	val := iface.(int64)
	util.AppendInt64(b, val, false)
}

//--------------------------------------------------------------------------

func uintConverter(iface interface{}, b *strings.Builder) {
	val := iface.(uint)
	util.AppendUint64(b, uint64(val), false)
}

func uint8Converter(iface interface{}, b *strings.Builder) {
	val := iface.(uint8)
	util.AppendUint64(b, uint64(val), false)
}

func uint16Converter(iface interface{}, b *strings.Builder) {
	val := iface.(uint16)
	util.AppendUint64(b, uint64(val), false)
}

func uint32Converter(iface interface{}, b *strings.Builder) {
	val := iface.(uint32)
	util.AppendUint64(b, uint64(val), false)
}

func uint64Converter(iface interface{}, b *strings.Builder) {
	val := iface.(uint64)
	util.AppendUint64(b, uint64(val), false)
}

//--------------------------------------------------------------------------

func int64SliceConverter(iface interface{}, b *strings.Builder) {
	val := iface.([]int64)
	for i, v := range val {
		util.AppendInt64(b, v, false)
		if i != len(val)-1 {
			b.WriteString(", ")
		}
	}
}

func stringSliceConverter(iface interface{}, b *strings.Builder) {
	val := iface.([]string)
	for i, v := range val {
		util.AppendString(b, v, false)
		if i != len(val)-1 {
			b.WriteString(", ")
		}
	}
}
