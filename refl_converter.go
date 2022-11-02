package sqlbatch

import (
	"database/sql"
	"github.com/lib/pq"
	"github.com/nsf/sqlbatch/util"
	"strings"
	"time"
)

func nullBoolConverter(iface any, b *strings.Builder) {
	val := iface.(sql.NullBool)
	util.AppendBool(b, val.Bool, !val.Valid)
}

func nullFloat64Converter(iface any, b *strings.Builder) {
	val := iface.(sql.NullFloat64)
	util.AppendFloat64(b, val.Float64, !val.Valid)
}

func nullInt64Converter(iface any, b *strings.Builder) {
	val := iface.(sql.NullInt64)
	util.AppendInt64(b, val.Int64, !val.Valid)
}

func nullStringConverter(iface any, b *strings.Builder) {
	val := iface.(sql.NullString)
	util.AppendString(b, val.String, !val.Valid)
}

func nullTimeConverter(iface any, b *strings.Builder) {
	val := iface.(pq.NullTime)
	util.AppendTime(b, val.Time, !val.Valid)
}

//--------------------------------------------------------------------------

func timeConverter(iface any, b *strings.Builder) {
	val := iface.(time.Time)
	util.AppendTime(b, val, false)
}

func byteSliceConverter(iface any, b *strings.Builder) {
	val := iface.([]byte)
	util.AppendByteSlice(b, val, val == nil)
}

//--------------------------------------------------------------------------

func boolConverter(iface any, b *strings.Builder) {
	val := iface.(bool)
	util.AppendBool(b, val, false)
}

func stringConverter(iface any, b *strings.Builder) {
	val := iface.(string)
	util.AppendString(b, val, false)
}

//--------------------------------------------------------------------------

func float32Converter(iface any, b *strings.Builder) {
	val := iface.(float32)
	util.AppendFloat64(b, float64(val), false)
}

func float64Converter(iface any, b *strings.Builder) {
	val := iface.(float64)
	util.AppendFloat64(b, val, false)
}

//--------------------------------------------------------------------------

func genericIntConverter[T ~int | ~int8 | ~int16 | ~int32 | ~int64](iface any, b *strings.Builder) {
	val := iface.(T)
	util.AppendInt64(b, int64(val), false)
}

//--------------------------------------------------------------------------

func genericUintConverter[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](iface any, b *strings.Builder) {
	val := iface.(T)
	util.AppendUint64(b, uint64(val), false)
}

//--------------------------------------------------------------------------

func int64SliceConverter(iface any, b *strings.Builder) {
	val := iface.([]int64)
	for i, v := range val {
		util.AppendInt64(b, v, false)
		if i != len(val)-1 {
			b.WriteString(", ")
		}
	}
}

func stringSliceConverter(iface any, b *strings.Builder) {
	val := iface.([]string)
	for i, v := range val {
		util.AppendString(b, v, false)
		if i != len(val)-1 {
			b.WriteString(", ")
		}
	}
}
