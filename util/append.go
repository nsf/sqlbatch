package util

import (
	"encoding/hex"
	"math"
	"strconv"
	"strings"
	"time"
)

// time.Time
func AppendTime(b *strings.Builder, t time.Time, isNull bool) {
	if isNull {
		b.WriteString("NULL")
	} else {
		b.WriteString("'")
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
