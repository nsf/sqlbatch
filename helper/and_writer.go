package helper

import (
	"strings"
)

type AndWriter struct {
	sb    *strings.Builder
	empty bool
}

func NewAndWriter(sb *strings.Builder) AndWriter {
	return AndWriter{
		sb:    sb,
		empty: true,
	}
}

func (lw *AndWriter) WriteString(s string) {
	if !lw.empty {
		lw.sb.WriteString(" AND ")
	} else {
		lw.empty = false
	}
	lw.sb.WriteString(s)
}

func (lw *AndWriter) Next() *strings.Builder {
	if !lw.empty {
		lw.sb.WriteString(" AND ")
	} else {
		lw.empty = false
	}
	return lw.sb
}
