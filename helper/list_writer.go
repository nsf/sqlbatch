package helper

import (
	"strings"
)

type ListWriter struct {
	sb    *strings.Builder
	empty bool
}

func NewListWriter(sb *strings.Builder) ListWriter {
	return ListWriter{
		sb:    sb,
		empty: true,
	}
}

func (lw *ListWriter) WriteString(s string) {
	if !lw.empty {
		lw.sb.WriteString(", ")
	} else {
		lw.empty = false
	}
	lw.sb.WriteString(s)
}

func (lw *ListWriter) Next() *strings.Builder {
	if !lw.empty {
		lw.sb.WriteString(", ")
	} else {
		lw.empty = false
	}
	return lw.sb
}
