package sqlbatch

import (
	"strings"
)

type listWriter struct {
	sb    *strings.Builder
	empty bool
}

func newListWriter(sb *strings.Builder) listWriter {
	return listWriter{
		sb:    sb,
		empty: true,
	}
}

func (lw *listWriter) WriteString(s string) {
	if !lw.empty {
		lw.sb.WriteString(", ")
	} else {
		lw.empty = false
	}
	lw.sb.WriteString(s)
}

func (lw *listWriter) Next() *strings.Builder {
	if !lw.empty {
		lw.sb.WriteString(", ")
	} else {
		lw.empty = false
	}
	return lw.sb
}
