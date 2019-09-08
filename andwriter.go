package sqlbatch

import (
	"strings"
)

type andWriter struct {
	sb    *strings.Builder
	empty bool
}

func newAndWriter(sb *strings.Builder) andWriter {
	return andWriter{
		sb:    sb,
		empty: true,
	}
}

func (lw *andWriter) WriteString(s string) {
	if !lw.empty {
		lw.sb.WriteString(" AND ")
	} else {
		lw.empty = false
	}
	lw.sb.WriteString(s)
}

func (lw *andWriter) Next() *strings.Builder {
	if !lw.empty {
		lw.sb.WriteString(" AND ")
	} else {
		lw.empty = false
	}
	return lw.sb
}
