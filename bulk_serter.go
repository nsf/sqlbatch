package sqlbatch

import (
	"github.com/nsf/sqlbatch/helper"
	"reflect"
	"strings"
	"unsafe"
)

// Bulk (in)serter or (up)serter
type bulkSerter struct {
	command  string // INSERT or UPSERT
	builder  strings.Builder
	si       *StructInfo
	b        *Batch
	nonEmpty bool
}

func (b *bulkSerter) writeHeader() {
	sb := &b.builder
	sb.WriteString(b.command + " INTO ")
	sb.WriteString(b.si.QuotedName)
	sb.WriteString(" (")
	fieldNamesWriter := helper.NewListWriter(sb)
	for _, f := range b.si.Fields {
		fieldNamesWriter.WriteString(f.QuotedName)
	}
	sb.WriteString(") VALUES ")
}

func (b *bulkSerter) addMany(v any) *bulkSerter {
	sliceVal := reflect.ValueOf(v)
	t := assertSliceOfStructs(sliceVal.Type())

	sliceLen := sliceVal.Len()
	if sliceLen == 0 {
		return b
	}
	b.nonEmpty = true
	structSize := t.Size()

	ptr := unsafe.Pointer(sliceVal.Pointer())
	si := GetStructInfo(t, b.b.customFieldInterfaceResolver)

	if b.si != nil && b.si != si {
		panic("mismatching struct type on subsequent bulkSerter method calls")
	}

	sb := &b.builder
	if b.si == nil {
		b.si = si
		b.writeHeader()
	} else {
		sb.WriteString(", ")
	}

	insert := b.command == "INSERT"
	for i := 0; i < sliceLen; i++ {
		sb.WriteString("(")
		writeFieldValues(si, unsafe.Pointer(uintptr(ptr)+structSize*uintptr(i)), sb, b.b.timeNow(), insert)
		sb.WriteString(")")
		if i != sliceLen-1 {
			sb.WriteString(", ")
		}
	}
	return b
}

func (b *bulkSerter) commit() *Batch {
	if !b.nonEmpty {
		return b.b
	}
	b.builder.WriteString(" RETURNING NOTHING")

	sb := b.b.beginNextStmt()
	sb.WriteString(b.builder.String())
	return b.b
}
