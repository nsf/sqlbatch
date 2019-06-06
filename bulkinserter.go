package sqlbatch

import (
	"reflect"
	"strings"
	"unsafe"
)

type BulkInserter struct {
	nestedBuilder
	builder strings.Builder
	si      *StructInfo
}

func (b *BulkInserter) Add(v interface{}) *BulkInserter {
	b.assertNotCommitted()

	structVal := reflect.ValueOf(v)
	t := assertPointerToStruct(structVal.Type())

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, CustomFieldInterfaceResolver)

	if b.si != nil && b.si != si {
		panic("mismatching struct type on subsequent BulkInserter.Insert() calls")
	}

	sb := &b.builder
	if b.si == nil {
		b.si = si
		// first call, start bulk insert statement
		sb.WriteString("INSERT INTO ")
		sb.WriteString(si.QuotedName)
		sb.WriteString(" (")
		fieldNamesWriter := newListWriter(sb)
		for _, f := range si.Fields {
			fieldNamesWriter.WriteString(f.QuotedName)
		}
		sb.WriteString(") VALUES ")
	} else {
		sb.WriteString(", ")
	}

	sb.WriteString("(")
	writeFieldValues(si, ptr, sb, b.b.now)
	sb.WriteString(")")
	return b
}

func (b *BulkInserter) Commit() {
	sb := b.b.beginNextStmt()
	sb.WriteString(b.builder.String())
	b.release()
}

func (b *Batch) BulkInserter() *BulkInserter {
	return &BulkInserter{nestedBuilder: b.allocateNestedBuilder("BulkInserter")}
}

func (b *Batch) WithBulkInserter(cb func(bulk *BulkInserter)) *Batch {
	bulk := b.BulkInserter()
	cb(bulk)
	bulk.Commit()
	return b
}
