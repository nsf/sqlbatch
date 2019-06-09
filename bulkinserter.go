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

func (b *BulkInserter) writeHeader() {
	sb := &b.builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(b.si.QuotedName)
	sb.WriteString(" (")
	fieldNamesWriter := newListWriter(sb)
	for _, f := range b.si.Fields {
		fieldNamesWriter.WriteString(f.QuotedName)
	}
	sb.WriteString(") VALUES ")
}

func (b *BulkInserter) AddMany(v interface{}) *BulkInserter {
	b.assertNotCommitted()

	sliceVal := reflect.ValueOf(v)
	t := assertSliceOfStructs(sliceVal.Type())

	sliceLen := sliceVal.Len()
	if sliceLen == 0 {
		return b
	}
	structSize := t.Size()

	ptr := unsafe.Pointer(sliceVal.Pointer())
	si := GetStructInfo(t, CustomFieldInterfaceResolver)

	if b.si != nil && b.si != si {
		panic("mismatching struct type on subsequent BulkInserter method calls")
	}

	sb := &b.builder
	if b.si == nil {
		b.si = si
		b.writeHeader()
	} else {
		sb.WriteString(", ")
	}

	for i := 0; i < sliceLen; i++ {
		sb.WriteString("(")
		writeFieldValues(si, ptr, sb, b.b.now)
		sb.WriteString(")")
		if i != sliceLen-1 {
			sb.WriteString(", ")
		}
		ptr = unsafe.Pointer(uintptr(ptr) + structSize)
	}
	return b
}

func (b *BulkInserter) Add(v interface{}) *BulkInserter {
	b.assertNotCommitted()

	structVal := reflect.ValueOf(v)
	t := assertPointerToStruct(structVal.Type())

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, CustomFieldInterfaceResolver)

	if b.si != nil && b.si != si {
		panic("mismatching struct type on subsequent BulkInserter method calls")
	}

	sb := &b.builder
	if b.si == nil {
		b.si = si
		// first call, start bulk insert statement
		b.writeHeader()
	} else {
		sb.WriteString(", ")
	}

	sb.WriteString("(")
	writeFieldValues(si, ptr, sb, b.b.now)
	sb.WriteString(")")
	return b
}

func (b *BulkInserter) Commit() *Batch {
	b.assertNotCommitted()

	b.builder.WriteString(" RETURNING NOTHING")

	sb := b.b.beginNextStmt()
	sb.WriteString(b.builder.String())
	b.release()
	return b.b
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

func (b *Batch) BulkInsert(v interface{}) *Batch {
	return b.BulkInserter().AddMany(v).Commit()
}
