package sqlbatch

import (
	"reflect"
	"strings"
	"unsafe"
)

// Bulk (in)serter or (up)serter
type BulkSerter struct {
	nestedBuilder
	command string // INSERT or UPSERT
	builder strings.Builder
	si      *StructInfo
}

func (b *BulkSerter) writeHeader() {
	sb := &b.builder
	sb.WriteString(b.command + " INTO ")
	sb.WriteString(b.si.QuotedName)
	sb.WriteString(" (")
	fieldNamesWriter := newListWriter(sb)
	for _, f := range b.si.Fields {
		fieldNamesWriter.WriteString(f.QuotedName)
	}
	sb.WriteString(") VALUES ")
}

func (b *BulkSerter) AddMany(v interface{}) *BulkSerter {
	b.assertNotCommitted()

	sliceVal := reflect.ValueOf(v)
	t := assertSliceOfStructs(sliceVal.Type())

	sliceLen := sliceVal.Len()
	if sliceLen == 0 {
		return b
	}
	structSize := t.Size()

	ptr := unsafe.Pointer(sliceVal.Pointer())
	si := GetStructInfo(t, b.b.customFieldInterfaceResolver)

	if b.si != nil && b.si != si {
		panic("mismatching struct type on subsequent BulkSerter method calls")
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
		writeFieldValues(si, ptr, sb, b.b.timeNow(), insert)
		sb.WriteString(")")
		if i != sliceLen-1 {
			sb.WriteString(", ")
		}
		ptr = unsafe.Pointer(uintptr(ptr) + structSize)
	}
	return b
}

func (b *BulkSerter) Add(v interface{}) *BulkSerter {
	b.assertNotCommitted()

	structVal := reflect.ValueOf(v)
	t := assertPointerToStruct(structVal.Type())

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, b.b.customFieldInterfaceResolver)

	if b.si != nil && b.si != si {
		panic("mismatching struct type on subsequent BulkSerter method calls")
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
	writeFieldValues(si, ptr, sb, b.b.timeNow(), b.command == "INSERT")
	sb.WriteString(")")
	return b
}

func (b *BulkSerter) Commit() *Batch {
	b.assertNotCommitted()

	b.builder.WriteString(" RETURNING NOTHING")

	sb := b.b.beginNextStmt()
	sb.WriteString(b.builder.String())
	b.release()
	return b.b
}

func (b *Batch) BulkInserter() *BulkSerter {
	return &BulkSerter{command: "INSERT", nestedBuilder: b.allocateNestedBuilder("BulkInserter")}
}

func (b *Batch) WithBulkInserter(cb func(bulk *BulkSerter)) *Batch {
	bulk := b.BulkInserter()
	cb(bulk)
	bulk.Commit()
	return b
}

func (b *Batch) BulkInsert(v interface{}) *Batch {
	return b.BulkInserter().AddMany(v).Commit()
}

func (b *Batch) BulkUpserter() *BulkSerter {
	return &BulkSerter{command: "UPSERT", nestedBuilder: b.allocateNestedBuilder("BulkUpserter")}
}

func (b *Batch) WithBulkUpserter(cb func(bulk *BulkSerter)) *Batch {
	bulk := b.BulkUpserter()
	cb(bulk)
	bulk.Commit()
	return b
}

func (b *Batch) BulkUpsert(v interface{}) *Batch {
	return b.BulkUpserter().AddMany(v).Commit()
}
