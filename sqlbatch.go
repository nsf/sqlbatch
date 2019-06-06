package sqlbatch

import (
	"github.com/lib/pq"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

var (
	CustomFieldInterfaceResolver FieldInterfaceResolver
	TimeNowFunc                  = time.Now
)

type Batch struct {
	stmtBuilder         strings.Builder
	liveNestedBuilders  map[int]struct{}
	nextNestedBuilderID int
	now                 time.Time
}

func New() *Batch {
	return &Batch{now: TimeNowFunc()}
}

func assertPointerToStruct(t reflect.Type) reflect.Type {
	if t.Kind() != reflect.Ptr {
		panic("pointer to struct expected")
	}
	t = t.Elem()
	if t.Kind() != reflect.Struct {
		panic("pointer to struct expected")
	}
	return t
}

func assertHasPrimaryKeys(si *StructInfo) {
	if len(si.PrimaryKeys) == 0 {
		panic("struct has no primary keys defined")
	}
}

func writePrimaryKeysWhereCondition(si *StructInfo, ptr unsafe.Pointer, sb *strings.Builder) {
	pkWriter := newListWriter(sb)
	for _, f := range si.PrimaryKeys {
		b := pkWriter.Next()
		b.WriteString(f.QuotedName)
		b.WriteString(" = ")
		f.Interface.Write(ptr, b)
	}
	sb.WriteString(" RETURNING NOTHING")
}

func writeFieldValues(si *StructInfo, ptr unsafe.Pointer, sb *strings.Builder, now time.Time) {
	fieldValuesWriter := newListWriter(sb)
	for _, f := range si.Fields {
		if f.IsCreated() || f.IsUpdated() {
			setTime(&f, ptr, now)
		}
		f.Interface.Write(ptr, fieldValuesWriter.Next())
	}
}

func setTime(f *FieldInfo, ptr unsafe.Pointer, t time.Time) {
	if f.IsNull() {
		f.Interface.Set(ptr, pq.NullTime{Valid: true, Time: t})
	} else {
		f.Interface.Set(ptr, t)
	}
}

func (b *Batch) beginNextStmt() *strings.Builder {
	sb := &b.stmtBuilder
	if sb.Len() != 0 {
		sb.WriteString("; ")
	}
	return sb
}

type BulkInserter struct {
	id      int
	b       *Batch
	builder strings.Builder
	si      *StructInfo
}

func (b *BulkInserter) Add(v interface{}) *BulkInserter {
	if b.id == -1 {
		panic("BulkInserter already committed")
	}

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
	b.b.releaseNestedBuilderID(&b.id)
}

func (b *Batch) releaseNestedBuilderID(v *int) {
	delete(b.liveNestedBuilders, *v)
	*v = -1
}

func (b *Batch) allocateNestedBuilderID() int {
	id := b.nextNestedBuilderID
	b.nextNestedBuilderID++
	if b.liveNestedBuilders == nil {
		b.liveNestedBuilders = map[int]struct{}{}
	}
	b.liveNestedBuilders[id] = struct{}{}
	return id
}

func (b *Batch) BulkInserter() *BulkInserter {
	return &BulkInserter{b: b, id: b.allocateNestedBuilderID()}
}

func (b *Batch) WithBulkInserter(cb func(bulk *BulkInserter)) *Batch {
	bulk := b.BulkInserter()
	cb(bulk)
	bulk.Commit()
	return b
}

func (b *Batch) Insert(v interface{}) *Batch {
	structVal := reflect.ValueOf(v)
	t := assertPointerToStruct(structVal.Type())

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, CustomFieldInterfaceResolver)

	sb := b.beginNextStmt()
	sb.WriteString("INSERT INTO ")
	sb.WriteString(si.QuotedName)
	sb.WriteString(" (")
	fieldNamesWriter := newListWriter(sb)
	for _, f := range si.Fields {
		fieldNamesWriter.WriteString(f.QuotedName)
	}
	sb.WriteString(") VALUES (")
	writeFieldValues(si, ptr, sb, b.now)
	sb.WriteString(") RETURNING NOTHING")
	return b
}

func (b *Batch) Update(v interface{}) *Batch {
	structVal := reflect.ValueOf(v)
	t := assertPointerToStruct(structVal.Type())

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, CustomFieldInterfaceResolver)
	assertHasPrimaryKeys(si)

	sb := b.beginNextStmt()
	sb.WriteString("UPDATE ")
	sb.WriteString(si.QuotedName)
	sb.WriteString(" SET ")
	valsWriter := newListWriter(sb)
	for _, f := range si.NonPrimaryKeys {
		if f.IsUpdated() {
			setTime(f, ptr, b.now)
		}
		b := valsWriter.Next()
		b.WriteString(f.QuotedName)
		b.WriteString(" = ")
		f.Interface.Write(ptr, b)
	}
	sb.WriteString(" WHERE ")
	writePrimaryKeysWhereCondition(si, ptr, sb)
	return b
}

func (b *Batch) Delete(v interface{}) *Batch {
	structVal := reflect.ValueOf(v)
	t := assertPointerToStruct(structVal.Type())

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, CustomFieldInterfaceResolver)
	assertHasPrimaryKeys(si)

	sb := b.beginNextStmt()
	sb.WriteString("DELETE FROM ")
	sb.WriteString(si.QuotedName)
	sb.WriteString(" WHERE ")
	writePrimaryKeysWhereCondition(si, ptr, sb)
	return b
}

func (b *Batch) Query() string {
	return b.stmtBuilder.String()
}
