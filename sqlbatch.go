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
	liveNestedBuilders  map[int]string
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
