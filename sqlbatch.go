package sqlbatch

import (
	"context"
	"database/sql"
	"errors"
	"github.com/lib/pq"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

type Batch struct {
	stmtBuilder                  strings.Builder
	liveNestedBuilders           map[int]string
	nextNestedBuilderID          int
	timeNowFunc                  func() time.Time
	now                          time.Time
	readIntos                    []readInto
	customFieldInterfaceResolver FieldInterfaceResolver
}

func New() *Batch {
	return &Batch{}
}

func assertSliceOfStructs(t reflect.Type) reflect.Type {
	if t.Kind() != reflect.Slice {
		panic("slice of structs expected")
	}
	t = t.Elem()
	if t.Kind() != reflect.Struct {
		panic("slice of structs expected")
	}
	return t
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

func assertPointerToStructOrSliceOfStructs(t reflect.Type) (reflect.Type, bool) {
	isSlice := false
	switch t.Kind() {
	case reflect.Ptr:
		t = t.Elem()
		switch t.Kind() {
		case reflect.Struct:
			// do nothing
		default:
			panic("pointer to struct or slice of structs expected")
		}
	case reflect.Slice:
		isSlice = true
		t = t.Elem()
		if t.Kind() != reflect.Struct {
			panic("pointer to struct or slice of structs expected")
		}
	}
	return t, isSlice
}

func assertPointerToStructOrPointerToSliceOfStructs(t reflect.Type) (reflect.Type, bool) {
	isSlice := false
	if t.Kind() != reflect.Ptr {
		panic("pointer to struct or pointer to slice of structs expected")
	}
	t = t.Elem()
	switch t.Kind() {
	case reflect.Slice:
		isSlice = true
		t = t.Elem()
		if t.Kind() != reflect.Struct {
			panic("pointer to struct or pointer to slice of structs expected")
		}
	case reflect.Struct:
		// do nothing
	default:
		panic("pointer to struct or pointer to slice of structs expected")
	}
	return t, isSlice
}

func assertHasPrimaryKeys(si *StructInfo) {
	if len(si.PrimaryKeys) == 0 {
		panic("struct has no primary keys defined")
	}
}

func writePrimaryKeysWhereCondition(si *StructInfo, ptr unsafe.Pointer, sb *strings.Builder) {
	pkWriter := newAndWriter(sb)
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

func (b *Batch) timeNow() time.Time {
	if !b.now.IsZero() {
		return b.now
	}
	if b.timeNowFunc != nil {
		b.now = b.timeNowFunc()
	} else {
		b.now = time.Now()
	}
	return b.now
}

func (b *Batch) customResolver() FieldInterfaceResolver {
	return b.customFieldInterfaceResolver
}

func (b *Batch) beginNextStmt() *strings.Builder {
	sb := &b.stmtBuilder
	if sb.Len() != 0 {
		sb.WriteString("; ")
	}
	return sb
}

func (b *Batch) SetTimeNowFunc(f func() time.Time) *Batch {
	b.timeNowFunc = f
	return b
}

func (b *Batch) SetCustomFieldInterfaceResolver(f FieldInterfaceResolver) *Batch {
	b.customFieldInterfaceResolver = f
	return b
}

func (b *Batch) Raw(args ...interface{}) *Batch {
	sb := b.beginNextStmt()
	b.Expr(args...).WriteTo(sb)
	return b
}

func (b *Batch) Insert(v interface{}) *Batch {
	structVal := reflect.ValueOf(v)
	t, isSlice := assertPointerToStructOrSliceOfStructs(structVal.Type())
	if isSlice {
		return b.BulkInsert(v)
	}

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, b.customResolver())

	sb := b.beginNextStmt()
	sb.WriteString("INSERT INTO ")
	sb.WriteString(si.QuotedName)
	sb.WriteString(" (")
	fieldNamesWriter := newListWriter(sb)
	for _, f := range si.Fields {
		fieldNamesWriter.WriteString(f.QuotedName)
	}
	sb.WriteString(") VALUES (")
	writeFieldValues(si, ptr, sb, b.timeNow())
	sb.WriteString(") RETURNING NOTHING")
	return b
}

func (b *Batch) Upsert(v interface{}) *Batch {
	structVal := reflect.ValueOf(v)
	t, isSlice := assertPointerToStructOrSliceOfStructs(structVal.Type())
	if isSlice {
		return b.BulkUpsert(v)
	}

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, b.customResolver())

	sb := b.beginNextStmt()
	sb.WriteString("UPSERT INTO ")
	sb.WriteString(si.QuotedName)
	sb.WriteString(" (")
	fieldNamesWriter := newListWriter(sb)
	for _, f := range si.Fields {
		fieldNamesWriter.WriteString(f.QuotedName)
	}
	sb.WriteString(") VALUES (")
	writeFieldValues(si, ptr, sb, b.timeNow())
	sb.WriteString(") RETURNING NOTHING")
	return b
}

func (b *Batch) Update(v interface{}) *Batch {
	structVal := reflect.ValueOf(v)
	t := assertPointerToStruct(structVal.Type())

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, b.customResolver())
	assertHasPrimaryKeys(si)

	sb := b.beginNextStmt()
	sb.WriteString("UPDATE ")
	sb.WriteString(si.QuotedName)
	sb.WriteString(" SET ")
	valsWriter := newListWriter(sb)
	for _, f := range si.NonPrimaryKeys {
		if f.IsUpdated() {
			setTime(f, ptr, b.timeNow())
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
	si := GetStructInfo(t, b.customResolver())
	assertHasPrimaryKeys(si)

	sb := b.beginNextStmt()
	sb.WriteString("DELETE FROM ")
	sb.WriteString(si.QuotedName)
	sb.WriteString(" WHERE ")
	writePrimaryKeysWhereCondition(si, ptr, sb)
	return b
}

func (b *Batch) Select(qs ...*QBuilder) *Batch {
	for _, q := range qs {
		if q.into == nil {
			panic("make sure to call Q().Into(&v) before submitting the Q")
		}
		val := reflect.ValueOf(q.into)
		t, isSlice := assertPointerToStructOrPointerToSliceOfStructs(val.Type())

		si := GetStructInfo(t, b.customResolver())
		b.readIntos = append(b.readIntos, readInto{
			si:    si,
			slice: isSlice,
			ptr:   unsafe.Pointer(val.Pointer()),
			val:   val,
			errp:  q.errp,
		})

		sb := b.beginNextStmt()
		if q.rawDefined {
			q.writeRawTo(sb, si)
		} else {
			sb.WriteString("SELECT ")
			fieldNamesWriter := newListWriter(sb)
			for _, f := range si.Fields {
				fieldNamesWriter.WriteString(f.QuotedName)
			}
			sb.WriteString(" FROM ")
			sb.WriteString(q.quotedTableName(si))

			q.setImplicitLimit(isSlice)
			q.WriteTo(sb, si)
		}
	}
	return b
}

type ExecContexter interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type QueryContexter interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

func (b *Batch) Exec(ctx context.Context, conn ExecContexter) error {
	_, err := conn.ExecContext(ctx, b.String())
	return err
}

var ErrNotFound = errors.New("not found")

var select1HackFailure = errors.New("SELECT 1 hack failure")

func skipSelectOneHack(rows *sql.Rows) error {
	gotNext := rows.Next()
	if !gotNext {
		return select1HackFailure
	}
	gotNext = rows.Next()
	if gotNext {
		return select1HackFailure
	}
	moreSets := rows.NextResultSet()
	if !moreSets {
		return select1HackFailure
	}
	return nil
}

func (b *Batch) Query(ctx context.Context, conn QueryContexter) error {
	rows, err := conn.QueryContext(ctx, "SELECT 1; "+b.String())
	if err != nil {
		return err
	}
	defer rows.Close()

	if err := skipSelectOneHack(rows); err != nil {
		return err
	}

	var ptrs []interface{}
	for _, r := range b.readIntos {
		if cap(ptrs) < len(r.si.Fields) {
			ptrs = make([]interface{}, 0, len(r.si.Fields))
		}
		ptrs = ptrs[:len(r.si.Fields)]
		if r.slice {
			val := r.val.Elem() // get the slice itself
			idx := 0
			for {
				gotNext := rows.Next()
				if !gotNext {
					break
				}
				if idx >= val.Cap() {
					newCap := val.Cap() * 2
					if idx >= newCap {
						newCap = idx + 1
					}
					newSlice := reflect.MakeSlice(val.Type(), val.Len(), newCap)
					reflect.Copy(newSlice, val)
					val.Set(newSlice)
				}
				if idx >= val.Len() {
					val.SetLen(idx + 1)
				}
				ptr := unsafe.Pointer(val.Index(idx).Addr().Pointer())
				for i, f := range r.si.Fields {
					f.Interface.GetPtr(ptr, &ptrs[i])
				}
				if err := rows.Scan(ptrs...); err != nil {
					return err
				}
				idx++
			}
			val.SetLen(idx)
		} else {
			hasValue := rows.Next()
			if !hasValue {
				if r.errp != nil {
					*r.errp = ErrNotFound
				}
			} else {
				for i, f := range r.si.Fields {
					f.Interface.GetPtr(r.ptr, &ptrs[i])
				}
				if err := rows.Scan(ptrs...); err != nil {
					return err
				}
				for rows.Next() {
					// skip all the extra rows for single item fetch
				}
			}
		}
		moreSets := rows.NextResultSet()
		if !moreSets {
			break
		}
	}
	return nil
}

func (b *Batch) String() string {
	return b.stmtBuilder.String()
}
