package sqlbatch

import (
	"context"
	"database/sql"
	"errors"
	"github.com/lib/pq"
	"github.com/nsf/sqlbatch/helper"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

type Batch struct {
	transaction                  bool
	stmtBuilder                  strings.Builder
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

func assertPointerOrPointerToSlice(t reflect.Type) (reflect.Type, bool) {
	if t.Kind() != reflect.Ptr {
		panic("pointer to value or pointer to slice of values")
	}
	t = t.Elem()
	return t, t.Kind() == reflect.Slice
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
	pkWriter := helper.NewAndWriter(sb)
	for _, f := range si.PrimaryKeys {
		b := pkWriter.Next()
		b.WriteString(f.QuotedName)
		b.WriteString(" = ")
		f.Interface.Write(ptr, b)
	}
	sb.WriteString(" RETURNING NOTHING")
}

func writeFieldValues(si *StructInfo, ptr unsafe.Pointer, sb *strings.Builder, now time.Time, insert bool) {
	fieldValuesWriter := helper.NewListWriter(sb)
	for _, f := range si.Fields {
		if f.IsCreated() || f.IsUpdated() {
			setTime(&f, ptr, now)
		}
		if insert && f.IsDefault() {
			sb := fieldValuesWriter.Next()
			sb.WriteString("DEFAULT")
		} else {
			f.Interface.Write(ptr, fieldValuesWriter.Next())
		}
	}
}

func writeTableName(si *StructInfo, table string, sb *strings.Builder) {
	if table == "" {
		sb.WriteString(si.QuotedName)
	} else {
		sb.WriteString(pq.QuoteIdentifier(table))
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
	return b.InsertInto(v, "")
}

func (b *Batch) InsertInto(v interface{}, table string) *Batch {
	structVal := reflect.ValueOf(v)
	t, isSlice := assertPointerToStructOrSliceOfStructs(structVal.Type())
	if isSlice {
		serter := bulkSerter{command: "INSERT", b: b}
		serter.addMany(v)
		return serter.commit()
	}

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, b.customResolver())

	sb := b.beginNextStmt()
	sb.WriteString("INSERT INTO ")
	writeTableName(si, table, sb)
	sb.WriteString(" (")
	fieldNamesWriter := helper.NewListWriter(sb)
	for _, f := range si.Fields {
		fieldNamesWriter.WriteString(f.QuotedName)
	}
	sb.WriteString(") VALUES (")
	writeFieldValues(si, ptr, sb, b.timeNow(), true)
	sb.WriteString(") RETURNING NOTHING")
	return b
}

func (b *Batch) Upsert(v interface{}) *Batch {
	return b.UpsertInto(v, "")
}

func (b *Batch) UpsertInto(v interface{}, table string) *Batch {
	structVal := reflect.ValueOf(v)
	t, isSlice := assertPointerToStructOrSliceOfStructs(structVal.Type())
	if isSlice {
		serter := bulkSerter{command: "UPSERT", b: b}
		serter.addMany(v)
		return serter.commit()
	}

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, b.customResolver())

	sb := b.beginNextStmt()
	sb.WriteString("UPSERT INTO ")
	writeTableName(si, table, sb)
	sb.WriteString(" (")
	fieldNamesWriter := helper.NewListWriter(sb)
	for _, f := range si.Fields {
		fieldNamesWriter.WriteString(f.QuotedName)
	}
	sb.WriteString(") VALUES (")
	writeFieldValues(si, ptr, sb, b.timeNow(), false)
	sb.WriteString(") RETURNING NOTHING")
	return b
}

func (b *Batch) Update(v interface{}) *Batch {
	return b.UpdateInto(v, "")
}

func (b *Batch) UpdateInto(v interface{}, table string) *Batch {
	structVal := reflect.ValueOf(v)
	t := assertPointerToStruct(structVal.Type())

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, b.customResolver())
	assertHasPrimaryKeys(si)

	sb := b.beginNextStmt()
	sb.WriteString("UPDATE ")
	writeTableName(si, table, sb)
	sb.WriteString(" SET ")
	valsWriter := helper.NewListWriter(sb)
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
	return b.DeleteFrom(v, "")
}

func (b *Batch) DeleteFrom(v interface{}, table string) *Batch {
	structVal := reflect.ValueOf(v)
	t := assertPointerToStruct(structVal.Type())

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, b.customResolver())
	assertHasPrimaryKeys(si)

	sb := b.beginNextStmt()
	sb.WriteString("DELETE FROM ")
	writeTableName(si, table, sb)
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
		var si *StructInfo
		var isSlice bool
		if q.fields != nil {
			if q.quotedTable == "" {
				panic("table must be specified explicitly when using Fields()")
			}
			_, isSlice = assertPointerOrPointerToSlice(val.Type())
			b.readIntos = append(b.readIntos, readInto{
				slice:     isSlice,
				val:       val,
				errp:      q.errp,
				primitive: true,
			})
		} else {
			var t reflect.Type
			t, isSlice = assertPointerToStructOrPointerToSliceOfStructs(val.Type())
			si = GetStructInfo(t, b.customResolver())
			b.readIntos = append(b.readIntos, readInto{
				si:    si,
				slice: isSlice,
				ptr:   unsafe.Pointer(val.Pointer()),
				val:   val,
				errp:  q.errp,
			})
		}

		sb := b.beginNextStmt()
		if q.rawDefined {
			q.writeRawTo(sb, si)
		} else {
			sb.WriteString("SELECT ")
			q.columns(sb, si)
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

func (b *Batch) Transaction() *Batch {
	b.transaction = true
	return b
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
		var numArgs int
		if r.primitive {
			numArgs = 1
		} else {
			numArgs = len(r.si.Fields)
		}
		if cap(ptrs) < numArgs {
			ptrs = make([]interface{}, 0, numArgs)
		}
		ptrs = ptrs[:numArgs]
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
				if r.primitive {
					ptrs[0] = val.Index(idx).Addr().Interface()
				} else {
					ptr := unsafe.Pointer(val.Index(idx).Addr().Pointer())
					for i, f := range r.si.Fields {
						f.Interface.GetPtr(ptr, &ptrs[i])
					}
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
				if r.primitive {
					ptrs[0] = r.val.Interface()
				} else {
					for i, f := range r.si.Fields {
						f.Interface.GetPtr(r.ptr, &ptrs[i])
					}
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
	if b.transaction {
		return "BEGIN; " + b.stmtBuilder.String() + "; COMMIT"
	} else {
		return b.stmtBuilder.String()
	}
}
