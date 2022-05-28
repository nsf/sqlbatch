package sqlbatch

import (
	"context"
	"database/sql"
	"errors"
	"github.com/lib/pq"
	"github.com/nsf/sqlbatch/helper"
	"reflect"
	"strings"
	"sync"
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
	numUncommittedQs             int
	numWriteStmts                int
}

func New() *Batch {
	return &Batch{}
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
	b.numWriteStmts++
	sb := &b.stmtBuilder
	if sb.Len() != 0 {
		sb.WriteString("; ")
	}
	return sb
}

func (b *Batch) parallelQuery(ctx context.Context, conn QueryContexter) error {
	var wg sync.WaitGroup
	wg.Add(len(b.readIntos))

	errors := make([]error, len(b.readIntos))
	for i, r := range b.readIntos {
		i, r := i, r
		go func() {
			rows, err := conn.QueryContext(ctx, r.stmt)
			if err != nil {
				errors[i] = err
				wg.Done()
				return
			}
			defer rows.Close()

			var numArgs int
			if r.primitive {
				numArgs = 1
			} else {
				numArgs = len(r.si.Fields)
			}
			ptrs := make([]interface{}, numArgs)
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
						errors[i] = err
						wg.Done()
						return
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
						errors[i] = err
						wg.Done()
						return
					}
					for rows.Next() {
						// skip all the extra rows for single item fetch
					}
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()

	// check errors
	numErrors := 0
	for _, e := range errors {
		if e != nil {
			errors[numErrors] = e
			numErrors++
		}
	}
	errors = errors[:numErrors]
	if len(errors) != 0 {
		return &MultiError{Errors: errors}
	} else {
		return nil
	}
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
	if q, ok := v.(*QueryBuilder); ok {
		if table == "" {
			if q.quotedTable == "" {
				panic("when using Delete/DeleteFrom with QueryBuilder, table name must be provided via QueryBuilder or directly")
			}
			table = q.quotedTable
		} else {
			table = pq.QuoteIdentifier(table)
		}
		sb := b.beginNextStmt()
		sb.WriteString("DELETE FROM ")
		sb.WriteString(table)
		q.WriteTo(sb, nil)
		return b
	}

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

func (b *Batch) QueryBuilder(into ...interface{}) *QueryBuilder {
	b.numUncommittedQs++
	if len(into) > 1 {
		panic("multiple arguments are not allowed, this is a single optional argument")
	}
	var intoVal interface{}
	if len(into) > 0 {
		if table, ok := into[0].(string); ok {
			return (&QueryBuilder{b: b}).Table(table)
		}
		intoVal = into[0]
	}
	return &QueryBuilder{b: b, into: intoVal}
}

func (b *Batch) Select(qs ...*QueryBuilder) *Batch {
	b.numUncommittedQs -= len(qs)
	for _, q := range qs {
		if q.into == nil {
			panic("make sure to call Q().Into(&v) before submitting the Q")
		}
		val := reflect.ValueOf(q.into)
		var si *StructInfo
		var isSlice bool
		var ri readInto

		if q.fields != nil {
			if q.quotedTable == "" {
				panic("table must be specified explicitly when using Fields()")
			}
			_, isSlice = assertPointerOrPointerToSlice(val.Type())
			ri = readInto{
				slice:     isSlice,
				val:       val,
				errp:      q.errp,
				primitive: true,
			}
		} else {
			var t reflect.Type
			t, isSlice = assertPointerToStructOrPointerToSliceOfStructs(val.Type())
			si = GetStructInfo(t, b.customResolver())
			ri = readInto{
				si:    si,
				slice: isSlice,
				ptr:   unsafe.Pointer(val.Pointer()),
				val:   val,
				errp:  q.errp,
			}
		}

		var sb strings.Builder
		if q.rawDefined {
			q.writeRawTo(&sb, si)
		} else {
			sb.WriteString("SELECT ")
			q.columns(&sb, si)
			sb.WriteString(" FROM ")
			sb.WriteString(q.quotedTableName(si))
			q.setImplicitLimit(isSlice)
			q.WriteTo(&sb, si)
		}
		ri.stmt = sb.String()

		b.readIntos = append(b.readIntos, ri)
	}
	return b
}

type ExecContexter interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type QueryContexter interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type ExecQueryContexter interface {
	ExecContexter
	QueryContexter
}

func (b *Batch) Run(ctx context.Context, conn ExecQueryContexter) error {
	if b.numWriteStmts > 0 && len(b.readIntos) > 0 {
		panic("Batch contains both SELECT and UPDATE/INSERT/UPSERT/DELETE statements. Batch should contain only reads or only writes, but not both.")
	}
	if b.numUncommittedQs > 0 {
		panic("Batch has uncommitted query builders, only create query builders using QueryBuilder() if you end up committing it (using QueryBuilder.End() or Batch.Select())")
	}
	if b.numWriteStmts > 0 {
		_, err := conn.ExecContext(ctx, b.String())
		return err
	} else {
		return b.parallelQuery(ctx, conn)
	}
}

func (b *Batch) Transaction() *Batch {
	b.transaction = true
	return b
}

var ErrNotFound = errors.New("not found")

func (b *Batch) Expr(args ...interface{}) ExprBuilder {
	if len(args) == 0 {
		return ExprBuilder{b: b}
	}
	return ExprBuilder{b: b, root: exprFromArgs(b, args...)}
}

func (b *Batch) String() string {
	for _, r := range b.readIntos {
		sb := b.beginNextStmt()
		sb.WriteString(r.stmt)
	}

	var out string
	if b.transaction {
		out = "BEGIN; " + b.stmtBuilder.String() + "; COMMIT"
	} else {
		out = b.stmtBuilder.String()
	}

	if len(b.readIntos) != 0 {
		b.stmtBuilder = strings.Builder{}
		b.numWriteStmts = 0
	}

	return out
}
