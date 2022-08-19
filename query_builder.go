package sqlbatch

import (
	"context"
	"github.com/lib/pq"
	"github.com/nsf/sqlbatch/helper"
	"reflect"
	"regexp"
	"strings"
)

type orderByField struct {
	field string
	asc   bool
}

type QueryBuilder struct {
	b             *Batch
	into          any
	whereExprs    []ExprBuilder
	limit         int64
	offset        int64
	limitDefined  bool
	offsetDefined bool
	orderByFields []orderByField
	errp          *error
	quotedTable   string
	raw           ExprBuilder
	rawDefined    bool
	prefix        string
	fields        []string
}

func (q *QueryBuilder) Prefix(prefix string) *QueryBuilder {
	q.prefix = prefix
	return q
}

func (q *QueryBuilder) Raw(args ...any) *QueryBuilder {
	q.raw = q.b.Expr(args...)
	q.rawDefined = true
	return q
}

func (q *QueryBuilder) setImplicitLimit(isSlice bool) {
	if !isSlice {
		q.limit = 1
		q.limitDefined = true
	}
}

func (q *QueryBuilder) Table(v string) *QueryBuilder {
	q.quotedTable = pq.QuoteIdentifier(v)
	return q
}

func (q *QueryBuilder) TableFromStruct(v any) *QueryBuilder {
	val := reflect.ValueOf(v)
	t, _ := assertPointerToStructOrPointerToSliceOfStructs(val.Type())
	si := GetStructInfo(t, q.b.customResolver())
	q.quotedTable = si.QuotedName
	return q
}

func (q *QueryBuilder) Fields(v ...string) *QueryBuilder {
	q.fields = v
	return q
}

func (q *QueryBuilder) Into(v any) *QueryBuilder {
	q.into = v
	return q
}

func (q *QueryBuilder) Where(args ...any) *QueryBuilder {
	q.whereExprs = append(q.whereExprs, q.b.Expr(args...))
	return q
}

func (q *QueryBuilder) Limit(v int64) *QueryBuilder {
	q.limit = v
	q.limitDefined = true
	return q
}

func (q *QueryBuilder) Offset(v int64) *QueryBuilder {
	q.offset = v
	q.offsetDefined = true
	return q
}

func (q *QueryBuilder) OrderBy(field string, asc bool) *QueryBuilder {
	q.orderByFields = append(q.orderByFields, orderByField{
		field: field,
		asc:   asc,
	})
	return q
}

func (q *QueryBuilder) WithErr(errp *error) *QueryBuilder {
	q.errp = errp
	return q
}

func (q *QueryBuilder) quotedTableName(si *StructInfo) string {
	tname := q.quotedTable
	if tname == "" {
		tname = si.QuotedName
	}

	if q.prefix != "" {
		return tname + " AS " + q.prefix
	}
	return tname
}

func (q *QueryBuilder) prefixedFieldName(name string) string {
	if q.prefix != "" {
		return q.prefix + "." + name
	} else {
		return name
	}
}

func (q *QueryBuilder) columns(sb *strings.Builder, si *StructInfo) {
	fieldNamesWriter := helper.NewListWriter(sb)
	if q.fields != nil {
		for _, f := range q.fields {
			fieldNamesWriter.WriteString(q.prefixedFieldName(f))
		}
	} else {
		for _, f := range si.Fields {
			fieldNamesWriter.WriteString(q.prefixedFieldName(f.QuotedName))
		}
	}
}

var specialRegexp = regexp.MustCompile(`:[a-z]+:`)

func (q *QueryBuilder) writeRawTo(sb *strings.Builder, si *StructInfo) {
	var tmp strings.Builder
	q.raw.WriteTo(&tmp)
	sb.WriteString(specialRegexp.ReplaceAllStringFunc(tmp.String(), func(v string) string {
		if len(v) < 2 {
			return v
		}
		v = v[1 : len(v)-1]
		if v == "columns" {
			var sb strings.Builder
			q.columns(&sb, si)
			return sb.String()
		} else if v == "table" {
			return q.quotedTableName(si)
		} else {
			return v
		}
	}))
}

func (q *QueryBuilder) WriteTo(sb *strings.Builder, si *StructInfo) {
	// WHERE
	if len(q.whereExprs) != 0 {
		sb.WriteString(" WHERE ")
	}
	for i, w := range q.whereExprs {
		w.WriteTo(sb)
		if i != len(q.whereExprs)-1 {
			sb.WriteString(" AND ")
		}
	}

	// ORDER BY
	if len(q.orderByFields) != 0 {
		sb.WriteString(" ORDER BY ")
	}
	for i, f := range q.orderByFields {
		if si != nil {
			ff := si.FindField(f.field)
			if ff == nil {
				panic("unknown column: " + f.field + " (in table: " + si.QuotedName + ")")
			}
			sb.WriteString(ff.QuotedName)
		} else {
			sb.WriteString(pq.QuoteIdentifier(f.field))
		}
		if f.asc {
			sb.WriteString(" ASC")
		} else {
			sb.WriteString(" DESC")
		}
		if i != len(q.orderByFields)-1 {
			sb.WriteString(", ")
		}
	}

	// LIMIT
	if q.limitDefined {
		q.b.Expr(" LIMIT ?", q.limit).WriteTo(sb)
	}

	// OFFSET
	if q.offsetDefined {
		q.b.Expr(" OFFSET ?", q.offset).WriteTo(sb)
	}
}

func (q *QueryBuilder) End() *Batch {
	return q.b.Select(q)
}

// shortcut for q.End().Run(ctx, conn)
func (q *QueryBuilder) Run(ctx context.Context, conn ExecQueryContexter) error {
	return q.End().Run(ctx, conn)
}
