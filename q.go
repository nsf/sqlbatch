package sqlbatch

import (
	"github.com/lib/pq"
	"strings"
)

type orderByField struct {
	field string
	asc   bool
}

type QBuilder struct {
	b             *Batch
	into          interface{}
	whereExprs    []ExprBuilder
	limit         int64
	offset        int64
	limitDefined  bool
	offsetDefined bool
	orderByFields []orderByField
	errp          *error
	quotedTable   string
}

func (b *Batch) Q(into ...interface{}) *QBuilder {
	if len(into) > 1 {
		panic("multiple arguments are not allowed, this is a single optional argument")
	}
	var intoVal interface{}
	if len(into) > 0 {
		intoVal = into[0]
	}
	return &QBuilder{b: b, into: intoVal}
}

func (q *QBuilder) setImplicitLimit(isSlice bool) {
	if !isSlice {
		q.limit = 1
		q.limitDefined = true
	}
}

func (q *QBuilder) Table(v string) *QBuilder {
	q.quotedTable = pq.QuoteIdentifier(v)
	return q
}

func (q *QBuilder) Into(v interface{}) *QBuilder {
	q.into = v
	return q
}

func (q *QBuilder) Where(args ...interface{}) *QBuilder {
	q.whereExprs = append(q.whereExprs, q.b.Expr(args...))
	return q
}

func (q *QBuilder) Limit(v int64) *QBuilder {
	q.limit = v
	q.limitDefined = true
	return q
}

func (q *QBuilder) Offset(v int64) *QBuilder {
	q.offset = v
	q.offsetDefined = true
	return q
}

func (q *QBuilder) OrderBy(field string, asc bool) *QBuilder {
	q.orderByFields = append(q.orderByFields, orderByField{
		field: field,
		asc:   asc,
	})
	return q
}

func (q *QBuilder) WithErr(errp *error) *QBuilder {
	q.errp = errp
	return q
}

func (q *QBuilder) WriteTo(sb *strings.Builder, si *StructInfo) {
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
		ff := si.FindField(f.field)
		if ff == nil {
			panic("unknown column: " + f.field + " (in table: " + si.QuotedName + ")")
		}
		sb.WriteString(ff.QuotedName)
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
