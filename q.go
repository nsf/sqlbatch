package sqlbatch

import (
	"context"
	"github.com/lib/pq"
	"regexp"
	"strings"
)

type orderByField struct {
	field string
	asc   bool
}

type postKind int

const (
	postNone postKind = iota
	postSelect
)

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
	raw           ExprBuilder
	rawDefined    bool
	prefix        string

	postKind postKind
}

func (b *Batch) QSelect(into ...interface{}) *QBuilder {
	q := b.Q(into...)
	q.postKind = postSelect
	return q
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

func (q *QBuilder) Prefix(prefix string) *QBuilder {
	q.prefix = prefix
	return q
}

func (q *QBuilder) Raw(args ...interface{}) *QBuilder {
	q.raw = q.b.Expr(args...)
	q.rawDefined = true
	return q
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

func (q *QBuilder) quotedTableName(si *StructInfo) string {
	tname := si.QuotedName
	if q.quotedTable != "" {
		tname = q.quotedTable
	}

	if q.prefix != "" {
		return tname + " AS " + q.prefix
	}
	return tname
}

func (q *QBuilder) prefixedFieldName(name string) string {
	if q.prefix != "" {
		return q.prefix + "." + name
	} else {
		return name
	}
}

var specialRegexp = regexp.MustCompile(`:[a-z]+:`)

func (q *QBuilder) writeRawTo(sb *strings.Builder, si *StructInfo) {
	var tmp strings.Builder
	q.raw.WriteTo(&tmp)
	sb.WriteString(specialRegexp.ReplaceAllStringFunc(tmp.String(), func(v string) string {
		if len(v) < 2 {
			return v
		}
		v = v[1 : len(v)-1]
		if v == "columns" {
			var sb strings.Builder
			fieldNamesWriter := newListWriter(&sb)
			for _, f := range si.Fields {
				fieldNamesWriter.WriteString(q.prefixedFieldName(f.QuotedName))
			}
			return sb.String()
		} else if v == "table" {
			return q.quotedTableName(si)
		} else {
			return v
		}
	}))
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

func (q *QBuilder) End() *Batch {
	switch q.postKind {
	case postSelect:
		return q.b.Select(q)
	default:
		return q.b
	}
}

// Shortcut for q.End().Query(ctx, conn)
func (q *QBuilder) Query(ctx context.Context, conn QueryContexter) error {
	return q.End().Query(ctx, conn)
}

// Shortcut for q.End().Exec(ctx, conn)
func (q *QBuilder) Exec(ctx context.Context, conn ExecContexter) error {
	return q.End().Exec(ctx, conn)
}
