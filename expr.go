package sqlbatch

import (
	"reflect"
	"regexp"
	"strings"
)

type exprKind int

const (
	exprAnd exprKind = iota
	exprOr
	exprScalar
)

type expr struct {
	kind exprKind
	val  string
	a    *expr
	b    *expr
}

type ExprBuilder struct {
	b    *Batch
	root *expr
}

var placeholderRegexp = regexp.MustCompile(`\?`)

func exprFromArgs(b *Batch, args ...interface{}) *expr {
	if len(args) == 0 {
		panic("some argument is required")
	}
	switch first := args[0].(type) {
	case string:
		if len(args) == 1 {
			// just a string
			return &expr{
				kind: exprScalar,
				val:  first,
			}
		} else {
			// format with args
			numQ := strings.Count(first, "?")
			if numQ != len(args)-1 {
				panic("invalid number of arguments, number of arguments should match number of ? placeholders")
			}
			i := 0
			rest := args[1:]
			return &expr{
				kind: exprScalar,
				val: placeholderRegexp.ReplaceAllStringFunc(first, func(v string) string {
					t := GetTypeInfo(reflect.TypeOf(rest[i]), b.customFieldInterfaceResolver)
					var sb strings.Builder
					t.Conv(rest[i], &sb)
					i++
					return sb.String()
				}),
			}
		}
	case ExprBuilder:
		return first.root
	default:
		panic("type not supported: " + reflect.TypeOf(args[0]).String())
	}
}

func (eb ExprBuilder) And(args ...interface{}) ExprBuilder {
	if eb.root == nil {
		return ExprBuilder{b: eb.b, root: exprFromArgs(eb.b, args...)}
	}
	return ExprBuilder{
		b: eb.b,
		root: &expr{
			a:    eb.root,
			b:    exprFromArgs(eb.b, args...),
			kind: exprAnd,
		},
	}
}

func (eb ExprBuilder) Or(args ...interface{}) ExprBuilder {
	if eb.root == nil {
		return ExprBuilder{b: eb.b, root: exprFromArgs(eb.b, args...)}
	}
	return ExprBuilder{
		b: eb.b,
		root: &expr{
			a:    eb.root,
			b:    exprFromArgs(eb.b, args...),
			kind: exprOr,
		},
	}
}

func (eb ExprBuilder) IsEmpty() bool {
	return eb.root == nil
}

func exprToString(e *expr, sb *strings.Builder) {
	switch e.kind {
	case exprScalar:
		sb.WriteString(e.val)
	case exprAnd:
		sb.WriteString("(")
		exprToString(e.a, sb)
		sb.WriteString(" AND ")
		exprToString(e.b, sb)
		sb.WriteString(")")
	case exprOr:
		sb.WriteString("(")
		exprToString(e.a, sb)
		sb.WriteString(" OR ")
		exprToString(e.b, sb)
		sb.WriteString(")")
	}
}

func (eb ExprBuilder) WriteTo(sb *strings.Builder) {
	exprToString(eb.root, sb)
}

func (eb ExprBuilder) String() string {
	var sb strings.Builder
	exprToString(eb.root, &sb)
	return sb.String()
}
