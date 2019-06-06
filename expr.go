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
	root    *expr
	current *expr
}

var placeholderRegexp = regexp.MustCompile(`\?`)

func exprFromArgs(args ...interface{}) *expr {
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
					t := GetTypeInfo(reflect.TypeOf(rest[i]), CustomFieldInterfaceResolver)
					var sb strings.Builder
					t.Conv(rest[i], &sb)
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

func Expr(args ...interface{}) ExprBuilder {
	e := exprFromArgs(args...)
	return ExprBuilder{
		current: e,
		root:    e,
	}
}

func (eb ExprBuilder) And(args ...interface{}) ExprBuilder {
	return ExprBuilder{
		current: eb.current,
		root: &expr{
			a:    eb.root,
			b:    exprFromArgs(args...),
			kind: exprAnd,
		},
	}
}

func (eb ExprBuilder) Or(args ...interface{}) ExprBuilder {
	return ExprBuilder{
		current: eb.current,
		root: &expr{
			a:    eb.root,
			b:    exprFromArgs(args...),
			kind: exprOr,
		},
	}
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

func (eb ExprBuilder) String() string {
	var sb strings.Builder
	exprToString(eb.root, &sb)
	return sb.String()
}
