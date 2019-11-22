package sqlbatch

import (
	"testing"
	"time"
)

func TestExprBuilder(t *testing.T) {
	b := New()
	assertStringEquals(t,
		b.Expr("foo = 1").And("bar = 2").And("baz = 3").String(),
		"((foo = 1 AND bar = 2) AND baz = 3)")
	assertStringEquals(t,
		b.Expr(b.Expr("aa = 1").And("bb = 2")).Or(b.Expr("cc = 3").And("dd = 4")).String(),
		"((aa = 1 AND bb = 2) OR (cc = 3 AND dd = 4))")
	assertStringEquals(t,
		b.Expr("foo = ?", 123).String(),
		"foo = 123")
	assertStringEquals(t,
		b.Expr("shop_id = ?", "id1").And("id = ?", "id2").String(),
		"(shop_id = 'id1' AND id = 'id2')")
	assertStringEquals(t,
		b.Expr("shop_id = ?", []byte{1, 2, 3}).Or("x > ?", time.Time{}).String(),
		`(shop_id = '\x010203' OR x > '0001-01-01 00:00:00')`)
	assertStringEquals(t,
		b.Expr("shop_id = ? AND id = ?", 5, 10).String(),
		`shop_id = 5 AND id = 10`)
}
