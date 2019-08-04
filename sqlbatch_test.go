package sqlbatch

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"math"
	"reflect"
	"testing"
	"time"
	"unicode/utf8"
	"unsafe"
)

func TestBatchInsert(t *testing.T) {
	type FooBar struct {
		ID        int `db:"primary_key"`
		Foo       string
		Bar       string
		CreatedAt time.Time
	}

	b := New()
	b.Insert(&FooBar{ID: 1, Foo: "foo 1", Bar: "bar 1", CreatedAt: time.Now()})
	b.Insert(&FooBar{ID: 2, Foo: "foo 2", Bar: "bar 2", CreatedAt: time.Now()})
	b.Update(&FooBar{ID: 1, Foo: "foo 222", Bar: "bar 222", CreatedAt: time.Now()})
	b.Update(&FooBar{ID: 2, Foo: "foo 222", Bar: "bar 222", CreatedAt: time.Now()})
	query := b.String()
	t.Log(query)

	db := openTestDBConnection(t)
	defer db.Close()

	dbExec(t, db, `
		DROP TABLE IF EXISTS "foo_bar";
		CREATE TABLE foo_bar (
			id INT NOT NULL,
			foo STRING NOT NULL,
			bar STRING NOT NULL,
			created_at TIMESTAMP NOT NULL,
			CONSTRAINT "primary" PRIMARY KEY (id ASC)
		)
	`)

	dbExec(t, db, query)
}

func TestStringFormatting(t *testing.T) {
	db := openTestDBConnection(t)
	defer db.Close()

	dbExec(t, db, `
		DROP TABLE IF EXISTS "string_format";
		CREATE TABLE "string_format" (
			key INT NOT NULL,
			value STRING NOT NULL,
			CONSTRAINT "primary" PRIMARY KEY (key ASC)
		)
	`)

	type StringFormat struct {
		Key   int64 `db:"primary_key"`
		Value string
	}

	for i := 0; i < 256; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			dbExec(t, db, New().Insert(&StringFormat{Key: 1, Value: string([]byte{byte(i)})}).String())

			var s string
			dbScanSingleRow(t, db, "SELECT value FROM string_format WHERE key = 1", &s)
			if !utf8.ValidString(s) {
				t.Errorf("invalid string")
			}

			dbExec(t, db, New().Delete(&StringFormat{Key: 1}).String())
		})
	}
}

func TestBytesFormatting(t *testing.T) {
	db := openTestDBConnection(t)
	defer db.Close()

	dbExec(t, db, `
		DROP TABLE IF EXISTS "bytes_format";
		CREATE TABLE "bytes_format" (
			key INT NOT NULL,
			value BYTES NOT NULL,
			CONSTRAINT "primary" PRIMARY KEY (key ASC)
		)
	`)

	type BytesFormat struct {
		Key   int64 `db:"primary_key"`
		Value []byte
	}

	for i := 0; i < 256; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			dbExec(t, db, New().Insert(&BytesFormat{Key: 1, Value: []byte{byte(i)}}).String())

			var b []byte
			dbScanSingleRow(t, db, "SELECT value FROM bytes_format WHERE key = 1", &b)
			if len(b) != 1 && bytes.Equal(b, []byte{byte(i)}) {
				t.Errorf("invalid bytes")
			}

			dbExec(t, db, New().Delete(&BytesFormat{Key: 1}).String())
		})
	}

	t.Run("MultipleBytes", func(t *testing.T) {
		dbExec(t, db, New().Insert(&BytesFormat{Key: 1, Value: []byte("проверка")}).String())

		var b []byte
		dbScanSingleRow(t, db, "SELECT value FROM bytes_format WHERE key = 1", &b)
		if string(b) != "проверка" {
			t.Errorf("invalid bytes")
		}

		dbExec(t, db, New().Delete(&BytesFormat{Key: 1}).String())
	})
}

func TestMiscFormatting(t *testing.T) {
	db := openTestDBConnection(t)
	defer db.Close()

	dbExec(t, db, `
		DROP TABLE IF EXISTS "misc_format";
		CREATE TABLE "misc_format" (
			key INT NOT NULL,
			f FLOAT8 NULL DEFAULT NULL,
			i INT NULL DEFAULT NULL,
			b BOOL NULL DEFAULT NULL,
			t TIMESTAMP NULL DEFAULT NULL,
			CONSTRAINT "primary" PRIMARY KEY (key ASC)
		)
	`)
	type MiscFormat struct {
		Key int64 `db:"primary_key"`
		F   sql.NullFloat64
		I   sql.NullInt64
		B   sql.NullBool
		T   pq.NullTime
	}

	t.Run("Float64", func(t *testing.T) {
		dbExec(t, db, New().
			Insert(&MiscFormat{Key: 1, F: sql.NullFloat64{}}).
			Insert(&MiscFormat{Key: 2, F: sql.NullFloat64{Valid: true, Float64: math.NaN()}}).
			Insert(&MiscFormat{Key: 3, F: sql.NullFloat64{Valid: true, Float64: math.Inf(1)}}).
			Insert(&MiscFormat{Key: 4, F: sql.NullFloat64{Valid: true, Float64: math.Inf(-1)}}).
			Insert(&MiscFormat{Key: 5, F: sql.NullFloat64{Valid: true, Float64: 3.14159265358}}).
			String())

		assertFloat64 := func(key int64, v sql.NullFloat64) {
			var ret sql.NullFloat64
			dbScanSingleRow(t, db, "SELECT f FROM misc_format WHERE key = "+fmt.Sprint(key), &ret)
			if math.IsNaN(ret.Float64) && math.IsNaN(v.Float64) {
				if ret.Valid != v.Valid {
					t.Errorf("value mismatch: %v vs %v", ret, v)
				}
			} else if ret != v {
				t.Errorf("value mismatch: %v vs %v", ret, v)
			}
		}
		assertFloat64(1, sql.NullFloat64{})
		assertFloat64(2, sql.NullFloat64{Valid: true, Float64: math.NaN()})
		assertFloat64(3, sql.NullFloat64{Valid: true, Float64: math.Inf(1)})
		assertFloat64(4, sql.NullFloat64{Valid: true, Float64: math.Inf(-1)})
		assertFloat64(5, sql.NullFloat64{Valid: true, Float64: 3.14159265358})
		dbExec(t, db, `DELETE FROM misc_format`)
	})

	t.Run("Int64", func(t *testing.T) {
		dbExec(t, db, New().
			Insert(&MiscFormat{Key: 1, I: sql.NullInt64{}}).
			Insert(&MiscFormat{Key: 2, I: sql.NullInt64{Valid: true, Int64: 0}}).
			Insert(&MiscFormat{Key: 3, I: sql.NullInt64{Valid: true, Int64: math.MinInt64}}).
			Insert(&MiscFormat{Key: 4, I: sql.NullInt64{Valid: true, Int64: math.MaxInt64}}).
			Insert(&MiscFormat{Key: 5, I: sql.NullInt64{Valid: true, Int64: 63463}}).
			String())

		assertInt64 := func(key int64, v sql.NullInt64) {
			var ret sql.NullInt64
			dbScanSingleRow(t, db, "SELECT i FROM misc_format WHERE key = "+fmt.Sprint(key), &ret)
			if ret != v {
				t.Errorf("value mismatch: %v vs %v", ret, v)
			}
		}
		assertInt64(1, sql.NullInt64{})
		assertInt64(2, sql.NullInt64{Valid: true, Int64: 0})
		assertInt64(3, sql.NullInt64{Valid: true, Int64: math.MinInt64})
		assertInt64(4, sql.NullInt64{Valid: true, Int64: math.MaxInt64})
		assertInt64(5, sql.NullInt64{Valid: true, Int64: 63463})
		dbExec(t, db, `DELETE FROM misc_format`)
	})

	t.Run("Bool", func(t *testing.T) {
		dbExec(t, db, New().
			Insert(&MiscFormat{Key: 1, B: sql.NullBool{}}).
			Insert(&MiscFormat{Key: 2, B: sql.NullBool{Valid: true, Bool: true}}).
			Insert(&MiscFormat{Key: 3, B: sql.NullBool{Valid: true, Bool: false}}).
			String())

		assertBool := func(key int64, v sql.NullBool) {
			var ret sql.NullBool
			dbScanSingleRow(t, db, "SELECT b FROM misc_format WHERE key = "+fmt.Sprint(key), &ret)
			if ret != v {
				t.Errorf("value mismatch: %v vs %v", ret, v)
			}
		}
		assertBool(1, sql.NullBool{})
		assertBool(2, sql.NullBool{Valid: true, Bool: true})
		assertBool(3, sql.NullBool{Valid: true, Bool: false})
		dbExec(t, db, `DELETE FROM misc_format`)
	})

	t.Run("Time", func(t *testing.T) {
		dbExec(t, db, New().
			Insert(&MiscFormat{Key: 1, T: pq.NullTime{}}).
			Insert(&MiscFormat{Key: 2, T: pq.NullTime{Valid: true, Time: rfc3339ToTime("2015-06-02T02:00:56Z")}}).
			Insert(&MiscFormat{Key: 3, T: pq.NullTime{Valid: true, Time: rfc3339ToTime("1996-12-19T16:39:57-08:00")}}).
			Insert(&MiscFormat{Key: 4, T: pq.NullTime{Valid: true, Time: rfc3339NanoToTime("2006-01-02T15:04:05.123456789Z")}}).
			String())

		assertTime := func(key int64, v pq.NullTime) {
			var ret pq.NullTime
			dbScanSingleRow(t, db, "SELECT t FROM misc_format WHERE key = "+fmt.Sprint(key), &ret)
			if ret.Valid != v.Valid {
				t.Errorf("value mismatch: %v vs %v", ret, v)
			}
			if ret.Time.UTC() != v.Time.UTC() {
				t.Errorf("value mismatch: %v vs %v", ret, v)
			}
		}
		assertTime(1, pq.NullTime{})
		assertTime(2, pq.NullTime{Valid: true, Time: rfc3339ToTime("2015-06-02T02:00:56Z")})
		assertTime(3, pq.NullTime{Valid: true, Time: rfc3339ToTime("1996-12-19T16:39:57-08:00")})
		// note that cockroachdb offers precision only up to microseconds, while Go can do nano with time.Time
		assertTime(4, pq.NullTime{Valid: true, Time: rfc3339NanoToTime("2006-01-02T15:04:05.123456000Z")})
		dbExec(t, db, `DELETE FROM misc_format`)
	})

}

func TestGetStructInfo(t *testing.T) {
	type GroupFoo struct {
		GFoo int
		GBar int
	}

	type CommonStruct struct {
		FieldA string
		FieldB string
		FieldC string `gorm:"primary_key"`
		E1     bool
		E2     string
		E3     float32
		E4     float64
		E5     int
		E6     int8
		E7     int16
		E8     int32
		E9     int64
		E10    uint
		E11    uint8
		E12    uint16
		E13    uint32
		E14    uint64
		E15    []byte
		E16    time.Time
		E17    pq.NullTime
		E18    sql.NullBool
		E19    sql.NullFloat64
		E20    sql.NullInt64
		E21    sql.NullString
		Foo    float32
		Bar    float32
		SomeA  int `db:"-"`
		SomeB  int `db:"-"`
	}

	type FooBar struct {
		Foo int `db:"column:baz,primary_key"`
		Bar int
		F1  bool
		F2  string
		F3  float32
		F4  float64
		F5  int
		F6  int8
		F7  int16
		F8  int32
		F9  int64
		F10 uint
		F11 uint8
		F12 uint16
		F13 uint32
		F14 uint64
		F15 []byte
		F16 time.Time
		F17 pq.NullTime
		F18 sql.NullBool
		F19 sql.NullFloat64
		F20 sql.NullInt64
		F21 sql.NullString
		CommonStruct
		GroupFoo `db:"group:foo"`
	}

	v := &FooBar{
		Foo: 123,
		Bar: 456,
		F1:  true,
		F2:  "abc",
		F3:  3.14,
		F4:  3.1415,
		F5:  1,
		F6:  2,
		F7:  3,
		F8:  4,
		F9:  5,
		F10: 6,
		F11: 7,
		F12: 8,
		F13: 9,
		F14: 10,
		F15: []byte("foobar"),
		F16: rfc3339ToTime("2015-06-02T02:00:56Z"),
		F17: pq.NullTime{Valid: true, Time: rfc3339ToTime("2015-06-02T02:00:56Z")},
		F18: sql.NullBool{Valid: true, Bool: true},
		F19: sql.NullFloat64{Valid: true, Float64: 3.1415},
		F20: sql.NullInt64{Valid: true, Int64: 31337},
		F21: sql.NullString{Valid: true, String: "hax0r"},
		CommonStruct: CommonStruct{
			FieldA: "a",
			FieldB: "b",
			FieldC: "ccc",
			E1:     true,
			E2:     "abc",
			E3:     3.14,
			E4:     3.1415,
			E5:     1,
			E6:     2,
			E7:     3,
			E8:     4,
			E9:     5,
			E10:    6,
			E11:    7,
			E12:    8,
			E13:    9,
			E14:    10,
			E15:    []byte("foobar"),
			E16:    rfc3339ToTime("2015-06-02T02:00:56Z"),
			E17:    pq.NullTime{Valid: true, Time: rfc3339ToTime("2015-06-02T02:00:56Z")},
			E18:    sql.NullBool{Valid: true, Bool: true},
			E19:    sql.NullFloat64{Valid: true, Float64: 3.1415},
			E20:    sql.NullInt64{Valid: true, Int64: 31337},
			E21:    sql.NullString{Valid: true, String: "hax0r"},
		},
		GroupFoo: GroupFoo{
			GFoo: 123,
			GBar: 321,
		},
	}
	ptr := unsafe.Pointer(v)
	si := GetStructInfo(reflect.TypeOf(v).Elem(), nil)

	assertField := func(idx *int, fs []FieldInfo, name string, val interface{}, primaryKey bool, customMatchers ...func(t *testing.T, f FieldInfo)) {
		f := fs[*idx]
		*idx++
		if f.Name != name {
			t.Errorf("field %q name mismatch: %q != %q", f.Name, f.Name, name)
		}

		var fval interface{}
		f.Interface.Get(ptr, &fval)
		if !reflect.DeepEqual(fval, val) {
			t.Errorf("field %q value mismatch: %v != %v", f.Name, fval, val)
		}

		if f.IsPrimaryKey() != primaryKey {
			t.Errorf("field %q primary key mismatch: %v != %v", f.Name, f.IsPrimaryKey(), primaryKey)
		}
		for _, m := range customMatchers {
			m(t, f)
		}
	}

	isOfGroup := func(group string) func(t *testing.T, f FieldInfo) {
		return func(t *testing.T, f FieldInfo) {
			if f.Group != group {
				t.Errorf("field %q group mismatch: %q != %q", f.Name, f.Group, group)
			}
		}
	}

	if si.Name != "foo_bar" {
		t.Errorf("unexpected struct name: %s", si.Name)
	}

	idx := 0
	assertField(&idx, si.Fields, "baz", int(123), true)
	assertField(&idx, si.Fields, "bar", int(456), false)

	assertField(&idx, si.Fields, "f1", bool(true), false)
	assertField(&idx, si.Fields, "f2", string("abc"), false)
	assertField(&idx, si.Fields, "f3", float32(3.14), false)
	assertField(&idx, si.Fields, "f4", float64(3.1415), false)
	assertField(&idx, si.Fields, "f5", int(1), false)
	assertField(&idx, si.Fields, "f6", int8(2), false)
	assertField(&idx, si.Fields, "f7", int16(3), false)
	assertField(&idx, si.Fields, "f8", int32(4), false)
	assertField(&idx, si.Fields, "f9", int64(5), false)
	assertField(&idx, si.Fields, "f10", uint(6), false)
	assertField(&idx, si.Fields, "f11", uint8(7), false)
	assertField(&idx, si.Fields, "f12", uint16(8), false)
	assertField(&idx, si.Fields, "f13", uint32(9), false)
	assertField(&idx, si.Fields, "f14", uint64(10), false)
	assertField(&idx, si.Fields, "f15", []byte("foobar"), false)
	assertField(&idx, si.Fields, "f16", rfc3339ToTime("2015-06-02T02:00:56Z"), false)
	assertField(&idx, si.Fields, "f17", pq.NullTime{Valid: true, Time: rfc3339ToTime("2015-06-02T02:00:56Z")}, false)
	assertField(&idx, si.Fields, "f18", sql.NullBool{Valid: true, Bool: true}, false)
	assertField(&idx, si.Fields, "f19", sql.NullFloat64{Valid: true, Float64: 3.1415}, false)
	assertField(&idx, si.Fields, "f20", sql.NullInt64{Valid: true, Int64: 31337}, false)
	assertField(&idx, si.Fields, "f21", sql.NullString{Valid: true, String: "hax0r"}, false)

	assertField(&idx, si.Fields, "field_a", string("a"), false)
	assertField(&idx, si.Fields, "field_b", string("b"), false)
	assertField(&idx, si.Fields, "field_c", string("ccc"), true)

	assertField(&idx, si.Fields, "e1", bool(true), false)
	assertField(&idx, si.Fields, "e2", string("abc"), false)
	assertField(&idx, si.Fields, "e3", float32(3.14), false)
	assertField(&idx, si.Fields, "e4", float64(3.1415), false)
	assertField(&idx, si.Fields, "e5", int(1), false)
	assertField(&idx, si.Fields, "e6", int8(2), false)
	assertField(&idx, si.Fields, "e7", int16(3), false)
	assertField(&idx, si.Fields, "e8", int32(4), false)
	assertField(&idx, si.Fields, "e9", int64(5), false)
	assertField(&idx, si.Fields, "e10", uint(6), false)
	assertField(&idx, si.Fields, "e11", uint8(7), false)
	assertField(&idx, si.Fields, "e12", uint16(8), false)
	assertField(&idx, si.Fields, "e13", uint32(9), false)
	assertField(&idx, si.Fields, "e14", uint64(10), false)
	assertField(&idx, si.Fields, "e15", []byte("foobar"), false)
	assertField(&idx, si.Fields, "e16", rfc3339ToTime("2015-06-02T02:00:56Z"), false)
	assertField(&idx, si.Fields, "e17", pq.NullTime{Valid: true, Time: rfc3339ToTime("2015-06-02T02:00:56Z")}, false)
	assertField(&idx, si.Fields, "e18", sql.NullBool{Valid: true, Bool: true}, false)
	assertField(&idx, si.Fields, "e19", sql.NullFloat64{Valid: true, Float64: 3.1415}, false)
	assertField(&idx, si.Fields, "e20", sql.NullInt64{Valid: true, Int64: 31337}, false)
	assertField(&idx, si.Fields, "e21", sql.NullString{Valid: true, String: "hax0r"}, false)

	assertField(&idx, si.Fields, "foo", float32(0), false, isOfGroup(""))

	assertField(&idx, si.Fields, "g_foo", int(123), false, isOfGroup("foo"))
	assertField(&idx, si.Fields, "g_bar", int(321), false, isOfGroup("foo"))

	if len(si.Fields) != idx {
		t.Errorf("expected %d fields (one ignored), got: %d", idx, len(si.Fields))
	}
}

func TestCreatedUpdated(t *testing.T) {
	type CreatedUpdated struct {
		ID        int64     `db:"primary_key"`
		CreatedAt time.Time `db:"created"`
		UpdatedAt time.Time `db:"updated"`
	}

	b1 := New()
	b1.SetTimeNowFunc(func() time.Time { return rfc3339ToTime("2012-12-12T12:12:12Z") })
	b1.Insert(&CreatedUpdated{ID: 1})
	assertStringEquals(t, b1.String(),
		`INSERT INTO "created_updated" ("id", "created_at", "updated_at") VALUES (1, TIMESTAMP '2012-12-12 12:12:12', TIMESTAMP '2012-12-12 12:12:12') RETURNING NOTHING`)
	b2 := New()
	b2.SetTimeNowFunc(func() time.Time { return rfc3339ToTime("2012-12-12T12:12:12Z") })
	b2.Update(&CreatedUpdated{ID: 1})
	assertStringEquals(t, b2.String(),
		`UPDATE "created_updated" SET "created_at" = TIMESTAMP '0001-01-01 00:00:00', "updated_at" = TIMESTAMP '2012-12-12 12:12:12' WHERE "id" = 1 RETURNING NOTHING`)
}

func TestGet(t *testing.T) {
	type CreatedUpdated struct {
		ID        int64     `db:"primary_key"`
		CreatedAt time.Time `db:"created"`
		UpdatedAt time.Time `db:"updated"`
	}

	var out1, out2 CreatedUpdated
	var out3 []CreatedUpdated

	{
		b := New()
		b.Select(b.Q(&out1).Where("id = ?", 1))
		b.Select(b.Q(&out2).Where("id = ?", 2))
		assertStringEquals(t, b.String(),
			`SELECT "id", "created_at", "updated_at" FROM "created_updated" WHERE id = 1 LIMIT 1; SELECT "id", "created_at", "updated_at" FROM "created_updated" WHERE id = 2 LIMIT 1`)
	}
	{
		b := New()
		b.Select(b.Q(&out1))
		assertStringEquals(t, b.String(),
			`SELECT "id", "created_at", "updated_at" FROM "created_updated" LIMIT 1`)
	}
	{
		b := New()
		b.Select(b.Q(&out1).Where("id = ?", 1).OrderBy("created_at", false).Limit(10))
		assertStringEquals(t, b.String(),
			`SELECT "id", "created_at", "updated_at" FROM "created_updated" WHERE id = 1 ORDER BY "created_at" DESC LIMIT 1`)
	}
	{
		b := New()
		b.Select(b.Q(&out3).Where("id = ?", 1).OrderBy("created_at", false).Limit(10))
		assertStringEquals(t, b.String(),
			`SELECT "id", "created_at", "updated_at" FROM "created_updated" WHERE id = 1 ORDER BY "created_at" DESC LIMIT 10`)
	}
}

func TestQuery(t *testing.T) {
	db := openTestDBConnection(t)
	defer db.Close()

	dbExec(t, db, `
		DROP TABLE IF EXISTS "test_query";
		CREATE TABLE "test_query" (
			id INT NOT NULL,
			a INT NOT NULL,
			b STRING NOT NULL,
			c TIMESTAMP NOT NULL,
			CONSTRAINT "primary" PRIMARY KEY (id ASC)
		);
		DROP TABLE IF EXISTS "test_query2";
		CREATE TABLE "test_query2" (
			id INT NOT NULL,
			d STRING NOT NULL,
			e STRING NOT NULL,
			f STRING NOT NULL,
			CONSTRAINT "primary" PRIMARY KEY (id ASC)
		)
	`)

	type TestQuery struct {
		ID int64 `db:"primary_key"`
		A  int64
		B  string
		C  time.Time
	}
	type TestQuery2 struct {
		ID int64 `db:"primary_key"`
		D  string
		E  string
		F  string
	}

	{
		b := New()
		b.Insert(&TestQuery{ID: 1, A: 1, B: "one", C: rfc3339ToTime("2015-06-02T02:00:56Z")})
		b.Insert(&TestQuery{ID: 2, A: 2, B: "two", C: rfc3339ToTime("2015-06-02T02:01:56Z")})
		b.Insert(&TestQuery{ID: 3, A: 3, B: "three", C: rfc3339ToTime("2015-06-02T02:02:56Z")})
		b.Insert(&TestQuery2{ID: 1, D: "D1", E: "E1", F: "F1"})
		b.Insert(&TestQuery2{ID: 2, D: "D2", E: "E2", F: "F2"})
		b.Insert(&TestQuery2{ID: 3, D: "D3", E: "E3", F: "F3"})
		if err := b.Exec(context.Background(), db); err != nil {
			t.Fatal(err)
		}
	}
	{
		var (
			out1 TestQuery
			out2 []TestQuery2
			out3 []TestQuery
		)
		b := New()
		b.Select(
			b.Q(&out3).Where("id > ?", 1).OrderBy("id", false),
			b.Q(&out1).Where("id = ?", 1).Limit(1),
			b.Q(&out2).OrderBy("id", true),
		)
		t.Log(b.String())
		if err := b.Query(context.Background(), db); err != nil {
			t.Fatal(err)
		}
		expected1 := TestQuery{ID: 1, A: 1, B: "one", C: rfc3339ToTime("2015-06-02T02:00:56+00:00")}
		expected2 := []TestQuery2{
			TestQuery2{ID: 1, D: "D1", E: "E1", F: "F1"},
			TestQuery2{ID: 2, D: "D2", E: "E2", F: "F2"},
			TestQuery2{ID: 3, D: "D3", E: "E3", F: "F3"},
		}
		expected3 := []TestQuery{
			TestQuery{ID: 3, A: 3, B: "three", C: rfc3339ToTime("2015-06-02T02:02:56+00:00")},
			TestQuery{ID: 2, A: 2, B: "two", C: rfc3339ToTime("2015-06-02T02:01:56+00:00")},
		}
		if !reflect.DeepEqual(out1, expected1) {
			t.Errorf("equality expected, got: %v != %v", out1, expected1)
		}
		if !reflect.DeepEqual(out2, expected2) {
			t.Errorf("equality expected, got: %v != %v", out2, expected2)
		}
		if !reflect.DeepEqual(out3, expected3) {
			t.Errorf("equality expected, got: %v != %v", out3, expected3)
		}
		t.Logf("%v", out1)
		t.Logf("%v", out2)
		t.Logf("%v", out3)
	}
}

func TestArrayInWhereExpr(t *testing.T) {
	db := openTestDBConnection(t)
	defer db.Close()

	dbExec(t, db, `
		DROP TABLE IF EXISTS "test_arr";
		CREATE TABLE "test_arr" (
			id INT NOT NULL,
			a STRING NOT NULL,
			CONSTRAINT "primary" PRIMARY KEY (id ASC)
		);
	`)

	type TestArr struct {
		ID int64 `db:"primary_key"`
		A  string
	}

	{
		b := New()
		b.Insert(&TestArr{ID: 1, A: "one"})
		b.Insert(&TestArr{ID: 2, A: "two"})
		b.Insert(&TestArr{ID: 3, A: "three"})
		b.Insert(&TestArr{ID: 4, A: "four"})
		b.Insert(&TestArr{ID: 5, A: "five"})
		b.Insert(&TestArr{ID: 6, A: "six"})
		if err := b.Exec(context.Background(), db); err != nil {
			t.Fatal(err)
		}
	}
	{
		var out []TestArr
		b := New()
		b.Select(
			b.Q(&out).Where("id in (?)", []int64{1, 2, 3}),
			b.Q(&out).Where("a in (?)", []string{"one", "two"}),
		)
		assertStringEquals(t, b.String(), `SELECT "id", "a" FROM "test_arr" WHERE id in (1, 2, 3); SELECT "id", "a" FROM "test_arr" WHERE a in ('one', 'two')`)
		if err := b.Query(context.Background(), db); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEmptyResults(t *testing.T) {
	db := openTestDBConnection(t)
	defer db.Close()

	dbExec(t, db, `
		DROP TABLE IF EXISTS "test_a";
		CREATE TABLE "test_a" (
			id INT NOT NULL,
			CONSTRAINT "primary" PRIMARY KEY (id ASC)
		);
		DROP TABLE IF EXISTS "test_b";
		CREATE TABLE "test_b" (
			id INT NOT NULL,
			CONSTRAINT "primary" PRIMARY KEY (id ASC)
		);
		DROP TABLE IF EXISTS "test_c";
		CREATE TABLE "test_c" (
			id INT NOT NULL,
			CONSTRAINT "primary" PRIMARY KEY (id ASC)
		);
	`)
	type TestA struct {
		ID int64 `db:"primary_key"`
	}
	type TestB struct {
		ID int64 `db:"primary_key"`
	}
	type TestC struct {
		ID int64 `db:"primary_key"`
	}
	{
		b := New()
		b.Insert(&TestC{ID: 1})
		b.Insert(&TestC{ID: 2})
		b.Insert(&TestC{ID: 3})
		if err := b.Exec(context.Background(), db); err != nil {
			t.Fatal(err)
		}
	}
	{
		var (
			outa []TestA
			outb []TestB
			outc []TestC
		)
		b := New()
		b.Select(
			b.Q(&outa),
			b.Q(&outb),
			b.Q(&outc),
		)
		if err := b.Query(context.Background(), db); err != nil {
			t.Fatal(err)
		}
		if len(outa) != 0 || len(outb) != 0 || len(outc) != 3 {
			t.Errorf("expected length of third to be 3, got: %d %d %d", len(outa), len(outb), len(outc))
		}
	}
}

func TestCustomTable(t *testing.T) {
	type Foo struct {
		A int
		B int
	}
	var out Foo
	b := New()
	b.Select(b.Q(&out).Table("bar"))
	assertStringEquals(t, b.String(), `SELECT "a", "b" FROM "bar" LIMIT 1`)
}

func TestQRaw(t *testing.T) {
	type Foo struct {
		A int
		B int
	}
	var out Foo
	b := New()
	b.Select(b.Q(&out).Raw(`SELECT :columns: FROM :table: WHERE a = ? AND b = ?`, 5, 10))
	assertStringEquals(t, b.String(), `SELECT "a", "b" FROM "foo" WHERE a = 5 AND b = 10`)
}
