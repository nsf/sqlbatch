package sqlbatch

import (
	"bytes"
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

func rfc3339ToTime(v string) time.Time {
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		panic(err)
	}
	return t
}

func rfc3339NanoToTime(v string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		panic(err)
	}
	return t
}

func openTestDBConnection(t *testing.T) *sql.DB {
	db, err := sql.Open("postgres", "host=127.0.0.1 port=26257 user=root dbname=test sslmode=disable binary_parameters=yes")
	if err != nil {
		t.Fatalf("db connection error: %s", err)
	}
	return db
}

func dbExec(t *testing.T, db *sql.DB, query string) {
	_, err := db.Exec(query)
	if err != nil {
		t.Fatalf("db exec error: %s", err)
	}
}

func dbScanSingleRow(t *testing.T, db *sql.DB, query string, vals ...interface{}) {
	rows, err := db.Query(query)
	if err != nil {
		t.Errorf("select failure: %s", err)
		return
	}
	defer rows.Close()
	if !rows.Next() {
		t.Errorf("zero rows")
		return
	}
	if err := rows.Scan(vals...); err != nil {
		t.Errorf("scan failure: %s", err)
		return
	}
}

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
	query := b.Query()
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
			dbExec(t, db, New().Insert(&StringFormat{Key: 1, Value: string([]byte{byte(i)})}).Query())

			var s string
			dbScanSingleRow(t, db, "SELECT value FROM string_format WHERE key = 1", &s)
			if !utf8.ValidString(s) {
				t.Errorf("invalid string")
			}

			dbExec(t, db, New().Delete(&StringFormat{Key: 1}).Query())
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
			dbExec(t, db, New().Insert(&BytesFormat{Key: 1, Value: []byte{byte(i)}}).Query())

			var b []byte
			dbScanSingleRow(t, db, "SELECT value FROM bytes_format WHERE key = 1", &b)
			if len(b) != 1 && bytes.Equal(b, []byte{byte(i)}) {
				t.Errorf("invalid bytes")
			}

			dbExec(t, db, New().Delete(&BytesFormat{Key: 1}).Query())
		})
	}

	t.Run("MultipleBytes", func(t *testing.T) {
		dbExec(t, db, New().Insert(&BytesFormat{Key: 1, Value: []byte("проверка")}).Query())

		var b []byte
		dbScanSingleRow(t, db, "SELECT value FROM bytes_format WHERE key = 1", &b)
		if string(b) != "проверка" {
			t.Errorf("invalid bytes")
		}

		dbExec(t, db, New().Delete(&BytesFormat{Key: 1}).Query())
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
			Query())

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
			Query())

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
			Query())

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
			Query())

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

		if f.PrimaryKey != primaryKey {
			t.Errorf("field %q primary key mismatch: %v != %v", f.Name, f.PrimaryKey, primaryKey)
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

func TestTagParse(t *testing.T) {
	cases := []struct {
		v        string
		expected tagInfo
	}{
		{"", tagInfo{}},
		{"primary_key", tagInfo{primaryKey: true}},
		{"foo", tagInfo{}},
		{"-", tagInfo{ignore: true}},
		{"column:foo", tagInfo{name: "foo"}},
		{"group:bar", tagInfo{group: "bar"}},
		{"column:foo,primary_key", tagInfo{name: "foo", primaryKey: true}},
		{"primary_key,column:foo", tagInfo{name: "foo", primaryKey: true}},
		{"primary_key,column:foo,-", tagInfo{name: "foo", primaryKey: true, ignore: true}},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			v := parseTag(c.v)
			if v != c.expected {
				t.Fail()
			}
		})
	}
}
