package sqlbatch

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"reflect"
	"testing"
	"time"
	"unsafe"
)

func TestBatchInsert(t *testing.T) {
	type FooBar struct {
		ID        int
		Foo       string
		Bar       string
		CreatedAt time.Time
	}

	b := New()
	b.Insert(&FooBar{ID: 1, Foo: "foo 1", Bar: "bar 1", CreatedAt: time.Now()})
	b.Insert(&FooBar{ID: 2, Foo: "foo 2", Bar: "bar 2", CreatedAt: time.Now()})
	query := b.Query()
	t.Log(query)

	db, err := sql.Open("postgres", "host=127.0.0.1 port=26257 user=root dbname=test sslmode=disable binary_parameters=yes")
	if err != nil {
		t.Fatalf("db connection error: %s", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		DROP TABLE IF EXISTS "foo_bar";
		CREATE TABLE foo_bar (
			id INT NOT NULL,
			foo STRING NOT NULL,
			bar STRING NOT NULL,
			created_at TIMESTAMP NOT NULL,
			CONSTRAINT "primary" PRIMARY KEY (id ASC)
		)
	`)
	if err != nil {
		t.Fatalf("db exec error: %s", err)
	}

	_, err = db.Exec(query)
	if err != nil {
		t.Fatalf("db exec error: %s", err)
	}
}

func rfc3339ToTime(v string) time.Time {
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		panic(err)
	}
	return t
}

func TestGetStructInfo(t *testing.T) {
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
	}
	ptr := unsafe.Pointer(v)
	si := GetStructInfo(reflect.TypeOf(v).Elem(), nil, nil)

	assertField := func(idx *int, fs []FieldInfo, name string, val interface{}, primaryKey bool) {
		f := fs[*idx]
		*idx++
		if f.Name != name {
			t.Errorf("field %q name mismatch: %q != %q", f.Name, f.Name, name)
		}

		var fval interface{}
		f.GetValue(ptr, &fval)
		if !reflect.DeepEqual(fval, val) {
			t.Errorf("field %q value mismatch: %v != %v", f.Name, fval, val)
		}

		if f.PrimaryKey != primaryKey {
			t.Errorf("field %q primary key mismatch: %v != %v", f.Name, f.PrimaryKey, primaryKey)
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

	assertField(&idx, si.Fields, "foo", float32(0), false)

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
