package sqlbatch

import (
	"testing"
)

func TestBulkInserterAdd(t *testing.T) {
	type TestStruct struct {
		ID int64 `db:"primary_key"`
		A  int
		B  int
	}
	b := New()
	b.WithBulkInserter(func(bulk *BulkInserter) {
		for i := 0; i < 3; i++ {
			bulk.Add(&TestStruct{ID: int64(i), A: i, B: i})
		}
	})
	assertStringEquals(t, b.Query(), `INSERT INTO "test_struct" ("id", "a", "b") VALUES (0, 0, 0), (1, 1, 1), (2, 2, 2) RETURNING NOTHING`)
}

func TestBulkInserterAddMany(t *testing.T) {
	type TestStruct struct {
		ID int64 `db:"primary_key"`
		A  int
		B  int
	}
	b := New()
	b.BulkInsert([]TestStruct{
		{ID: 1, A: 111, B: 1111},
		{ID: 2, A: 222, B: 2222},
		{ID: 3, A: 333, B: 3333},
	})
	assertStringEquals(t, b.Query(), `INSERT INTO "test_struct" ("id", "a", "b") VALUES (1, 111, 1111), (2, 222, 2222), (3, 333, 3333) RETURNING NOTHING`)
}
