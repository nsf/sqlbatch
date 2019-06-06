package sqlbatch

import (
	"testing"
)

func TestBulkInsert(t *testing.T) {
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
	assertStringEquals(t, b.Query(), `INSERT INTO "test_struct" ("id", "a", "b") VALUES (0, 0, 0), (1, 1, 1), (2, 2, 2)`)
}
