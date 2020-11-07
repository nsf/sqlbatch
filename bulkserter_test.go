package sqlbatch

import (
	"testing"
)

func TestBulkInserterAddMany(t *testing.T) {
	type TestStruct struct {
		ID int64 `db:"primary_key"`
		A  int
		B  int
	}
	b := New()
	b.Insert([]TestStruct{
		{ID: 1, A: 111, B: 1111},
		{ID: 2, A: 222, B: 2222},
		{ID: 3, A: 333, B: 3333},
	})
	assertStringEquals(t, b.String(), `INSERT INTO "test_struct" ("id", "a", "b") VALUES (1, 111, 1111), (2, 222, 2222), (3, 333, 3333) RETURNING NOTHING`)
}
