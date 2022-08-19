package sqlbatch

import (
	"database/sql"
	"reflect"
	"testing"
	"time"
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
	db, err := sql.Open("postgres", "host=127.0.0.1 port=26257 user=root dbname=test sslmode=disable")
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

func dbScanSingleRow(t *testing.T, db *sql.DB, query string, vals ...any) {
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

func assertStringEquals(t *testing.T, v, expected string) {
	if v != expected {
		t.Errorf("string values mismatch:\ngot:\n%s\nexpected:\n%s", v, expected)
	}
}

func assertDeepEquals(t *testing.T, a, b any) {
	if !reflect.DeepEqual(a, b) {
		t.Errorf("equality expected, got: %v != %v", a, b)
	}
}
