package store

import (
	"os"
	"testing"
	"time"
)

// testDSN reads PG_TEST_DSN; integration tests skip when it is unset.
func testDSN() string {
	return os.Getenv("PG_TEST_DSN")
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dsn := testDSN()
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set; skipping PostgreSQL integration test")
	}
	return NewIsolatedTestStore(t, dsn, "test_store")
}

func at(hour int) time.Time {
	return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).
		Add(time.Duration(hour) * time.Hour)
}

func itoaTest(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
