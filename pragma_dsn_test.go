package main

import (
	"strings"
	"testing"

	ha "github.com/litesql/go-ha"
)

// TestWithPragmasForwardsToDriverDSN proves the tuning base-ha used to apply via
// a driver-typed connection hook is now carried on the DSN and forwarded, intact,
// by litesql/go-ha to its underlying pure-Go SQLite driver. go-ha splits an HA
// name into (driver DSN, HA options) and passes unrecognized query params —
// our _pragma= set — straight through to the driver, which runs each as
// PRAGMA name=value on every new connection. This is what lets base-ha drop its
// direct low-level SQLite driver import while keeping identical per-connection tuning.
func TestWithPragmasForwardsToDriverDSN(t *testing.T) {
	const path = "/var/base/data.db"

	dsn, opts, err := ha.NameToOptions(withPragmas(path))
	if err != nil {
		t.Fatalf("NameToOptions: %v", err)
	}
	if len(opts) != 0 {
		t.Fatalf("pragmas must not be consumed as HA options, got %d", len(opts))
	}
	if !strings.HasPrefix(dsn, path+"?") {
		t.Fatalf("driver DSN lost the db path: %q", dsn)
	}
	for _, p := range sqlitePragmas {
		if want := "_pragma=" + p; !strings.Contains(dsn, want) {
			t.Errorf("driver DSN missing %q (dsn=%s)", want, dsn)
		}
	}
}

// TestWithPragmasAppendsToExistingQuery ensures withPragmas extends, not clobbers,
// a path that already carries a query string.
func TestWithPragmasAppendsToExistingQuery(t *testing.T) {
	if got := withPragmas("data.db?_txlock=immediate"); !strings.HasPrefix(got, "data.db?_txlock=immediate&_pragma=") {
		t.Fatalf("existing query not preserved: %q", got)
	}
}
