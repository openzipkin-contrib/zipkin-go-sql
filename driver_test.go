package zipkinsql

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	zipkin "github.com/openzipkin/zipkin-go"
	zipkinreporter "github.com/openzipkin/zipkin-go/reporter/recorder"
)

func createDB(t *testing.T, opts ...TraceOption) (*sql.DB, *zipkinreporter.ReporterRecorder) {
	reporter := zipkinreporter.NewReporter()
	tracer, _ := zipkin.NewTracer(reporter)

	driverName, err := Register("sqlite3", tracer, opts...)
	if err != nil {
		t.Fatalf("unable to register driver")
	}

	db, err := sql.Open(driverName, "file:test.db?cache=shared&mode=memory")
	if err != nil {
		t.Fatal(err)
	}

	return db, reporter
}

type testCase struct {
	opts          []TraceOption
	expectedSpans int
}

func TestQuerySuccess(t *testing.T) {
	ctx := context.Background()
	testCases := []testCase{
		{[]TraceOption{WithAllowRootSpan(false)}, 0},
		{[]TraceOption{WithAllowRootSpan(true)}, 1},
	}
	for _, c := range testCases {
		db, recorder := createDB(t, c.opts...)

		rows, err := db.QueryContext(ctx, "SELECT 1")
		if err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}
		defer rows.Close()

		for rows.Next() {
			var n int
			if err = rows.Scan(&n); err != nil {
				t.Fatalf("unexpected error: %s", err.Error())
			}
		}
		if err = rows.Err(); err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}

		spans := recorder.Flush()
		if want, have := c.expectedSpans, len(spans); want != have {
			t.Fatalf("unexpected number of spans, want: %d, have: %d", want, have)
		}

		if c.expectedSpans > 0 {
			if want, have := "sql/query", spans[0].Name; want != have {
				t.Fatalf("unexpected span name, want: %s, have: %s", want, have)
			}
		}

		db.Close()
		recorder.Close()
	}
}

func TestExecSuccess(t *testing.T) {
	ctx := context.Background()

	testCases := []testCase{
		{[]TraceOption{WithAllowRootSpan(false)}, 0},
		{[]TraceOption{WithAllowRootSpan(true)}, 1},
		{[]TraceOption{WithAllowRootSpan(true), WithLastInsertIDSpan(true)}, 2},
		{[]TraceOption{WithAllowRootSpan(true), WithRowsAffectedSpan(true)}, 2},
		{[]TraceOption{WithAllowRootSpan(true), WithLastInsertIDSpan(true), WithRowsAffectedSpan(true)}, 3},
	}
	for _, c := range testCases {
		db, recorder := createDB(t, c.opts...)

		sqlStmt := `
		create table foo (id integer not null primary key, name text);
		delete from foo;
	`

		res, err := db.ExecContext(ctx, sqlStmt)
		if err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}

		_, err = res.LastInsertId()
		if err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}

		_, err = res.RowsAffected()
		if err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}

		spans := recorder.Flush()
		if want, have := c.expectedSpans, len(spans); want != have {
			t.Fatalf("unexpected number of spans, want: %d, have: %d", want, have)
		}

		if c.expectedSpans > 0 {
			if want, have := "sql/exec", spans[0].Name; want != have {
				t.Fatalf("unexpected span name, want: %s, have: %s", want, have)
			}
		}

		db.Close()
		recorder.Close()
	}
}
