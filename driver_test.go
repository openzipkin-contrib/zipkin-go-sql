package zipkinsql

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	zipkin "github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
	zipkinreporter "github.com/openzipkin/zipkin-go/reporter/recorder"
)

func createDB(t *testing.T, opts ...TraceOption) (*sql.DB, *zipkin.Tracer, *zipkinreporter.ReporterRecorder) {
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

	return db, tracer, reporter
}

type testCase struct {
	opts          []TraceOption
	expectedSpans int
}

func TestQuerySuccess(t *testing.T) {
	testCases := []testCase{
		{[]TraceOption{WithAllowRootSpan(false)}, 0},
		{[]TraceOption{WithAllowRootSpan(true)}, 1},
		{[]TraceOption{WithAllowRootSpan(true), WithTagQuery(true), WithTagQueryParams(true)}, 1},
	}
	for _, c := range testCases {
		db, _, recorder := createDB(t, c.opts...)

		rows, err := db.Query("SELECT 1 WHERE 1 = ?", 1)
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

			if errMsg, ok := spans[0].Tags["error"]; ok {
				t.Fatalf("unexpected error: %s", errMsg)
			}
		}

		db.Close()
		recorder.Close()
	}
}
func TestQueryContextSuccess(t *testing.T) {
	ctx := context.Background()
	testCases := []testCase{
		{[]TraceOption{WithAllowRootSpan(false)}, 0},
		{[]TraceOption{WithAllowRootSpan(true)}, 1},
		{[]TraceOption{WithAllowRootSpan(true), WithTagQuery(true), WithTagQueryParams(true)}, 1},
	}
	for _, c := range testCases {
		db, _, recorder := createDB(t, c.opts...)

		rows, err := db.QueryContext(ctx, "SELECT 1 WHERE 1 = ?", 1)
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

			if errMsg, ok := spans[0].Tags["error"]; ok {
				t.Fatalf("unexpected error: %s", errMsg)
			}
		}

		db.Close()
		recorder.Close()
	}
}

func TestQueryContextPropagationSuccess(t *testing.T) {
	ctx := context.Background()
	db, tracer, recorder := createDB(t, WithAllowRootSpan(false))

	span, ctx := tracer.StartSpanFromContext(ctx, "root")

	rows, err := db.QueryContext(ctx, "SELECT 1 WHERE 1 = ?", 1)
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

	span.Finish()

	spans := recorder.Flush()
	if want, have := 2, len(spans); want != have {
		t.Fatalf("unexpected number of spans, want: %d, have: %d", want, have)
	}

	if want, have := "sql/query", spans[0].Name; want != have {
		t.Fatalf("unexpected span name, want: %s, have: %s", want, have)
	}

	if errMsg, ok := spans[0].Tags["error"]; ok {
		t.Fatalf("unexpected error: %s", errMsg)
	}

	db.Close()
	recorder.Close()
}

func TestExecContextSuccess(t *testing.T) {
	ctx := context.Background()

	testCases := []testCase{
		{[]TraceOption{WithAllowRootSpan(false)}, 0},
		{[]TraceOption{WithAllowRootSpan(true)}, 1},
		{[]TraceOption{WithAllowRootSpan(true), WithLastInsertIDSpan(true)}, 2},
		{[]TraceOption{WithAllowRootSpan(true), WithRowsAffectedSpan(true)}, 2},
		{[]TraceOption{WithAllowRootSpan(true), WithLastInsertIDSpan(true), WithRowsAffectedSpan(true)}, 3},
		{[]TraceOption{WithAllowRootSpan(true), WithLastInsertIDSpan(true), WithRowsAffectedSpan(true), WithTagQuery(true), WithTagQueryParams(true)}, 3},
		{[]TraceOption{WithAllowRootSpan(true), WithRemoteEndpoint(model.Endpoint{ServiceName: "myservice"})}, 1},
	}
	for _, c := range testCases {
		db, _, recorder := createDB(t, c.opts...)

		sqlStmt := `
		drop table if exists foo;
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

			if errMsg, ok := spans[0].Tags["error"]; ok {
				t.Fatalf("unexpected error: %s", errMsg)
			}
		}

		db.Close()
		recorder.Close()
	}
}

func TestTxWithCommitSuccess(t *testing.T) {
	ctx := context.Background()

	testCases := []testCase{
		{[]TraceOption{WithAllowRootSpan(false)}, 0},
		{[]TraceOption{WithAllowRootSpan(true)}, 5},
		{[]TraceOption{WithAllowRootSpan(true), WithTagQuery(true), WithTagQueryParams(true)}, 5},
	}

	for _, c := range testCases {
		db, _, recorder := createDB(t, c.opts...)

		sqlStmt := `
	drop table if exists foo;
	create table foo (id integer not null primary key, name text);
	delete from foo;
`

		_, err := db.ExecContext(ctx, sqlStmt)
		if err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}
		stmt, err := tx.Prepare("insert into foo(id, name) values(?, ?)")
		if err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}
		defer stmt.Close()
		_, err = stmt.Exec("1", "こんにちわ世界")
		if err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}
		tx.Commit()

		spans := recorder.Flush()
		if want, have := c.expectedSpans, len(spans); want != have {
			t.Fatalf("unexpected number of spans, want: %d, have: %d", want, have)
		}

		if c.expectedSpans > 0 {
			if want, have := "sql/exec", spans[0].Name; want != have {
				t.Fatalf("unexpected first span name, want: %s, have: %s", want, have)
			}
			if want, have := "sql/begin_transaction", spans[1].Name; want != have {
				t.Fatalf("unexpected second span name, want: %s, have: %s", want, have)
			}
			if want, have := "sql/prepare", spans[2].Name; want != have {
				t.Fatalf("unexpected third span name, want: %s, have: %s", want, have)
			}
			if want, have := "sql/exec", spans[3].Name; want != have {
				t.Fatalf("unexpected fourth span name, want: %s, have: %s", want, have)
			}
			if want, have := "sql/commit", spans[4].Name; want != have {
				t.Fatalf("unexpected fifth span name, want: %s, have: %s", want, have)
			}

			for i := 0; i < 5; i++ {
				if errMsg, ok := spans[i].Tags["error"]; ok {
					t.Fatalf("unexpected error: %s", errMsg)
				}
			}
		}

		db.Close()
		recorder.Close()
	}
}

func TestTxWithRollbackSuccess(t *testing.T) {
	ctx := context.Background()

	testCases := []testCase{
		{[]TraceOption{WithAllowRootSpan(false)}, 0},
		{[]TraceOption{WithAllowRootSpan(true)}, 5},
		{[]TraceOption{WithAllowRootSpan(true), WithTagQuery(true), WithTagQueryParams(true)}, 5},
	}

	for _, c := range testCases {
		db, _, recorder := createDB(t, c.opts...)

		sqlStmt := `
	drop table if exists foo;
	create table foo (id integer not null primary key, name text);
	delete from foo;
`

		_, err := db.ExecContext(ctx, sqlStmt)
		if err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}
		stmt, err := tx.Prepare("insert into foo(id, name) values(?, ?)")
		if err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}
		defer stmt.Close()
		_, err = stmt.Exec("1", "こんにちわ世界")
		tx.Rollback()

		spans := recorder.Flush()
		if want, have := c.expectedSpans, len(spans); want != have {
			t.Fatalf("unexpected number of spans, want: %d, have: %d", want, have)
		}

		if c.expectedSpans > 0 {
			if want, have := "sql/exec", spans[0].Name; want != have {
				t.Fatalf("unexpected first span name, want: %s, have: %s", want, have)
			}
			if want, have := "sql/begin_transaction", spans[1].Name; want != have {
				t.Fatalf("unexpected second span name, want: %s, have: %s", want, have)
			}
			if want, have := "sql/prepare", spans[2].Name; want != have {
				t.Fatalf("unexpected third span name, want: %s, have: %s", want, have)
			}
			if want, have := "sql/exec", spans[3].Name; want != have {
				t.Fatalf("unexpected fourth span name, want: %s, have: %s", want, have)
			}
			if want, have := "sql/rollback", spans[4].Name; want != have {
				t.Fatalf("unexpected fifth span name, want: %s, have: %s", want, have)
			}

			for i := 0; i < c.expectedSpans; i++ {
				if errMsg, ok := spans[i].Tags["error"]; ok {
					t.Fatalf("unexpected error: %s", errMsg)
				}
			}
		}

		db.Close()
		recorder.Close()
	}
}
