// build+ ignore

package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	zipkinsql "github.com/openzipkin-contrib/zipkin-go-sql"

	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/reporter/recorder"

	"database/sql/driver"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type testCase struct {
	driverName string
	driver     driver.Driver
	dsn        string
}

var (
	postgresTestCase = testCase{
		driverName: "postgres",
		driver:     &pq.Driver{},
		dsn:        "postgres://test_user:test_pass@localhost/test_db?sslmode=disable",
	}
)

const maxPingRetries = 5

func TestDriver(t *testing.T) {
	tCases := []testCase{
		postgresTestCase,
	}

	rec := recorder.NewReporter()
	tracer, _ := zipkin.NewTracer(rec, zipkin.WithSampler(zipkin.AlwaysSample))

	for _, tCase := range tCases {
		t.Run("Testing driver "+tCase.driverName, func(t *testing.T) {
			driver := zipkinsql.Wrap(tCase.driver, tracer, zipkinsql.WithAllTraceOptions())
			driverName := "traced-" + tCase.driverName
			sql.Register(driverName, driver)
			db, err := sql.Open(driverName, tCase.dsn)
			if err != nil {
				t.Fatalf("failed to open a DB: %v\n", err)
			}

			db.SetConnMaxLifetime(5 * time.Second)
			defer db.Close()

			for i := 0; i < maxPingRetries; i++ {
				if err = db.Ping(); err == nil {
					break
				}
				if i == maxPingRetries-1 {
					t.Fatalf("failed to ping the database: %v\n", err)
				}
				time.Sleep(time.Duration(i+1) * 200 * time.Millisecond)
			}
			ctx := context.Background()

			row := db.QueryRowContext(ctx, "SELECT 1")
			res := 0
			if err := row.Scan(&res); err != nil {
				t.Fatalf("failed to scan the result: %v\n", err)
			}

			if want, have := 1, res; want != have {
				t.Errorf("incorrect result: want %d, have: %d", want, have)
			}

			spans := rec.Flush()
			if want, have := 1, len(spans); want != have {
				t.Errorf("incorrect number of spans: want %d, have: %d", want, have)
			}

			s := spans[0]
			if want, have := "sql/query", s.Name; want != have {
				t.Errorf("incorrect span name: want %q, have: %q", want, have)
			}

			if want, have := "SELECT 1", s.Tags["sql.query"]; want != have {
				t.Errorf("incorrect tag: want %q, have: %q", want, have)
			}
		})
	}
}

func TestSQLX(t *testing.T) {
	rec := recorder.NewReporter()
	tracer, _ := zipkin.NewTracer(rec, zipkin.WithSampler(zipkin.AlwaysSample))

	driverName, err := zipkinsql.Register("postgres", tracer, zipkinsql.WithAllTraceOptions())
	if err != nil {
		t.Fatalf("failed to register the driver: %v\n", err)
	}

	db, err := sql.Open(driverName, postgresTestCase.dsn)
	if err != nil {
		t.Fatalf("failed to open a DB: %v\n", err)
	}

	db.SetConnMaxLifetime(5 * time.Second)
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatalf("failed to ping the database: %v\n", err)
	}

	ctx := context.Background()
	dbx := sqlx.NewDb(db, "postgres")
	row := dbx.QueryRowContext(ctx, "SELECT 2")
	res := 0
	if err := row.Scan(&res); err != nil {
		t.Fatalf("failed to scan the result: %v\n", err)
	}

	if want, have := 2, res; want != have {
		t.Errorf("incorrect result: want %d, have: %d", want, have)
	}

	spans := rec.Flush()
	if want, have := 1, len(spans); want != have {
		t.Errorf("incorrect number of spans: want %d, have: %d", want, have)
	}

	s := spans[0]
	if want, have := "sql/query", s.Name; want != have {
		t.Errorf("incorrect span name of spans: want %q, have: %q", want, have)
	}

	if want, have := "SELECT 2", s.Tags["sql.query"]; want != have {
		t.Errorf("incorrect tag: want %q, have: %q", want, have)
	}
}
