// build+ ignore

package integration_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	zipkinsql "github.com/jcchavezs/zipkin-instrumentation-sql"
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/reporter/recorder"
	"gotest.tools/assert"

	"database/sql/driver"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	driverName       string
	driver           driver.Driver
	dsn              string
	createTableQuery string
	createUserQuery  string
	getUserQuery     string
	deleteUserQuery  string
}

var (
	postgresTestCase = testCase{
		driverName: "postgres",
		driver:     &pq.Driver{},
		dsn:        "postgres://test_user:test_pass@localhost/test_db?sslmode=disable",
	}
	mysqlTestCase = testCase{
		driverName: "mysql",
		driver:     &mysql.MySQLDriver{},
		dsn:        "test_user:test_pass@/test_db",
	}
)

const maxPingRetries = 5

func TestDriver(t *testing.T) {
	tCases := []testCase{
		postgresTestCase,
		mysqlTestCase,
	}

	rec := recorder.NewReporter()
	tracer, _ := zipkin.NewTracer(rec, zipkin.WithSampler(zipkin.AlwaysSample))

	for _, tCase := range tCases {
		t.Run("Testing driver "+tCase.driverName, func(t *testing.T) {
			driver := zipkinsql.Wrap(tCase.driver, tracer, zipkinsql.WithAllTraceOptions())
			driverName := "traced-" + tCase.driverName
			sql.Register(driverName, driver)
			db, err := sql.Open(driverName, tCase.dsn)
			require.NoError(t, err)
			db.SetMaxIdleConns(0)
			for i := 0; i < maxPingRetries; i++ {
				if err = db.Ping(); err == nil {
					break
				}
				if i == maxPingRetries-1 {
					t.Fatalf("failed to ping the database: %v\n", err)
				}
				time.Sleep(time.Duration(i+1) * time.Millisecond)
			}
			ctx := context.Background()

			row := db.QueryRowContext(ctx, "SELECT 1")
			res := 0
			err = row.Scan(&res)
			require.NoError(t, err)
			assert.Equal(t, 1, res)

			spans := rec.Flush()
			assert.Equal(t, 1, len(spans))

			s := spans[0]
			assert.Equal(t, "sql/query", s.Name)
			assert.Equal(t, "SELECT 1", s.Tags["sql.query"])
		})
	}
}

func TestSQLX(t *testing.T) {
	rec := recorder.NewReporter()
	tracer, _ := zipkin.NewTracer(rec, zipkin.WithSampler(zipkin.AlwaysSample))

	driverName, err := zipkinsql.Register("postgres", tracer, zipkinsql.WithAllTraceOptions())
	require.NoError(t, err)

	db, err := sql.Open(driverName, postgresTestCase.dsn)
	require.NoError(t, err)
	db.SetMaxIdleConns(0)

	ctx := context.Background()
	dbx := sqlx.NewDb(db, "postgres")
	row := dbx.QueryRowContext(ctx, "SELECT 2")
	res := 0
	err = row.Scan(&res)
	require.NoError(t, err)
	assert.Equal(t, 2, res)
	spans := rec.Flush()
	assert.Equal(t, 1, len(spans))

	s := spans[0]
	assert.Equal(t, "sql/query", s.Name)
	assert.Equal(t, "SELECT 2", s.Tags["sql.query"])
}
