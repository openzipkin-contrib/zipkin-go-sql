# Zipkin instrumentation SQL

A sql wrapper including Zipkin instrumentation

## Usage

```go
import (
    _ "github.com/go-sql-driver/mysql"
    zipkinsql "github.com/jcchavezs/zipkin-instrumentation-sql"
)

var (
    driverName string
    err        error
    db         *sql.DB
)

// Register our zipkinsql wrapper for the provided MySQL driver.
driverName, err = zipkinsql.Register("mysql", zipkinsql.WithAllTraceOptions())
if err != nil {
    log.Fatalf("unable to register zipkin driver: %v\n", err)
}

// Connect to a MySQL database using the ocsql driver wrapper.
db, err = sql.Open(driverName, "myDSN")
```