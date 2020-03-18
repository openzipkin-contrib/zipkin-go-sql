# Zipkin instrumentation SQL

[![Build Status](https://travis-ci.com/jcchavezs/zipkin-instrumentation-sql.svg?branch=master)](https://travis-ci.com/jcchavezs/zipkin-instrumentation-sql)
[![Go Report Card](https://goreportcard.com/badge/github.com/jcchavezs/zipkin-instrumentation-sql)](https://goreportcard.com/report/github.com/jcchavezs/zipkin-instrumentation-sql)
[![GoDoc](https://godoc.org/github.com/jcchavezs/zipkin-instrumentation-sql?status.svg)](https://godoc.org/github.com/jcchavezs/zipkin-instrumentation-sql)

A SQL wrapper including Zipkin instrumentation

## Usage

```go
import (
    _ "github.com/go-sql-driver/mysql"
    zipkinsql "github.com/jcchavezs/zipkin-instrumentation-sql"
    zipkin "github.com/openzipkin/zipkin-go"
)

var (
    driverName string
    err        error
    db         *sql.DB
    tracer     *zipkin.Tracer
)

// Register our zipkinsql wrapper for the provided MySQL driver.
driverName, err = zipkinsql.Register("mysql", tracer, zipkinsql.WithAllTraceOptions())
if err != nil {
    log.Fatalf("unable to register zipkin driver: %v\n", err)
}

// Connect to a MySQL database using the zipkinsql driver wrapper.
db, err = sql.Open(driverName, "postgres://user:pass@127.0.0.1:5432/db")
```

You can also wrap your own driver with zipkin instrumentation as follows:

```go

import (
    mysql "github.com/go-sql-driver/mysql"
    zipkinsql "github.com/jcchavezs/zipkin-instrumentation-sql"
    zipkinmodel "github.com/openzipkin/zipkin-go/model"
)

var (
    driver driver.Driver
    err    error
    db     *sql.DB
    tracer *zipkin.Tracer
)

// Explicitly wrap the MySQL driver with zipkinsql
driver = zipkinsql.Wrap(
    &mysql.MySQLDriver{},
    tracer,
    zipkinsql.WithRemoteEndpoint(zipkinmodel.Endpoint{
        ServiceName: "resultsdb",
        Port: 5432
    }),
)

// Register our zipkinsql wrapper as a database driver
sql.Register("zipkinsql-mysql", driver)

// Connect to a MySQL database using the zipkinsql driver wrapper
db, err = sql.Open("zipkinsql-mysql", "postgres://user:pass@127.0.0.1:5432/db")
```

Projects providing their own abstractions on top of database/sql/driver can also wrap an existing driver.Conn interface directly with zipkinsql.

```go
import zipkinsql "github.com/jcchavezs/zipkin-instrumentation-sql"

func initializeConn(...) driver.Conn {
    // create custom driver.Conn
    conn := Connect(...)

    // wrap with zipkinsql
    return zipkinsql.WrapConn(conn, tracer, zipkinsql.WithAllTraceOptions())
}
```

Go 1.10+ provides a new driver.Connector interface that can be 
wrapped  directly by zipkinsql without the need for zipkinsql to
register a driver.Driver.

Example:

```go
import(
    zipkinsql "github.com/jcchavezs/zipkin-instrumentation-sql"
    "github.com/lib/pq"
)
var (
    connector driver.Connector
    err       error
    db        *sql.DB
    tracer *zipkin.Tracer
)

connector, err = pq.NewConnector("postgres://user:pass@host:5432/db")
if err != nil {
    log.Fatalf("unable to create postgres connector: %v\n", err)
}
// Wrap the driver.Connector with ocsql.
connector = zipkinsql.WrapConnector(connector, tracer, zipkinsql.WithAllTraceOptions())
// Use the wrapped driver.Connector.
db = sql.OpenDB(connector)
```

## Using jmoiron/sqlx

If using the `sqlx` library with named queries you will need to use the
`sqlx.NewDb` function to wrap an existing `*sql.DB` connection. `sqlx.Open` and `sqlx.Connect` methods won't work.

First create a `*sql.DB` connection and then create a `*sqlx.DB` connection by wrapping the former and **keeping the same driver name** e.g.:

```go
driverName, err := zipkinsql.Register("postgres", zipkinsql.WithAllTraceOptions())
if err != nil { ... }

db, err := sql.Open(driverName, "postgres://localhost:5432/my_database")
if err != nil { ... }

// Keep the driver name!
dbx := sqlx.NewDB(db, "postgres")
```

## Usage of *Context methods

Instrumentation is possible if the context is being passed downstream in methods.
This is not only for instrumentation purposes but also a [good practice](https://medium.com/@cep21/how-to-correctly-use-context-context-in-go-1-7-8f2c0fafdf39) in go programming. `database/sql` package exposes already a set of methods that receive the context as first paramenter:

- `*DB.Begin` -> `*DB.BeginTx`
- `*DB.Exec` -> `*DB.ExecContext`
- `*DB.Ping` -> `*DB.PingContext`
- `*DB.Prepare` -> `*DB.PrepareContext`
- `*DB.Query` -> `*DB.QueryContext`
- `*DB.QueryRow` -> `*DB.QueryRowContext`
- `*Stmt.Exec` -> `*Stmt.ExecContext`
- `*Stmt.Query` -> `*Stmt.QueryContext`
- `*Stmt.QueryRow` -> `*Stmt.QueryRowContext`
- `*Tx.Exec` -> `*Tx.ExecContext`
- `*Tx.Prepare` -> `*Tx.PrepareContext`
- `*Tx.Query` -> `*Tx.QueryContext`
- `*Tx.QueryRow` -> `*Tx.QueryRowContext`
