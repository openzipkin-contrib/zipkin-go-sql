// +build go1.10

package zipkinsql

import (
	"context"
	"database/sql"
	"database/sql/driver"

	zipkin "github.com/openzipkin/zipkin-go"
)

var errConnDone = sql.ErrConnDone

// Compile time assertion
var (
	_ driver.DriverContext = &zDriver{}
	_ driver.Connector     = &zDriver{}
)

// WrapConnector allows wrapping a database driver.Connector which eliminates
// the need to register zipkinsql as an available driver.Driver.
func WrapConnector(dc driver.Connector, t *zipkin.Tracer, options ...TraceOption) driver.Connector {
	opts := TraceOptions{}
	for _, o := range options {
		o(&opts)
	}

	return &zDriver{
		parent:    dc.Driver(),
		connector: dc,
		tracer:    t,
		options:   opts,
	}
}

// zDriver implements driver.Driver
type zDriver struct {
	parent    driver.Driver
	connector driver.Connector
	tracer    *zipkin.Tracer
	options   TraceOptions
}

func wrapDriver(d driver.Driver, t *zipkin.Tracer, o TraceOptions) driver.Driver {
	if _, ok := d.(driver.DriverContext); ok {
		return zDriver{parent: d, tracer: t, options: o}
	}
	return struct{ driver.Driver }{zDriver{parent: d, tracer: t, options: o}}
}

func wrapConn(parent driver.Conn, t *zipkin.Tracer, options TraceOptions) driver.Conn {
	var (
		n, hasNameValueChecker = parent.(driver.NamedValueChecker)
		s, hasSessionResetter  = parent.(driver.SessionResetter)
	)
	c := &zConn{parent: parent, tracer: t, options: options}
	switch {
	case !hasNameValueChecker && !hasSessionResetter:
		return c
	case hasNameValueChecker && !hasSessionResetter:
		return struct {
			conn
			driver.NamedValueChecker
		}{c, n}
	case !hasNameValueChecker && hasSessionResetter:
		return struct {
			conn
			driver.SessionResetter
		}{c, s}
	case hasNameValueChecker && hasSessionResetter:
		return struct {
			conn
			driver.NamedValueChecker
			driver.SessionResetter
		}{c, n, s}
	}
	panic("unreachable")
}

func wrapStmt(stmt driver.Stmt, query string, tracer *zipkin.Tracer, options TraceOptions) driver.Stmt {
	var (
		_, hasExeCtx    = stmt.(driver.StmtExecContext)
		_, hasQryCtx    = stmt.(driver.StmtQueryContext)
		c, hasColConv   = stmt.(driver.ColumnConverter)
		n, hasNamValChk = stmt.(driver.NamedValueChecker)
	)

	s := zStmt{parent: stmt, query: query, tracer: tracer, options: options}
	switch {
	case !hasExeCtx && !hasQryCtx && !hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
		}{s}
	case !hasExeCtx && hasQryCtx && !hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
		}{s, s}
	case hasExeCtx && !hasQryCtx && !hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
		}{s, s}
	case hasExeCtx && hasQryCtx && !hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
		}{s, s, s}
	case !hasExeCtx && !hasQryCtx && hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.ColumnConverter
		}{s, c}
	case !hasExeCtx && hasQryCtx && hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
			driver.ColumnConverter
		}{s, s, c}
	case hasExeCtx && !hasQryCtx && hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.ColumnConverter
		}{s, s, c}
	case hasExeCtx && hasQryCtx && hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
			driver.ColumnConverter
		}{s, s, s, c}

	case !hasExeCtx && !hasQryCtx && !hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.NamedValueChecker
		}{s, n}
	case !hasExeCtx && hasQryCtx && !hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
			driver.NamedValueChecker
		}{s, s, n}
	case hasExeCtx && !hasQryCtx && !hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.NamedValueChecker
		}{s, s, n}
	case hasExeCtx && hasQryCtx && !hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
			driver.NamedValueChecker
		}{s, s, s, n}
	case !hasExeCtx && !hasQryCtx && hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.ColumnConverter
			driver.NamedValueChecker
		}{s, c, n}
	case !hasExeCtx && hasQryCtx && hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
			driver.ColumnConverter
			driver.NamedValueChecker
		}{s, s, c, n}
	case hasExeCtx && !hasQryCtx && hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.ColumnConverter
			driver.NamedValueChecker
		}{s, s, c, n}
	case hasExeCtx && hasQryCtx && hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
			driver.ColumnConverter
			driver.NamedValueChecker
		}{s, s, s, c, n}
	}
	panic("unreachable")
}

func (d zDriver) OpenConnector(name string) (driver.Connector, error) {
	var err error
	d.connector, err = d.parent.(driver.DriverContext).OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return d, err
}

func (d zDriver) Connect(ctx context.Context) (driver.Conn, error) {
	c, err := d.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &zConn{parent: c, tracer: d.tracer, options: d.options}, nil
}

func (d zDriver) Driver() driver.Driver {
	return d
}
