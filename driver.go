package zipkinsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	zipkin "github.com/openzipkin/zipkin-go"
)

type conn interface {
	driver.Pinger
	driver.Execer
	driver.ExecerContext
	driver.Queryer
	driver.QueryerContext
	driver.Conn
	driver.ConnPrepareContext
	driver.ConnBeginTx
}

var (
	// Type assertions
	_ driver.Driver = &zDriver{}
	_ conn          = &zConn{}
	_ driver.Result = &zResult{}
	_ driver.Rows   = &zRows{}
)

var (
	regMu sync.Mutex
)

// Register initializes and registers our zipkinsql wrapped database driver
// identified by its driverName and using provided TraceOptions. On success it
// returns the generated driverName to use when calling sql.Open.
// It is possible to register multiple wrappers for the same database driver if
// needing different TraceOptions for different connections.
func Register(driverName string, tracer *zipkin.Tracer, options ...TraceOption) (string, error) {
	// retrieve the driver implementation we need to wrap with instrumentation
	db, err := sql.Open(driverName, "")
	if err != nil {
		return "", err
	}
	dri := db.Driver()
	if err = db.Close(); err != nil {
		return "", err
	}

	regMu.Lock()
	defer regMu.Unlock()
	registerName := fmt.Sprintf("%s-zipkinsql-%d", driverName, len(sql.Drivers()))
	sql.Register(registerName, Wrap(dri, tracer, options...))

	return registerName, nil
}

// Wrap takes a SQL driver and wraps it with Zipkin instrumentation.
func Wrap(d driver.Driver, t *zipkin.Tracer, options ...TraceOption) driver.Driver {
	o := TraceOptions{}
	for _, option := range options {
		option(&o)
	}
	return wrapDriver(d, t, o)
}

// zipkinDriver implements driver.Driver
type zDriver struct {
	driver  driver.Driver
	tracer  *zipkin.Tracer
	options TraceOptions
}

func wrapDriver(d driver.Driver, t *zipkin.Tracer, o TraceOptions) driver.Driver {
	return &zDriver{driver: d, tracer: t, options: o}
}

func wrapConn(c driver.Conn, t *zipkin.Tracer, options TraceOptions) driver.Conn {
	return &zConn{driver: c, tracer: t, options: options}
}

func wrapStmt(stmt driver.Stmt, query string, tracer *zipkin.Tracer, options TraceOptions) driver.Stmt {
	s := zStmt{driver: stmt, query: query, options: options, tracer: tracer}
	_, hasExeCtx := stmt.(driver.StmtExecContext)
	_, hasQryCtx := stmt.(driver.StmtQueryContext)
	c, hasColCnv := stmt.(driver.ColumnConverter)
	switch {
	case !hasExeCtx && !hasQryCtx && !hasColCnv:
		return struct {
			driver.Stmt
		}{s}
	case !hasExeCtx && hasQryCtx && !hasColCnv:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
		}{s, s}
	case hasExeCtx && !hasQryCtx && !hasColCnv:
		return struct {
			driver.Stmt
			driver.StmtExecContext
		}{s, s}
	case hasExeCtx && hasQryCtx && !hasColCnv:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
		}{s, s, s}
	case !hasExeCtx && !hasQryCtx && hasColCnv:
		return struct {
			driver.Stmt
			driver.ColumnConverter
		}{s, c}
	case !hasExeCtx && hasQryCtx && hasColCnv:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
			driver.ColumnConverter
		}{s, s, c}
	case hasExeCtx && !hasQryCtx && hasColCnv:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.ColumnConverter
		}{s, s, c}
	case hasExeCtx && hasQryCtx && hasColCnv:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
			driver.ColumnConverter
		}{s, s, s, c}
	}

	panic("unreachable")
}

func (d zDriver) Open(name string) (driver.Conn, error) {
	c, err := d.driver.Open(name)
	if err != nil {
		return nil, err
	}
	return wrapConn(c, d.tracer, d.options), nil
}

// zConn implements driver.Conn
type zConn struct {
	driver  driver.Conn
	tracer  *zipkin.Tracer
	options TraceOptions
}

func (c zConn) Ping(ctx context.Context) (err error) {
	if pinger, ok := c.driver.(driver.Pinger); ok {
		err = pinger.Ping(ctx)
	}
	return
}

func (c zConn) Exec(query string, args []driver.Value) (res driver.Result, err error) {
	if exec, ok := c.driver.(driver.Execer); ok {
		return exec.Exec(query, args)
	}

	return nil, driver.ErrSkip
}

func (c zConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (res driver.Result, err error) {
	if execCtx, ok := c.driver.(driver.ExecerContext); ok {
		if zipkin.SpanFromContext(ctx) == nil {
			return execCtx.ExecContext(ctx, query, args)
		}

		span, _ := c.tracer.StartSpanFromContext(ctx, "sql/exec")
		defer span.Finish()

		if c.options.Query {
			span.Tag("sql.query", query)
		}

		setSpanDefaultTags(span, c.options.DefaultTags)

		if res, err = execCtx.ExecContext(ctx, query, args); err != nil {
			zipkin.TagError.Set(span, err.Error())
			return nil, err
		}

		return zResult{driver: res, ctx: ctx, options: c.options}, nil
	}

	return nil, driver.ErrSkip
}

func (c zConn) Query(query string, args []driver.Value) (rows driver.Rows, err error) {
	if queryer, ok := c.driver.(driver.Queryer); ok {
		return queryer.Query(query, args)
	}

	return nil, driver.ErrSkip
}

func (c zConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (rows driver.Rows, err error) {
	if queryerCtx, ok := c.driver.(driver.QueryerContext); ok {
		if zipkin.SpanFromContext(ctx) == nil {
			return queryerCtx.QueryContext(ctx, query, args)
		}

		span, _ := c.tracer.StartSpanFromContext(ctx, "sql/exec")
		defer span.Finish()

		if c.options.Query {
			span.Tag("sql.query", query)
		}

		setSpanDefaultTags(span, c.options.DefaultTags)

		if rows, err = queryerCtx.QueryContext(ctx, query, args); err != nil {
			zipkin.TagError.Set(span, err.Error())
			return nil, err
		}

		return zRows{driver: rows, ctx: ctx, options: c.options}, nil
	}

	return nil, driver.ErrSkip
}

func (c zConn) Prepare(query string) (stmt driver.Stmt, err error) {
	stmt, err = c.driver.Prepare(query)
	if err != nil {
		return nil, err
	}

	stmt = wrapStmt(stmt, query, c.tracer, c.options)
	return
}

func (c *zConn) Close() error {
	return c.driver.Close()
}

func (c *zConn) Begin() (driver.Tx, error) {
	return c.Begin()
}

func (c *zConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if prepCtx, ok := c.driver.(driver.ConnPrepareContext); ok {
		return prepCtx.PrepareContext(ctx, query)
	}

	return c.driver.Prepare(query)
}

func (c *zConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if zipkin.SpanFromContext(ctx) == nil {
		if connBeginTx, ok := c.driver.(driver.ConnBeginTx); ok {
			return connBeginTx.BeginTx(ctx, opts)
		}

		return c.driver.Begin()
	}

	span, _ := c.tracer.StartSpanFromContext(ctx, "sql/begin_transaction")
	defer span.Finish()

	setSpanDefaultTags(span, c.options.DefaultTags)

	if connBeginTx, ok := c.driver.(driver.ConnBeginTx); ok {
		tx, err := connBeginTx.BeginTx(ctx, opts)
		setSpanError(span, err)
		if err != nil {
			return nil, err
		}
		return zTx{driver: tx, ctx: ctx}, nil
	}

	tx, err := c.driver.Begin()
	setSpanError(span, err)
	if err != nil {
		return nil, err
	}

	return zTx{driver: tx, ctx: ctx, tracer: c.tracer}, nil
}

// zResult implements driver.Result
type zResult struct {
	driver  driver.Result
	ctx     context.Context
	tracer  *zipkin.Tracer
	options TraceOptions
}

func (r zResult) LastInsertId() (int64, error) {
	if !r.options.LastInsertID {
		return r.driver.LastInsertId()
	}

	span, _ := r.tracer.StartSpanFromContext(r.ctx, "sql/last_insert_id")
	defer span.Finish()

	setSpanDefaultTags(span, r.options.DefaultTags)

	id, err := r.driver.LastInsertId()
	setSpanError(span, err)

	return id, err
}

func (r zResult) RowsAffected() (cnt int64, err error) {
	zipkin.SpanFromContext(r.ctx)
	if r.options.RowsAffected && zipkin.SpanFromContext(r.ctx) != nil {
		span, _ := r.tracer.StartSpanFromContext(r.ctx, "sql/rows_affected")
		setSpanDefaultTags(span, r.options.DefaultTags)
		defer func() {
			span.Tag("sql.affected_rows", fmt.Sprintf("%d", cnt))
			setSpanError(span, err)
			span.Finish()
		}()
	}

	cnt, err = r.driver.RowsAffected()
	return
}

// zStmt implements driver.Stmt
type zStmt struct {
	driver  driver.Stmt
	query   string
	tracer  *zipkin.Tracer
	options TraceOptions
}

func (s zStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.driver.Exec(args)
}

func (s zStmt) Close() error {
	return s.driver.Close()
}

func (s zStmt) NumInput() int {
	return s.driver.NumInput()
}

func (s zStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.driver.Query(args)
}

func (s zStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (res driver.Result, err error) {
	if zipkin.SpanFromContext(ctx) == nil {
		return s.driver.(driver.StmtExecContext).ExecContext(ctx, args)
	}

	span, ctx := s.tracer.StartSpanFromContext(ctx, "sql/exec")
	defer func() {
		setSpanError(span, err)
		span.Finish()
	}()

	if s.options.Query {
		span.Tag("sql.query", s.query)
	}

	setSpanDefaultTags(span, s.options.DefaultTags)

	execContext := s.driver.(driver.StmtExecContext)
	res, err = execContext.ExecContext(ctx, args)
	if err != nil {
		return nil, err
	}
	res, err = zResult{driver: res, ctx: ctx, options: s.options}, nil
	return
}

func (s zStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (rows driver.Rows, err error) {
	if zipkin.SpanFromContext(ctx) == nil {
		return s.driver.(driver.StmtQueryContext).QueryContext(ctx, args)
	}

	span, ctx := s.tracer.StartSpanFromContext(ctx, "sql/query")
	defer func() {
		setSpanError(span, err)
		span.Finish()
	}()

	if s.options.Query {
		span.Tag("sql.query", s.query)
	}

	setSpanDefaultTags(span, s.options.DefaultTags)

	defer func() {
		setSpanError(span, err)
		span.Finish()
	}()

	// we already tested driver to implement StmtQueryContext
	queryContext := s.driver.(driver.StmtQueryContext)
	rows, err = queryContext.QueryContext(ctx, args)
	if err != nil {
		return nil, err
	}
	rows, err = zRows{driver: rows, ctx: ctx, options: s.options}, nil
	return
}

// zRows implements driver.Rows.
type zRows struct {
	driver  driver.Rows
	ctx     context.Context
	options TraceOptions
}

func (r zRows) Columns() []string {
	return r.driver.Columns()
}

func (r zRows) Close() error {
	return r.driver.Close()
}

func (r zRows) Next(dest []driver.Value) error {
	return r.driver.Next(dest)
}

// zTx implemens driver.Tx
type zTx struct {
	driver  driver.Tx
	ctx     context.Context
	tracer  *zipkin.Tracer
	options TraceOptions
}

func (t zTx) Commit() (err error) {
	span, _ := t.tracer.StartSpanFromContext(t.ctx, "sql/commit")
	defer func() {
		setSpanDefaultTags(span, t.options.DefaultTags)
		setSpanError(span, err)
		span.Finish()
	}()

	err = t.driver.Commit()
	return
}

func (t zTx) Rollback() (err error) {
	span, _ := t.tracer.StartSpanFromContext(t.ctx, "sql/rollback")
	defer func() {
		setSpanDefaultTags(span, t.options.DefaultTags)
		setSpanError(span, err)
		span.Finish()
	}()

	err = t.driver.Rollback()
	return
}

func setSpanError(span zipkin.Span, err error) {
	if err != nil {
		zipkin.TagError.Set(span, err.Error())
	}
}

func setSpanDefaultTags(span zipkin.Span, tags map[string]string) {
	for key, value := range tags {
		span.Tag(key, value)
	}
}
