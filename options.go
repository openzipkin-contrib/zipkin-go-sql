package zipkinsql

// TraceOption allows for managing zipkinsql configuration using functional options.
type TraceOption func(o *TraceOptions)

// TraceOptions holds configuration of our zipkinsql tracing middleware.
// By default all options are set to false intentionally when creating a wrapped
// driver and provide the most sensible default with both performance and
// security in mind.
type TraceOptions struct {
	// RowsAffected, if set to true, will enable the creation of spans on
	// RowsAffected calls.
	RowsAffected bool

	// Query, if set to true, will enable recording of sql queries in spans.
	// Only allow this if it is safe to have queries recorded with respect to
	// security.
	Query bool

	// LastInsertID, if set to true, will enable the creation of spans on
	// LastInsertId calls.
	LastInsertID bool

	// DefaultTags will be set to each span as default.
	DefaultTags map[string]string
}

// WithAllTraceOptions enables all available trace options.
func WithAllTraceOptions() TraceOption {
	return func(o *TraceOptions) {
		*o = AllTraceOptions
	}
}

// AllTraceOptions has all tracing options enabled.
var AllTraceOptions = TraceOptions{
	RowsAffected: true,
	Query:        true,
	LastInsertID: true,
}

// WithOptions sets our ocsql tracing middleware options through a single
// TraceOptions object.
func WithOptions(options TraceOptions) TraceOption {
	return func(o *TraceOptions) {
		*o = options
	}
}

// WithRowsAffected if set to true, will enable the creation of spans on
// RowsAffected calls.
func WithRowsAffected(b bool) TraceOption {
	return func(o *TraceOptions) {
		o.RowsAffected = b
	}
}

// WithLastInsertID if set to true, will enable the creation of spans on
// LastInsertId calls.
func WithLastInsertID(b bool) TraceOption {
	return func(o *TraceOptions) {
		o.LastInsertID = b
	}
}

// WithQuery if set to true, will enable recording of sql queries in spans.
// Only allow this if it is safe to have queries recorded with respect to
// security.
func WithQuery(b bool) TraceOption {
	return func(o *TraceOptions) {
		o.Query = b
	}
}

// WithDefaultTags will be set to each span as default.
func WithDefaultTags(tags map[string]string) TraceOption {
	return func(o *TraceOptions) {
		o.DefaultTags = tags
	}
}
