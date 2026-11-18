package storage

import (
	"sync"
	"sync/atomic"
)

// maxTrackedMetricGroups is the maximum number of metric groups tracked
// by metricGroupSeriesLimiter at the same time.
//
// The limit puts a hard bound on the memory occupied by the limiter.
// When the number of tracked groups exceeds this limit, all the counters
// are reset. This may temporarily allow more new series than configured,
// but keeps the memory usage bounded under high-cardinality workloads
// with too many distinct metric groups.
//
// It is a var instead of a const in order to allow overriding it in tests.
var maxTrackedMetricGroups = 100_000

// metricGroupSeriesLimiter limits the number of new series, which can be
// registered per metric group (metric name) per hour.
//
// This protects the inverted index from unbounded growth when a single
// metric contains a label with unbounded set of values (aka high-cardinality
// label, e.g. user_id or request_id).
//
// The limiter tracks only genuinely new series - it must be consulted
// only when the series isn't found in the tsid cache and in the indexdb.
// This guarantees that already registered series continue accepting rows
// after the limit is reached.
//
// The memory usage of the limiter is bounded by maxTrackedMetricGroups
// map entries, e.g. a few megabytes in the worst case.
type metricGroupSeriesLimiter struct {
	// maxSeriesPerGroup is the maximum number of new series, which can be
	// registered per metric group during the current hour.
	maxSeriesPerGroup uint64

	// resetsTotal is the number of times the internal counters were reset
	// because of hour change or because maxTrackedMetricGroups was exceeded.
	resetsTotal atomic.Uint64

	mu sync.Mutex

	// hour is the hour for which the counters below are collected.
	hour uint64

	// groups contains the number of new series registered during the current
	// hour keyed by metric group hash.
	groups map[uint64]uint64
}

func newMetricGroupSeriesLimiter(maxSeriesPerGroup int) *metricGroupSeriesLimiter {
	return &metricGroupSeriesLimiter{
		maxSeriesPerGroup: uint64(maxSeriesPerGroup),
		groups:            make(map[uint64]uint64),
	}
}

// Register registers a new series for the given metric group hash at the given hour.
//
// It returns false if the number of new series registered for the given group
// during the given hour exceeds the configured limit.
func (l *metricGroupSeriesLimiter) Register(groupHash, hour uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.hour != hour {
		// New hour has started - reset all the counters.
		l.hour = hour
		if len(l.groups) > 0 {
			clear(l.groups)
			l.resetsTotal.Add(1)
		}
	}
	n, ok := l.groups[groupHash]
	if !ok && len(l.groups) >= maxTrackedMetricGroups {
		// Too many distinct metric groups - reset the counters in order to
		// keep the memory usage bounded.
		clear(l.groups)
		l.resetsTotal.Add(1)
	}
	if n >= l.maxSeriesPerGroup {
		return false
	}
	l.groups[groupHash] = n + 1
	return true
}

// MaxItems returns the maximum number of new series allowed per metric group per hour.
func (l *metricGroupSeriesLimiter) MaxItems() uint64 {
	return l.maxSeriesPerGroup
}

// ResetsTotal returns the number of times the internal counters were reset.
func (l *metricGroupSeriesLimiter) ResetsTotal() uint64 {
	return l.resetsTotal.Load()
}
