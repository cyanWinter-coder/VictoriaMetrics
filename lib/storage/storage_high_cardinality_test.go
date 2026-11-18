package storage

import (
	"fmt"
	"testing"
	"time"
)

// testGenerateHighCardinalityMetricRows generates groupsCount metric groups
// with seriesPerGroup unique series in every group. The series inside a group
// differ only by the user_id label value, simulating a high-cardinality label.
func testGenerateHighCardinalityMetricRows(groupsCount, seriesPerGroup int, tr TimeRange) []MetricRow {
	var mrs []MetricRow
	var mn MetricName
	for g := range groupsCount {
		mn.MetricGroup = fmt.Appendf(nil, "metric_%d", g)
		mn.Tags = []Tag{
			{[]byte("job"), []byte("webservice")},
			{[]byte("instance"), []byte("1.2.3.4")},
			{[]byte("group_id"), fmt.Appendf(nil, "%d", g)},
			{},
		}
		for i := range seriesPerGroup {
			mn.Tags[3] = Tag{[]byte("user_id"), fmt.Appendf(nil, "%d", i)}
			metricNameRaw := mn.marshalRaw(nil)
			mrs = append(mrs, MetricRow{
				MetricNameRaw: metricNameRaw,
				Timestamp:     tr.MinTimestamp + int64(i%1000),
				Value:         float64(i),
			})
		}
	}
	return mrs
}

// testCountMetricNamesForGroup is a test helper that counts time series
// for the given metric group within the given time range.
func testCountMetricNamesForGroup(s *Storage, groupID int, tr TimeRange) int {
	tfs := NewTagFilters()
	groupIDStr := fmt.Appendf(nil, "%d", groupID)
	if err := tfs.Add([]byte("group_id"), groupIDStr, false, false); err != nil {
		panic(fmt.Sprintf("unexpected error in TagFilters.Add: %v", err))
	}
	names, err := s.SearchMetricNames(noDeadlineContext, nil, []*TagFilters{tfs}, tr, 1e9)
	if err != nil {
		panic(fmt.Sprintf("SearchMetricNames() failed unexpectedly: %v", err))
	}
	return len(names)
}

// TestStorageHighCardinalityPerGroupSeriesLimit verifies that the per-metric-group
// series limiter protects the inverted index from unbounded growth when a metric
// contains a high-cardinality label, while already registered series continue
// accepting rows.
func TestStorageHighCardinalityPerGroupSeriesLimit(t *testing.T) {
	defer testRemoveAll(t)

	const (
		groupsCount    = 4
		seriesPerGroup = 1000
		limit          = 100
	)
	minTimestamp := time.Now().UnixMilli()
	tr := TimeRange{minTimestamp, minTimestamp + 1000}
	mrs := testGenerateHighCardinalityMetricRows(groupsCount, seriesPerGroup, tr)
	numRows := uint64(len(mrs))

	opts := OpenOptions{
		MaxSeriesPerMetricGroup: limit,
	}
	s := MustOpenStorage(t.Name(), opts)
	s.AddRows(mrs, defaultPrecisionBits)
	s.DebugFlush()

	var m Metrics
	s.UpdateMetrics(&m)
	if got := m.RowsReceivedTotal; got != numRows {
		t.Fatalf("unexpected Metrics.RowsReceivedTotal: got %d, want %d", got, numRows)
	}
	if got := m.PerGroupSeriesLimitRowsDropped; got == 0 {
		t.Fatalf("unexpected Metrics.PerGroupSeriesLimitRowsDropped: got %d, want > 0", got)
	}
	if got, want := m.RowsAddedTotal, numRows-m.PerGroupSeriesLimitRowsDropped; got != want {
		t.Fatalf("unexpected Metrics.RowsAddedTotal: got %d, want %d", got, want)
	}

	// The number of registered series per group must be limited.
	// The upper bound is 2*limit in order to avoid flakiness when the test
	// runs across an hour boundary, since the limiter counters are reset
	// every hour.
	registeredPerGroup := make([]int, groupsCount)
	for g := range groupsCount {
		group := g
		n := testCountMetricNamesForGroup(s, group, tr)
		if n < limit || n > 2*limit {
			t.Fatalf("unexpected number of registered series for group %d: got %d, want in [%d, %d]", group, n, limit, 2*limit)
		}
		registeredPerGroup[g] = n
	}

	// The query results must be complete for the registered series.
	if got, want := testCountAllMetricNames(s, tr), int(m.RowsAddedTotal); got != want {
		t.Fatalf("unexpected total number of registered series: got %d, want %d", got, want)
	}

	// Re-insert the same rows. The already registered series must continue
	// accepting rows via the fast path, which isn't subject to the limiter.
	s.AddRows(mrs, defaultPrecisionBits)
	s.DebugFlush()

	var m2 Metrics
	s.UpdateMetrics(&m2)
	addedTotal := uint64(0)
	for _, n := range registeredPerGroup {
		addedTotal += uint64(n)
	}
	if got := m2.RowsAddedTotal - m.RowsAddedTotal; got < addedTotal {
		t.Fatalf("previously registered series must continue accepting rows; "+
			"rows added during re-insertion: got %d, want at least %d", got, addedTotal)
	}
	for g := range groupsCount {
		group := g
		if n := testCountMetricNamesForGroup(s, group, tr); n < registeredPerGroup[g] {
			t.Fatalf("previously registered series for group %d disappeared: got %d, want at least %d", group, n, registeredPerGroup[g])
		}
	}
	s.MustClose()

	// Open the storage again and verify that the registered series survive
	// the restart and continue accepting rows.
	s = MustOpenStorage(t.Name(), opts)
	s.AddRows(mrs, defaultPrecisionBits)
	s.DebugFlush()
	for g := range groupsCount {
		group := g
		if n := testCountMetricNamesForGroup(s, group, tr); n < registeredPerGroup[g] {
			t.Fatalf("previously registered series for group %d disappeared after restart: got %d, want at least %d", group, n, registeredPerGroup[g])
		}
	}
	s.MustClose()
}

// TestStorageHighCardinalityPerGroupSeriesLimitDisabled verifies that all
// the high-cardinality series are registered when the limit isn't set.
func TestStorageHighCardinalityPerGroupSeriesLimitDisabled(t *testing.T) {
	defer testRemoveAll(t)

	const (
		groupsCount    = 2
		seriesPerGroup = 500
	)
	minTimestamp := time.Now().UnixMilli()
	tr := TimeRange{minTimestamp, minTimestamp + 1000}
	mrs := testGenerateHighCardinalityMetricRows(groupsCount, seriesPerGroup, tr)

	s := MustOpenStorage(t.Name(), OpenOptions{})
	s.AddRows(mrs, defaultPrecisionBits)
	s.DebugFlush()

	var m Metrics
	s.UpdateMetrics(&m)
	if got, want := m.RowsAddedTotal, uint64(len(mrs)); got != want {
		t.Fatalf("unexpected Metrics.RowsAddedTotal: got %d, want %d", got, want)
	}
	if got := m.PerGroupSeriesLimitRowsDropped; got != 0 {
		t.Fatalf("unexpected Metrics.PerGroupSeriesLimitRowsDropped: got %d, want 0", got)
	}
	if got, want := testCountAllMetricNames(s, tr), len(mrs); got != want {
		t.Fatalf("unexpected total number of registered series: got %d, want %d", got, want)
	}
	s.MustClose()
}
