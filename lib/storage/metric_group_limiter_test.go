package storage

import (
	"testing"
)

func TestMetricGroupSeriesLimiterLimit(t *testing.T) {
	l := newMetricGroupSeriesLimiter(3)

	// The first 3 series for the group must be accepted.
	for i := range 3 {
		if !l.Register(42, 100) {
			t.Fatalf("series %d for the group must be accepted", i)
		}
	}
	// The subsequent series for the same group must be rejected during the same hour.
	for i := range 5 {
		if l.Register(42, 100) {
			t.Fatalf("series %d beyond the limit must be rejected", i)
		}
	}
	// Series for another group must not be affected.
	for i := range 3 {
		if !l.Register(43, 100) {
			t.Fatalf("series %d for another group must be accepted", i)
		}
	}
	if n := l.MaxItems(); n != 3 {
		t.Fatalf("unexpected MaxItems(); got %d; want %d", n, 3)
	}
}

func TestMetricGroupSeriesLimiterHourRotation(t *testing.T) {
	l := newMetricGroupSeriesLimiter(2)

	if !l.Register(42, 100) || !l.Register(42, 100) {
		t.Fatalf("the first 2 series must be accepted")
	}
	if l.Register(42, 100) {
		t.Fatalf("the series beyond the limit must be rejected")
	}
	// The counters must be reset when the hour changes.
	if !l.Register(42, 101) {
		t.Fatalf("the series must be accepted after the hour change")
	}
	if n := l.ResetsTotal(); n != 1 {
		t.Fatalf("unexpected ResetsTotal(); got %d; want %d", n, 1)
	}
}

func TestMetricGroupSeriesLimiterTooManyGroups(t *testing.T) {
	// Shrink the limit on the number of tracked groups in order to avoid
	// generating too many groups in the test.
	origMaxTracked := maxTrackedMetricGroups
	maxTrackedMetricGroups = 16
	defer func() { maxTrackedMetricGroups = origMaxTracked }()

	l := newMetricGroupSeriesLimiter(10)

	// Register more groups than the limit allows. The counters must be reset
	// at least once in order to keep the memory usage bounded.
	for i := range 100 {
		if !l.Register(uint64(i), 100) {
			t.Fatalf("the first series for group %d must be accepted", i)
		}
	}
	if n := l.ResetsTotal(); n == 0 {
		t.Fatalf("expecting at least one reset because of too many tracked groups")
	}
	if n := len(l.groups); n > 16 {
		t.Fatalf("the number of tracked groups exceeds the limit; got %d", n)
	}
}

func TestMetricGroupSeriesLimiterConcurrent(_ *testing.T) {
	l := newMetricGroupSeriesLimiter(1000)
	concurrency := 8
	ch := make(chan struct{}, concurrency)
	for i := range concurrency {
		go func(n int) {
			for j := range 2000 {
				l.Register(uint64(n*2000+j), 100)
			}
			ch <- struct{}{}
		}(i)
	}
	for range concurrency {
		<-ch
	}
}

func TestMetricGroupSeriesLimiterGroupIndependence(t *testing.T) {
	l := newMetricGroupSeriesLimiter(1)
	for i := range 100 {
		groupHash := uint64(i)
		if !l.Register(groupHash, 100) {
			t.Fatalf("the first series for group %d must be accepted", i)
		}
		if l.Register(groupHash, 100) {
			t.Fatalf("the second series for group %d must be rejected", i)
		}
	}
	if got, want := len(l.groups), 100; got != want {
		t.Fatalf("unexpected number of tracked groups; got %d; want %d", got, want)
	}
	for i := range 100 {
		if got := l.groups[uint64(i)]; got != 1 {
			t.Fatalf("unexpected counter for group %d; got %d; want 1", i, got)
		}
	}
}
