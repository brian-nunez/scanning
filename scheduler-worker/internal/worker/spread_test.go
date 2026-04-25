package worker

import (
	"testing"
	"time"
)

func TestSpreadOffsetFromID(t *testing.T) {
	offset, err := SpreadOffsetFromID("01234567-89ab-cdef-0123-456789abcdef", 604800)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if offset < 0 || offset >= 604800 {
		t.Fatalf("offset out of range: %d", offset)
	}
}

func TestNextRunAtDaily(t *testing.T) {
	now := time.Date(2026, 4, 25, 14, 0, 0, 0, time.UTC)
	next, _, err := NextRunAtForFrequency("01234567-89ab-cdef-0123-456789abcdef", "daily", now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !next.After(now) {
		t.Fatalf("expected next run to be in the future, got %s", next)
	}

	if next.Hour() > 23 || next.Minute() > 59 || next.Second() > 59 {
		t.Fatalf("invalid daily timestamp: %s", next)
	}
}
