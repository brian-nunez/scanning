package worker

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	daySeconds  int64 = 24 * 60 * 60
	weekSeconds int64 = 7 * daySeconds
)

func SpreadOffsetFromID(id string, periodSeconds int64) (int64, error) {
	clean := strings.ReplaceAll(id, "-", "")

	if len(clean) < 16 {
		return 0, fmt.Errorf("id too short")
	}

	n, err := strconv.ParseUint(clean[:16], 16, 64)
	if err != nil {
		return 0, err
	}

	return int64(n % uint64(periodSeconds)), nil
}

func NextRunAtForFrequency(assetID, frequency string, now time.Time) (time.Time, int64, error) {
	n := now.UTC()

	switch strings.ToLower(frequency) {
	case "daily":
		start := time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, time.UTC).Add(24 * time.Hour)
		offset, err := SpreadOffsetFromID(assetID, daySeconds)
		if err != nil {
			return time.Time{}, 0, err
		}

		return start.Add(time.Duration(offset) * time.Second), offset, nil
	case "weekly":
		start := startOfWeek(n).AddDate(0, 0, 7)
		offset, err := SpreadOffsetFromID(assetID, weekSeconds)
		if err != nil {
			return time.Time{}, 0, err
		}

		return start.Add(time.Duration(offset) * time.Second), offset, nil
	case "monthly":
		start := time.Date(n.Year(), n.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0)
		days := daysInMonth(start.Year(), start.Month())

		dayOffset, err := SpreadOffsetFromID(assetID, int64(days))
		if err != nil {
			return time.Time{}, 0, err
		}

		secondOffset, err := SpreadOffsetFromID(assetID, daySeconds)
		if err != nil {
			return time.Time{}, 0, err
		}

		offset := dayOffset*daySeconds + secondOffset
		return start.AddDate(0, 0, int(dayOffset)).Add(time.Duration(secondOffset) * time.Second), offset, nil
	case "manual":
		// Manual jobs are primarily API-triggered; keep next_run_at in the near future.
		return n.Add(24 * time.Hour), 0, nil
	default:
		return time.Time{}, 0, fmt.Errorf("unsupported frequency: %s", frequency)
	}
}

func startOfWeek(t time.Time) time.Time {
	utc := t.UTC()
	midnight := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)

	weekday := int(midnight.Weekday())
	if weekday == 0 {
		weekday = 7
	}

	return midnight.AddDate(0, 0, -(weekday - 1))
}

func daysInMonth(year int, month time.Month) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}
