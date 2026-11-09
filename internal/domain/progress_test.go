package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestProgressAccumulatorCountsFirstAcceptedTaskOnce(t *testing.T) {
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	firstTask, secondTask := uuid.New(), uuid.New()
	progress := NewProgressAccumulator(uuid.New(), true, now)
	progress.Add(GradedAttempt{TaskID: firstTask, Difficulty: "easy", Verdict: "wrong_answer", OccurredAt: now.Add(-48 * time.Hour)})
	progress.Add(GradedAttempt{TaskID: firstTask, Difficulty: "easy", Verdict: "accepted", OccurredAt: now.Add(-24 * time.Hour)})
	progress.Add(GradedAttempt{TaskID: firstTask, Difficulty: "hard", Verdict: "accepted", OccurredAt: now})
	progress.Add(GradedAttempt{TaskID: secondTask, Difficulty: "medium", Verdict: "accepted", OccurredAt: now})
	progress.Add(GradedAttempt{TaskID: secondTask, Difficulty: "medium", Verdict: "infrastructure_error", OccurredAt: now})
	progress.Add(GradedAttempt{TaskID: secondTask, Difficulty: "medium", Verdict: "checker_error", OccurredAt: now})
	progress.Add(GradedAttempt{TaskID: secondTask, Difficulty: "medium", Verdict: "cancelled", OccurredAt: now})
	got := progress.Summary()
	if got.Attempts != 4 || got.AcceptedAttempts != 3 || got.SolvedTasks != 2 || got.Score != 30 || got.AccuracyPercent != 75 {
		t.Fatalf("unexpected summary: %+v", got)
	}
	if len(got.Daily) != 30 || got.Daily[29].Date != "2026-11-09" || got.Daily[29].Attempts != 2 || got.Daily[29].Accepted != 2 {
		t.Fatalf("unexpected 30-day chart: %+v", got.Daily)
	}
	if got.ByDifficulty[0].Solved != 1 || got.ByDifficulty[1].Solved != 1 || got.ByDifficulty[2].Solved != 0 {
		t.Fatalf("first accepted version did not set difficulty: %+v", got.ByDifficulty)
	}
}

func TestProgressAccumulatorEmptyAndOldActivity(t *testing.T) {
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	progress := NewProgressAccumulator(uuid.New(), false, now)
	progress.Add(GradedAttempt{TaskID: uuid.New(), Difficulty: "hard", Verdict: "accepted", OccurredAt: now.AddDate(0, 0, -35)})
	got := progress.Summary()
	if got.Score != 30 || got.SolvedTasks != 1 || got.Daily[0].Attempts != 0 || got.Daily[29].Attempts != 0 || got.Public {
		t.Fatalf("lifetime totals or empty recent chart incorrect: %+v", got)
	}
	zero := NewProgressAccumulator(uuid.New(), false, now).Summary()
	if zero.AccuracyPercent != 0 || zero.Attempts != 0 || zero.Score != 0 || len(zero.Daily) != 30 {
		t.Fatalf("empty progress incorrect: %+v", zero)
	}
}
