package domain

import (
	"time"

	"github.com/google/uuid"
)

// GradedAttempt содержит только факты, необходимые для сводки практики.
// Поток должен идти по времени проверки по возрастанию: первая принятая версия
// определяет очки задачи навсегда.
type GradedAttempt struct {
	TaskID     uuid.UUID
	Difficulty string
	Verdict    string
	OccurredAt time.Time
}

type ProgressDay struct {
	Date     string `json:"date"`
	Attempts int    `json:"attempts"`
	Accepted int    `json:"accepted"`
}

type ProgressDifficulty struct {
	Difficulty string `json:"difficulty"`
	Solved     int    `json:"solved"`
}

type ProgressSummary struct {
	UserID           string               `json:"user_id"`
	Public           bool                 `json:"public"`
	Attempts         int                  `json:"attempts"`
	AcceptedAttempts int                  `json:"accepted_attempts"`
	SolvedTasks      int                  `json:"solved_tasks"`
	Score            int                  `json:"score"`
	AccuracyPercent  int                  `json:"accuracy_percent"`
	Daily            []ProgressDay        `json:"daily"`
	ByDifficulty     []ProgressDifficulty `json:"by_difficulty"`
}

type RatedUser struct {
	UserID      string `json:"user_id"`
	SolvedTasks int    `json:"solved_tasks"`
	Score       int    `json:"score"`
}

type ProgressAccumulator struct {
	summary      ProgressSummary
	seen         map[uuid.UUID]bool
	days         map[string]int
	acceptedDays map[string]int
	difficulty   map[string]int
	now          time.Time
}

func NewProgressAccumulator(userID uuid.UUID, public bool, now time.Time) *ProgressAccumulator {
	return &ProgressAccumulator{
		summary: ProgressSummary{UserID: userID.String(), Public: public},
		seen:    make(map[uuid.UUID]bool), days: make(map[string]int),
		acceptedDays: make(map[string]int), difficulty: make(map[string]int),
		now: now.UTC(),
	}
}

// Add учитывает только финальную пользовательскую проверку; ошибки инфраструктуры
// и отмена не ухудшают точность. Источник обязан дать попытки в порядке времени.
func (p *ProgressAccumulator) Add(attempt GradedAttempt) {
	if !gradedVerdict(attempt.Verdict) {
		return
	}
	p.summary.Attempts++
	day := attempt.OccurredAt.UTC().Format("2006-01-02")
	if withinProgressWindow(attempt.OccurredAt, p.now) {
		p.days[day]++
	}
	if attempt.Verdict != "accepted" {
		return
	}
	p.summary.AcceptedAttempts++
	if withinProgressWindow(attempt.OccurredAt, p.now) {
		p.acceptedDays[day]++
	}
	if p.seen[attempt.TaskID] {
		return
	}
	p.seen[attempt.TaskID] = true
	p.summary.SolvedTasks++
	p.summary.Score += RatingPoints(attempt.Difficulty)
	p.difficulty[attempt.Difficulty]++
}

func (p *ProgressAccumulator) Summary() ProgressSummary {
	result := p.summary
	if result.Attempts > 0 {
		result.AccuracyPercent = (result.AcceptedAttempts*100 + result.Attempts/2) / result.Attempts
	}
	today := time.Date(p.now.Year(), p.now.Month(), p.now.Day(), 0, 0, 0, 0, time.UTC)
	result.Daily = make([]ProgressDay, 0, 30)
	for i := 29; i >= 0; i-- {
		date := today.AddDate(0, 0, -i).Format("2006-01-02")
		result.Daily = append(result.Daily, ProgressDay{Date: date, Attempts: p.days[date], Accepted: p.acceptedDays[date]})
	}
	result.ByDifficulty = []ProgressDifficulty{
		{Difficulty: "easy", Solved: p.difficulty["easy"]},
		{Difficulty: "medium", Solved: p.difficulty["medium"]},
		{Difficulty: "hard", Solved: p.difficulty["hard"]},
	}
	return result
}

func RatingPoints(difficulty string) int {
	switch difficulty {
	case "easy":
		return 10
	case "medium":
		return 20
	case "hard":
		return 30
	default:
		return 0
	}
}

func gradedVerdict(verdict string) bool {
	switch verdict {
	case "accepted", "wrong_answer", "compilation_error", "runtime_error", "time_limit_exceeded", "memory_limit_exceeded", "output_limit_exceeded":
		return true
	default:
		return false
	}
}

func withinProgressWindow(at, now time.Time) bool {
	date := at.UTC()
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -29)
	return !date.Before(start) && !date.After(now)
}
