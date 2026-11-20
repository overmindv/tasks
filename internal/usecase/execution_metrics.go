package usecase

import (
	"time"

	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/observability"
)

func (s *CodeSubmissionService) WithExecutionMetrics(metrics *observability.Execution) *CodeSubmissionService {
	s.metrics = metrics
	return s
}

// executionTransition is observed only after the submission transaction commits.
type executionTransition struct {
	attempt   observability.AttemptKind
	mode      domain.RunMode
	verdict   domain.ExecutionVerdict
	createdAt time.Time
}

func finalTransition(s domain.CodeSubmission, verdict domain.ExecutionVerdict) executionTransition {
	return executionTransition{mode: s.RunMode, verdict: verdict, createdAt: s.CreatedAt}
}

func (t executionTransition) observe(metrics *observability.Execution) {
	if metrics == nil {
		return
	}
	if t.attempt != 0 {
		metrics.Attempt(t.attempt)
	}
	if t.verdict != "" {
		metrics.Final(t.mode, t.verdict, time.Since(t.createdAt))
	}
}
