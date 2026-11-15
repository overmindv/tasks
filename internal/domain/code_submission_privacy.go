package domain

// PublicCodeSubmission строит независимую безопасную копию, включая legacy записи.
// Admin не является отдельным диагностическим каналом публичного API.
func PublicCodeSubmission(submission CodeSubmission) CodeSubmission {
	submission.SourceCode = ""
	submission.Compilation = publicExecutionPhase(submission.Compilation)
	submission.Execution = publicExecutionPhase(submission.Execution)
	tests := make([]ExecutionTestResult, 0, len(submission.Tests))
	for _, test := range submission.Tests {
		if test.SampleVisible {
			tests = append(tests, test)
		}
	}
	submission.Tests = tests
	if submission.Failure != nil {
		verdict := ExecutionVerdictInfrastructureError
		if submission.Verdict != nil {
			verdict = *submission.Verdict
		}
		submission.Failure = SafeExecutionFailure(verdict)
	}
	return submission
}

func publicExecutionPhase(phase *ExecutionPhaseResult) *ExecutionPhaseResult {
	if phase == nil {
		return nil
	}
	return &ExecutionPhaseResult{ExitCode: phase.ExitCode, DurationMS: phase.DurationMS, MemoryBytes: phase.MemoryBytes}
}

// SafeExecutionFailure не переносит произвольные error.code/message sandbox в БД/API.
func SafeExecutionFailure(verdict ExecutionVerdict) *ExecutionFailure {
	code := "INFRASTRUCTURE_ERROR"
	switch verdict {
	case ExecutionVerdictCheckerError:
		code = "CHECKER_ERROR"
	case ExecutionVerdictCompilationError:
		code = "COMPILATION_ERROR"
	case ExecutionVerdictRuntimeError:
		code = "RUNTIME_ERROR"
	case ExecutionVerdictTimeLimitExceeded:
		code = "TIME_LIMIT_EXCEEDED"
	case ExecutionVerdictMemoryLimitExceeded:
		code = "MEMORY_LIMIT_EXCEEDED"
	case ExecutionVerdictOutputLimitExceeded:
		code = "OUTPUT_LIMIT_EXCEEDED"
	case ExecutionVerdictWrongAnswer:
		code = "WRONG_ANSWER"
	case ExecutionVerdictCancelled:
		code = "CANCELLED"
	}
	return &ExecutionFailure{Code: code, Message: "execution could not be completed successfully"}
}
