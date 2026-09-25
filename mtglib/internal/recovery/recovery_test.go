package recovery_test

import (
	"strings"
	"testing"

	"github.com/9seconds/mtg/v2/mtglib/internal/recovery"
)

type recordingLogger struct {
	messages []string
	errors   []string
}

func (l *recordingLogger) WarningError(msg string, err error) {
	l.messages = append(l.messages, msg)
	l.errors = append(l.errors, err.Error())
}

func guarded(logger recovery.Logger, fn func(), cleanups ...func()) {
	defer recovery.Guard(logger, "guarded", cleanups...)

	fn()
}

func TestGuardDoesNothingWithoutAPanic(t *testing.T) {
	logger := &recordingLogger{}
	cleaned := false

	guarded(logger, func() {}, func() { cleaned = true })

	if len(logger.messages) != 0 || cleaned {
		t.Fatalf("a call that did not panic was logged %q or cleaned up %v", logger.messages, cleaned)
	}
}

func TestGuardLogsThePanicWithItsStackAndRunsCleanups(t *testing.T) {
	logger := &recordingLogger{}
	cleaned := 0

	guarded(logger, func() { panic("boom") }, func() { cleaned++ }, func() { cleaned++ })

	if len(logger.messages) != 1 || logger.messages[0] != "recovered from a panic in guarded" {
		t.Fatalf("unexpected messages %q", logger.messages)
	}

	if !strings.HasPrefix(logger.errors[0], "boom\ngoroutine ") || !strings.Contains(logger.errors[0], "recovery_test.TestGuardLogsThePanicWithItsStackAndRunsCleanups") {
		t.Fatalf("the logged error does not carry the panic value and the panicking stack: %q", logger.errors[0])
	}

	if cleaned != 2 {
		t.Fatalf("ran %d of 2 cleanups", cleaned)
	}
}

func TestGuardSurvivesACleanupThatPanicsAndRunsTheRest(t *testing.T) {
	logger := &recordingLogger{}
	cleaned := false

	guarded(logger, func() { panic("first") }, func() { panic("second") }, func() { cleaned = true })

	if len(logger.messages) != 2 || logger.messages[1] != "recovered from a panic in guarded cleanup" {
		t.Fatalf("unexpected messages %q", logger.messages)
	}

	if !strings.HasPrefix(logger.errors[1], "second\n") {
		t.Fatalf("the cleanup panic was not logged: %q", logger.errors[1])
	}

	if !cleaned {
		t.Fatalf("a panicking cleanup stopped the remaining cleanups")
	}
}
