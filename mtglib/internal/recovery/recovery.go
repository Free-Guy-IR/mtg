package recovery

import (
	"fmt"
	"runtime/debug"
)

type Logger interface {
	WarningError(msg string, err error)
}

func Guard(logger Logger, where string, cleanups ...func()) {
	value := recover()
	if value == nil {
		return
	}

	logger.WarningError(
		"recovered from a panic in "+where,
		fmt.Errorf("%v\n%s", value, debug.Stack()),
	)

	for _, cleanup := range cleanups {
		runCleanup(logger, where, cleanup)
	}
}

func runCleanup(logger Logger, where string, cleanup func()) {
	defer Guard(logger, where+" cleanup")

	cleanup()
}
