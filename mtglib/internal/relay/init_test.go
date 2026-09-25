package relay_test

type loggerMock struct{}

func (l loggerMock) Printf(format string, args ...any) {}

func (l loggerMock) WarningError(msg string, err error) {}
