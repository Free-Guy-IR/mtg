package relay

type Logger interface {
	Printf(msg string, args ...any)
	WarningError(msg string, err error)
}
