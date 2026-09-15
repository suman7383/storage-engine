package storageengine

import (
	"io"
	"log/slog"
	"os"
)

// LogLevel represents the severity of a log message.
type LogLevel = slog.Level

const (
	LevelDebug = slog.LevelDebug
	LevelInfo  = slog.LevelInfo
	LevelWarn  = slog.LevelWarn
	LevelError = slog.LevelError
)

// LoggerOptions configures the logger behaviour.
type LoggerOptions struct {
	// Level is the minimum log level that will be emitted. Defaults to LevelInfo.
	Level LogLevel

	// Output is the writer to write log lines to. Defaults to os.Stderr.
	Output io.Writer

	// AddSource includes the source file and line number in every log record.
	AddSource bool
}

// logger is the package-level structured logger. It is initialised with
// sensible defaults and can be replaced via SetLogger or SetLoggerFromOptions.
var logger *slog.Logger

func init() {
	logger = newLogger(LoggerOptions{})
}

// newLogger creates a new *slog.Logger from the given options, applying
// defaults for any zero-value fields.
func newLogger(opts LoggerOptions) *slog.Logger {
	if opts.Output == nil {
		opts.Output = os.Stderr
	}

	handlerOpts := &slog.HandlerOptions{
		Level:     opts.Level,
		AddSource: opts.AddSource,
	}

	handler := slog.NewJSONHandler(opts.Output, handlerOpts)
	return slog.New(handler)
}

// SetLogger replaces the package-level logger with the provided *slog.Logger.
// This allows callers to bring their own fully-configured logger (e.g. one
// shared across the whole application).
func SetLogger(l *slog.Logger) {
	if l == nil {
		return
	}
	logger = l
}

// SetLoggerFromOptions builds and installs a new logger from LoggerOptions.
func SetLoggerFromOptions(opts LoggerOptions) {
	logger = newLogger(opts)
}
