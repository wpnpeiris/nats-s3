package logging

import (
	"fmt"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"os"
)

// Supported logger formats and levels.
const (
	// FormatJSON encodes logs as JSON to stdout.
	FormatJSON = "json"

	// LevelDebug enables debug and above.
	LevelDebug = "debug"
	// LevelInfo enables info and above.
	LevelInfo = "info"
	// LevelWarning enables warning and error only.
	LevelWarning = "warning"
	// LevelError enables error only.
	LevelError = "error"
)

// Config declares runtime logging preferences.
type Config struct {
	// Format selects encoder. When set to "json" logs are emitted as JSON to
	// stdout; any other value uses logfmt to stderr.
	Format string
	// Level selects the minimum level to log. Valid values are
	// "debug", "info", "warning", or "error".
	Level string
}

// NewLogger builds a go-kit/log Logger using the provided configuration.
func NewLogger(cfg Config) log.Logger {
	var lvl level.Option
	switch cfg.Level {
	case LevelError:
		lvl = level.AllowError()
	case LevelWarning:
		lvl = level.AllowWarn()
	case LevelInfo:
		lvl = level.AllowInfo()
	case LevelDebug:
		lvl = level.AllowDebug()
	default:
		// Default to info level for unknown/invalid configurations
		fmt.Fprintf(os.Stderr, "WARNING: Unknown log level '%s', defaulting to 'info'\n", cfg.Level)
		lvl = level.AllowInfo()
	}

	var logger log.Logger
	switch cfg.Format {
	case FormatJSON:
		logger = log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
	default:
		logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	}

	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	logger = level.NewFilter(logger, lvl)

	return logger
}

// Info logs at info level.
func Info(l log.Logger, kv ...any) {
	if err := level.Info(l).Log(kv...); err != nil {
		fmt.Fprintf(os.Stderr, "log info write failed: %v\n", err)
	}
}

// Warn logs at warning level.
func Warn(l log.Logger, kv ...any) {
	if err := level.Warn(l).Log(kv...); err != nil {
		fmt.Fprintf(os.Stderr, "log warn write failed: %v\n", err)
	}
}

// Error logs at error level.
func Error(l log.Logger, kv ...any) {
	if err := level.Error(l).Log(kv...); err != nil {
		fmt.Fprintf(os.Stderr, "log error write failed: %v\n", err)
	}
}

// Debug logs at debug level.
func Debug(l log.Logger, kv ...any) {
	if err := level.Debug(l).Log(kv...); err != nil {
		fmt.Fprintf(os.Stderr, "log debug write failed: %v\n", err)
	}
}
