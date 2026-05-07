package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/ravinggo/zerolog"
	"github.com/ravinggo/zerolog/log"
	"github.com/ravinggo/zerolog/pkgerrors"
	"gopkg.in/natefinch/lumberjack.v2"

	baseenv "github.com/ravinggo/game/common/base-env"
)

// ILogger defines the subset of zerolog.Logger methods exposed for dependency
// injection and testing. Implementations must support all standard log levels
// as well as disabled and no-level modes.
// Written by Claude Code claude-opus-4-6.
type ILogger interface {
	Trace() *Event
	Debug() *Event
	Info() *Event
	Warn() *Event
	Error() *Event
	Fatal() *Event
	Panic() *Event
	NoLevel() *Event
	Disabled() *Event
	WithLevel(Level) *Event
}

type (
	Logger  = zerolog.Logger
	Context = zerolog.Context
	Event   = zerolog.Event
	Level   = zerolog.Level
)

var (
	Log      *Logger
	LogSkip3 Logger
)

// InitDefaultLogger reads the base-env configuration and constructs the global
// Log and LogSkip3 loggers. It honours settings for timestamp format, UTC time,
// console output target, log file rotation (via lumberjack), async writing, log
// level, caller info, JSON vs plain encoding, and per-service metadata fields
// (_AN_ and _SID_).
// Written by Claude Code claude-opus-4-6.
func InitDefaultLogger() {
	envCnf := baseenv.GetConfig()
	if !envCnf.LogTimestamp {
		zerolog.TimeFieldFormat = "2006-01-02T15:04:05.000Z07:00"
	} else {
		zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	}
	if envCnf.LogUtcTime {
		zerolog.TimestampFunc = func() time.Time {
			return time.Now().UTC()
		}
	}
	zerolog.MessageFieldName = "msg"
	zerolog.ErrorFieldName = "err"

	var writers []io.Writer
	if envCnf.LogConsole != "" {
		if envCnf.LogEncodingMode == "json" {
			if envCnf.LogConsole == "stdout" {
				writers = append(writers, os.Stdout)
			} else if envCnf.LogConsole == "stderr" {
				writers = append(writers, os.Stderr)
			} else if envCnf.LogConsole == "discard" {
				writers = append(writers, io.Discard)
			} else {
				panic("Console must be stdout or stderr or discard")
			}
		} else {
			if envCnf.LogConsole == "stdout" {
				writers = append(
					writers, os.Stdout,
				)
			} else if envCnf.LogConsole == "stderr" {
				writers = append(
					writers, os.Stderr,
				)
			} else if envCnf.LogConsole == "discard" {
				writers = append(writers, io.Discard)
			} else {
				panic("Console must be stdout or stderr or discard")
			}
		}
	}

	serverId := envCnf.ServerId
	appName := envCnf.ServerType
	logDir := envCnf.LogDir
	if logDir != "" {
		fp := filepath.Join(logDir, appName)
		if serverId != 0 {
			filepath.Join(fp, strconv.Itoa(int(serverId)))
		}
		fp += ".log"
		l := &lumberjack.Logger{
			Filename:   fp,
			MaxSize:    envCnf.LogMaxSize,   // megabytes
			MaxBackups: envCnf.LogMaxBackup, // 最多保留多少文件
			MaxAge:     3650,                // days
		}
		writers = append(writers, l)
	}

	var writer io.Writer
	if len(writers) > 1 {
		writer = zerolog.MultiLevelWriter(writers...)
	} else if len(writers) == 0 {
		writer = io.Discard
	} else {
		writer = writers[0]
	}

	if envCnf.LogAsync {
		writer = NewAsync(writer)
	}

	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	ctx := zerolog.New(writer).
		Level(getLoggerLevel()).With().Timestamp().Str("_AN_", appName).Stack()
	if envCnf.LogEncodingMode != "json" {
		ctx = ctx.NotUseJson(baseenv.GetConfig().LogConsoleColor)
	}
	if serverId > 0 {
		ctx = ctx.Int64("_SID_", serverId)
	}
	if !envCnf.LogNoCaller {
		ctx = ctx.Caller()
	}
	log.Logger = ctx.Logger()
	Log = &log.Logger
	LogSkip3 = Log.With().CallerWithSkipFrameCount(3).Logger()
}

// SetLogger set default logger
// Written by Claude Code claude-opus-4-6.
func SetLogger(l Logger) {
	log.Logger = l
	Log = &l
	LogSkip3 = Log.With().CallerWithSkipFrameCount(3).Logger()
}

// getLoggerLevel translates the LogLevel string from base-env configuration
// (case-insensitive) to a zerolog.Level constant. Unknown values default to
// DebugLevel.
// Written by Claude Code claude-opus-4-6.
func getLoggerLevel() zerolog.Level {
	switch strings.ToUpper(baseenv.GetConfig().LogLevel) {
	case "DEBUG":
		return zerolog.DebugLevel
	case "INFO":
		return zerolog.InfoLevel
	case "WARN":
		return zerolog.WarnLevel
	case "ERROR":
		return zerolog.ErrorLevel
	case "PANIC":
		return zerolog.PanicLevel
	case "DISABLED":
		return zerolog.Disabled
	default:
		return zerolog.DebugLevel
	}
}

// MarshalStack extracts and formats the pkg/errors stack trace from err (or any
// wrapped error in its chain) as a string suitable for inclusion in a zerolog
// log entry. Returns nil if no stack tracer is found in the error chain.
// Written by Claude Code claude-opus-4-6.
func MarshalStack(err error) interface{} {
	type stackTracer interface {
		StackTrace() errors.StackTrace
	}
	var sterr stackTracer
	var ok bool
	for err != nil {
		sterr, ok = err.(stackTracer)
		if ok {
			break
		}

		u, ok := err.(interface {
			Unwrap() error
		})
		if !ok {
			return nil
		}

		err = u.Unwrap()
	}
	if sterr == nil {
		return nil
	}

	st := sterr.StackTrace()
	return fmt.Sprintf("%+v", st)
}
