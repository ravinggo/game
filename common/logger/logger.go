package logger

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"gopkg.in/natefinch/lumberjack.v2"

	baseenv "github.com/ravinggo/game/common/base-env"
)

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
	Log    *Logger
	Writer io.Writer
)

func init() {
	envCnf := baseenv.GetConfig()
	if !envCnf.LogTimestamp {
		zerolog.TimeFieldFormat = "2006-01-02T15:04:05.000Z07:00"
	} else {
		zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	}
	var tz *time.Location
	if envCnf.LogUtcTime {
		zerolog.TimestampFunc = func() time.Time {
			return time.Now().UTC()
		}
		tz = time.UTC
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
					writers, zerolog.ConsoleWriter{
						Out: os.Stdout, TimeFormat: zerolog.TimeFieldFormat, TimeLocation: tz,
						FormatCaller: func(i interface{}) string {
							return i.(string)
						},
					},
				)
			} else if envCnf.LogConsole == "stderr" {
				writers = append(
					writers, zerolog.ConsoleWriter{
						Out: os.Stderr, TimeFormat: zerolog.TimeFieldFormat, TimeLocation: tz,
						FormatCaller: func(i interface{}) string {
							return i.(string)
						},
					},
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
	Writer = writer
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	ctx := zerolog.New(writer).
		Level(getLoggerLevel()).With().Timestamp().Str("_AN_", appName).Stack()
	if serverId > 0 {
		ctx = ctx.Int64("_SID_", serverId)
	}
	if !envCnf.LogNoCaller {
		ctx = ctx.Caller()
	}
	log.Logger = ctx.Logger()
	Log = &log.Logger
}

// SetLogger set default logger
func SetLogger(l Logger) {
	log.Logger = l
}

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
