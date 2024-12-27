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
	"gopkg.in/natefinch/lumberjack.v2"

	baseenv "github.com/ravinggo/game/common/base-env"
)

type Logger = zerolog.Logger

var Log *Logger

func init() {
	envCnf := baseenv.GetConfig()
	var writers []io.Writer
	if envCnf.LogConsole != "" {
		if envCnf.LogConsole == "stdout" {
			writers = append(writers, zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
		} else if envCnf.LogConsole == "stderr" {
			writers = append(writers, zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
		} else {
			panic("Console must be stdout or stderr")
		}
	}
	serverId := envCnf.ServerId
	appName := envCnf.AppName
	logDir := envCnf.LogDir
	if logDir != "" {
		fp := filepath.Join(logDir, appName)
		if serverId != 0 {
			filepath.Join(fp, strconv.Itoa(int(serverId)))
		}
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
		writer = zerolog.ConsoleWriter{Out: io.Discard}
	} else {
		writer = writers[0]
	}
	defaultLog := zerolog.New(writer).
		Level(getLoggerLevel()).With().Timestamp().
		Str("appName", appName).Int64("serverId", serverId).
		Stack().Caller().Logger()

	log.Logger = defaultLog
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
