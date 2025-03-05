package base_env

import (
	"encoding/json"

	"github.com/kelseyhightower/envconfig"
)

const (
	ServerType      = "SERVER_TYPE"
	ServerId        = "SERVER_ID"
	ConfType        = "CONF_TYPE"
	ConfHosts       = "CONF_HOSTS"
	ConfPath        = "CONF_PATH"
	ConfAuth        = "CONF_AUTH"
	ConfFormat      = "CONF_FORMAT"
	TcpAddr         = "TCP_ADDR"
	LogLevel        = "LOG_LEVEL"
	LogAsync        = "LOG_ASYNC"
	LogUtcTime      = "LOG_UTC_TIME"
	LogTimestamp    = "LOG_TIMESTAMP"
	LogConsole      = "LOG_CONSOLE"
	LogDir          = "LOG_DIR"
	LogMaxSize      = "LOG_MAX_SIZE"
	LogMaxBackup    = "LOG_MAX_BACKUP"
	LogMaxAge       = "LOG_MAX_AGE"
	LogEncodingMode = "LOG_ENCODING_MODE"
	LogNoCaller     = "LOG_NO_CALLER"
	ErrorStackTrace = "ERROR_STACK_TRACE"
	PprofAddr       = "PPROF_ADDR"
	LogConsoleColor = "LOG_CONSOLE_COLOR"
	GameTLS         = "GAME_TLS"
)

type Config struct {
	ServerType string `envconfig:"SERVER_TYPE"` // app name
	ServerId   int64  `envconfig:"SERVER_ID"`   // app server id

	/* ============================== */
	/* ========== config =========== */
	/* ============================== */

	ConfType   string `envconfig:"CONF_TYPE"`   // config type
	ConfHosts  string `envconfig:"CONF_HOSTS"`  // config hosts
	ConfPath   string `envconfig:"CONF_PATH"`   // config path
	ConfAuth   string `envconfig:"CONF_AUTH"`   // config auth user:pass? token?
	ConfFormat string `envconfig:"CONF_FORMAT"` // config format,json? yaml?

	/* ============================== */
	/* ============ network ============= */
	/* ============================== */
	TcpAddr   string `envconfig:"TCP_ADDR"`
	PprofAddr string `envconfig:"PPROF_ADDR"`

	/* ============================== */
	/* ============ logger ============= */
	/* ============================== */

	// LogLevel DEBUG, INFO, WARN, ERROR, PANIC
	// default: DEBUG
	LogLevel string `envconfig:"LOG_LEVEL"`

	// LogAsync async output log data
	LogAsync bool `envconfig:"LOG_ASYNC"`

	// LogUtcTime use utc time or local time
	LogUtcTime bool `envconfig:"LOG_UTC_TIME"`

	// LogTimestamp output log timestamp or time.String()
	LogTimestamp bool `envconfig:"LOG_TIMESTAMP"`

	// LogConsole output console. default stderr. yet can stdout or discard
	LogConsole string `envconfig:"LOG_CONSOLE"`

	LogConsoleColor bool `envconfig:"LOG_CONSOLE_COLOR"`

	// LogDir write log to directory
	LogDir string `envconfig:"LOG_DIR"`
	// LogMaxSize lumberjack Maximum size of a single file
	LogMaxSize int `envconfig:"LOG_MAX_SIZE"`
	// LogMaxBackup lumberjack How many log files are retained at most?
	LogMaxBackup int `envconfig:"LOG_MAX_BACKUP"`

	// LogEncodingMode Log output format default "json" ,yet can "console"
	LogEncodingMode string `envconfig:"LOG_ENCODING_MODE"`

	// LogNoCaller No call line output
	LogNoCaller bool `envconfig:"LOG_NO_CALLER"`
	// ErrorStackTrace output berror caller stack trace
	ErrorStackTrace bool `envconfig:"ERROR_STACK_TRACE"`

	GameTLS bool `envconfig:"GAME_TLS"`
}

var (
	cfg = Config{
		ServerType:      "ravinggo-game",
		ServerId:        0,
		ConfType:        "consul",
		ConfHosts:       "127.0.0.1:8500",
		ConfPath:        "game/",
		ConfAuth:        "",
		ConfFormat:      "",
		TcpAddr:         "80",
		LogLevel:        "debug",
		LogConsole:      "stdout",
		LogDir:          "",
		LogEncodingMode: "0",
		LogNoCaller:     false,
		LogTimestamp:    false,
		LogAsync:        false,
		ErrorStackTrace: false,
		LogConsoleColor: true,
	}
)

func InitConfig() {
	envconfig.MustProcess("", &cfg)
}

func GetConfig() *Config {
	return &cfg
}

func (c *Config) String() string {
	data, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return string(data)
}
