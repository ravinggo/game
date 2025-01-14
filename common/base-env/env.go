package base_env

import (
	"encoding/json"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	ServerType string `envconfig:"ServerType"` // app name
	ServerId   int64  `envconfig:"SERVER_ID"`  // app server id

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
	TcpAddr string `envconfig:"TCP_ADDR"`

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

	// LogDir write log to directory
	LogDir string `envconfig:"LOG_DIR"`
	// LogMaxSize lumberjack Maximum size of a single file
	LogMaxSize int `envconfig:"LOG_MAX_SIZE"`
	// LogMaxBackup lumberjack How many log files are retained at most?
	LogMaxBackup int `envconfig:"LOG_MAX_BACKUP"`

	// LogEncodingMode Log output format default "console" ,yet can "json"
	LogEncodingMode string `envconfig:"LOG_ENCODING_MODE"`

	// LogNoCaller No call line output
	LogNoCaller bool `envconfig:"LOG_NO_CALLER"`
	// ErrorStackTrace output berror caller stack trace
	ErrorStackTrace bool `envconfig:"ERROR_STACK_TRACE"`
}

var (
	cfg = Config{
		ServerType:      "ravinggo-game",
		ServerId:        10,
		ConfType:        "consul",
		ConfHosts:       "127.0.0.1:8500",
		ConfPath:        "game/",
		ConfAuth:        "",
		ConfFormat:      "",
		TcpAddr:         "80",
		LogLevel:        "debug",
		LogConsole:      "stderr",
		LogDir:          "",
		LogEncodingMode: "",
		LogNoCaller:     false,
		LogTimestamp:    false,
		LogAsync:        false,
		ErrorStackTrace: false,
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
