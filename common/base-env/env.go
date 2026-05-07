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
	OpenCheat       = "OPEN_CHEAT"
)

// Config holds all runtime configuration read from environment variables at startup.
// Each field maps to a specific env var (see the envconfig tags). Default values are
// defined in the package-level cfg variable and can be overridden by the environment.
// Written by Claude Code claude-opus-4-6.
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

	OpenCheat bool `envconfig:"OPEN_CHEAT"`
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
		OpenCheat:       false,
	}
)

// InitConfig populates the package-level Config singleton from the current environment
// using kelseyhightower/envconfig. It panics if a required variable is missing or
// cannot be parsed. Call once at program startup before calling GetConfig.
// Written by Claude Code claude-opus-4-6.
func InitConfig() {
	envconfig.MustProcess("", &cfg)
}

// GetConfig returns a pointer to the package-level Config singleton. Callers must not
// mutate the returned value. InitConfig should be called before GetConfig.
// Written by Claude Code claude-opus-4-6.
func GetConfig() *Config {
	return &cfg
}

// String serialises the Config to a JSON string for logging or debugging purposes.
// It panics if JSON marshalling fails, which should never happen for this type.
// Written by Claude Code claude-opus-4-6.
func (c *Config) String() string {
	data, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return string(data)
}
