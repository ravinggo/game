package base_env

import (
	"encoding/json"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	AppName  string `envconfig:"APP_NAME"` // 应用名
	ServerId int64  `envconfig:"SERVER_ID"`

	/* ============================== */
	/* ========== 配置中心 =========== */
	/* ============================== */

	ConfType   string `envconfig:"CONF_TYPE"`   // 配置类型
	ConfHosts  string `envconfig:"CONF_HOSTS"`  // 配置集群地址
	ConfPath   string `envconfig:"CONF_PATH"`   // 配置节点路径
	ConfAuth   string `envconfig:"CONF_AUTH"`   // 配置认证信息
	ConfFormat string `envconfig:"CONF_FORMAT"` // 配置文件格式

	/* ============================== */
	/* ============ 网络 ============= */
	/* ============================== */
	TcpAddr string `envconfig:"TCP_ADDR"`

	/* ============================== */
	/* ============ 日志 ============= */
	/* ============================== */

	// LogLevel DEBUG, INFO, WARN, ERROR, PANIC
	// default: DEBUG
	LogLevel string `envconfig:"LOG_LEVEL"`

	// 日志使用utc时间
	LogUtcTime bool `envconfig:"LOG_UTC_TIME"`

	LogTimestamp bool `envconfig:"LOG_TIMESTAMP"`

	// output console. default stderr. yet can stdout or discard
	LogConsole string `envconfig:"LOG_CONSOLE"`

	LogDir string `envconfig:"LOG_DIR"`
	// lumberjack 文件到多大时进行rotate
	LogMaxSize int `envconfig:"LOG_MAX_SIZE"`
	// lumberjack 最多保留多少个rotate
	LogMaxBackup int `envconfig:"LOG_MAX_BACKUP"`

	// 日志输出格式 默认"console",另有"json"
	LogEncodingMode string `envconfig:"LOG_ENCODING_MODE"`

	LogNotCaller bool `envconfig:"LOG_NOT_CALLER"`
}

var (
	cfg = Config{
		AppName:         "ravinggo-game",
		ServerId:        0,
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
		LogNotCaller:    false,
		LogTimestamp:    false,
	}
)

func init() {
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
