// 配置加载：
//
// 加载顺序：默认值 → config.yml（或 CONFIG_FILE 环境变量指定路径） → 环境变量覆盖
// 环境变量优先级最高，支持 Docker / K8s 部署时无需修改配置文件。
//
// MySQL DSN 支持两种配置方式：
//   - 直接设置 mysql.dsn（完整连接串）
//   - 分开设置 mysql.user / mysql.password / mysql.host / mysql.port / mysql.database（内部用 go-sql-driver 拼装）
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/goccy/go-yaml"
)

// Config 是对外暴露的不可变配置，所有字段已解析为 Go 类型
type Config struct {
	HTTPAddr        string
	MySQLDSN        string        // 完整 MySQL 连接串（内部拼装或直接读取 dsn）
	RequestTimeout  time.Duration // 单个 HTTP 请求的最大处理时间
	DeliveryTimeout time.Duration // 单次 HTTP 投递的超时
	ShutdownTimeout time.Duration // 优雅关闭的总等待时间

	WorkerCount  int           // Worker goroutine 数量
	QueueSize    int           // 内存 channel 缓冲大小
	PollInterval time.Duration // Dispatcher 轮询间隔
}

// fileConfig 是 YAML 文件的原始结构，所有值都是字符串或原始类型
type fileConfig struct {
	HTTP struct {
		Addr string `yaml:"addr"`
	} `yaml:"http"`
	MySQL struct {
		DSN      string            `yaml:"dsn"` // 完整 DSN，设置后忽略其他字段
		User     string            `yaml:"user"`
		Password string            `yaml:"password"`
		Host     string            `yaml:"host"`
		Port     string            `yaml:"port"`
		Database string            `yaml:"database"`
		Params   map[string]string `yaml:"params"` // 额外连接参数，如 parseTime=true
	} `yaml:"mysql"`
	Timeouts struct {
		Request  string `yaml:"request"`
		Delivery string `yaml:"delivery"`
		Shutdown string `yaml:"shutdown"`
	} `yaml:"timeouts"`
	Worker struct {
		Count        int    `yaml:"count"`
		QueueSize    int    `yaml:"queue_size"`
		PollInterval string `yaml:"poll_interval"`
	} `yaml:"worker"`
}

// Load 返回解析完成的 Config，加载优先级：默认值 < YAML 文件 < 环境变量
func Load() (*Config, error) {
	raw := defaultFileConfig()

	if err := loadConfigFile(&raw); err != nil {
		return nil, err
	}

	if err := applyEnvOverrides(&raw); err != nil {
		return nil, err
	}

	// 将字符串解析为 time.Duration
	requestTimeout, err := parseDurationValue("request timeout", raw.Timeouts.Request)
	if err != nil {
		return nil, err
	}

	deliveryTimeout, err := parseDurationValue("delivery timeout", raw.Timeouts.Delivery)
	if err != nil {
		return nil, err
	}

	shutdownTimeout, err := parseDurationValue("shutdown timeout", raw.Timeouts.Shutdown)
	if err != nil {
		return nil, err
	}

	pollInterval, err := parseDurationValue("poll interval", raw.Worker.PollInterval)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		HTTPAddr:        raw.HTTP.Addr,
		MySQLDSN:        mysqlDSN(raw),
		RequestTimeout:  requestTimeout,
		DeliveryTimeout: deliveryTimeout,
		ShutdownTimeout: shutdownTimeout,
		WorkerCount:     raw.Worker.Count,
		QueueSize:       raw.Worker.QueueSize,
		PollInterval:    pollInterval,
	}

	// 启动时校验：缺少关键配置直接报错
	if cfg.MySQLDSN == "" {
		return nil, fmt.Errorf("mysql config is required: set mysql.dsn or mysql.user/mysql.password/mysql.database in config.yml")
	}
	if cfg.WorkerCount <= 0 {
		return nil, fmt.Errorf("worker.count must be greater than 0")
	}
	if cfg.QueueSize <= 0 {
		return nil, fmt.Errorf("worker.queue_size must be greater than 0")
	}

	return cfg, nil
}

// defaultFileConfig 设置所有字段的默认值，后续被 YAML 和环境变量覆盖
func defaultFileConfig() fileConfig {
	var cfg fileConfig
	cfg.HTTP.Addr = ":8080"
	cfg.MySQL.Host = "127.0.0.1"
	cfg.MySQL.Port = "3306"
	cfg.MySQL.Params = map[string]string{"parseTime": "true"}
	cfg.Timeouts.Request = "5s"
	cfg.Timeouts.Delivery = "5s"
	cfg.Timeouts.Shutdown = "10s"
	cfg.Worker.Count = 4
	cfg.Worker.QueueSize = 16
	cfg.Worker.PollInterval = "1s"
	return cfg
}

// mysqlDSN 返回 MySQL 连接字符串
// 优先用 dsn 字段；否则用 user/password/host/port/database 拼装
func mysqlDSN(cfg fileConfig) string {
	if cfg.MySQL.DSN != "" {
		return cfg.MySQL.DSN
	}
	if cfg.MySQL.User == "" || cfg.MySQL.Database == "" {
		return ""
	}

	mysqlCfg := mysqlDriver.NewConfig()
	mysqlCfg.User = cfg.MySQL.User
	mysqlCfg.Passwd = cfg.MySQL.Password
	mysqlCfg.Net = "tcp"
	mysqlCfg.Addr = cfg.MySQL.Host + ":" + cfg.MySQL.Port
	mysqlCfg.DBName = cfg.MySQL.Database
	mysqlCfg.Params = cfg.MySQL.Params
	if mysqlCfg.Params == nil {
		mysqlCfg.Params = map[string]string{}
	}
	mysqlCfg.ParseTime = true

	return mysqlCfg.FormatDSN()
}

// loadConfigFile 按优先级读取 YAML 配置文件：
//  1. CONFIG_FILE 环境变量指定的路径（不存在则报错）
//  2. 当前目录 config.yml
//  3. 未找到配置文件时保留默认值，后续仍可由环境变量覆盖
func loadConfigFile(cfg *fileConfig) error {
	path := os.Getenv("CONFIG_FILE")
	required := path != "" // 如果显式指定了 CONFIG_FILE，文件必须存在
	if path == "" {
		path = "config.yml"
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && !required {
			return nil
		}
		return fmt.Errorf("read config file %s: %w", path, err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("parse config file %s: %w", path, err)
	}

	return nil
}

// applyEnvOverrides 环境变量覆盖 YAML 配置中的对应字段
// 环境变量优先级最高，适用于 Docker / K8s 部署场景
func applyEnvOverrides(cfg *fileConfig) error {
	if v := os.Getenv("HTTP_ADDR"); v != "" {
		cfg.HTTP.Addr = v
	}
	if v := os.Getenv("MYSQL_DSN"); v != "" {
		cfg.MySQL.DSN = v
	}
	if v := os.Getenv("REQUEST_TIMEOUT"); v != "" {
		cfg.Timeouts.Request = v
	}
	if v := os.Getenv("DELIVERY_TIMEOUT"); v != "" {
		cfg.Timeouts.Delivery = v
	}
	if v := os.Getenv("SHUTDOWN_TIMEOUT"); v != "" {
		cfg.Timeouts.Shutdown = v
	}
	if v := os.Getenv("WORKER_COUNT"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("WORKER_COUNT is invalid: %w", err)
		}
		cfg.Worker.Count = parsed
	}
	if v := os.Getenv("QUEUE_SIZE"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("QUEUE_SIZE is invalid: %w", err)
		}
		cfg.Worker.QueueSize = parsed
	}
	if v := os.Getenv("POLL_INTERVAL"); v != "" {
		cfg.Worker.PollInterval = v
	}

	return nil
}

func parseDurationValue(name string, raw string) (time.Duration, error) {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s is invalid: %w", name, err)
	}
	return d, nil
}
