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

type Config struct {
	HTTPAddr        string
	MySQLDSN        string
	RequestTimeout  time.Duration
	DeliveryTimeout time.Duration
	ShutdownTimeout time.Duration

	WorkerCount  int
	QueueSize    int
	PollInterval time.Duration
}

type fileConfig struct {
	HTTP struct {
		Addr string `yaml:"addr"`
	} `yaml:"http"`
	MySQL struct {
		DSN      string            `yaml:"dsn"`
		User     string            `yaml:"user"`
		Password string            `yaml:"password"`
		Host     string            `yaml:"host"`
		Port     string            `yaml:"port"`
		Database string            `yaml:"database"`
		Params   map[string]string `yaml:"params"`
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

func Load() (*Config, error) {
	raw := defaultFileConfig()

	if err := loadConfigFile(&raw); err != nil {
		return nil, err
	}

	if err := applyEnvOverrides(&raw); err != nil {
		return nil, err
	}

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

func loadConfigFile(cfg *fileConfig) error {
	path := os.Getenv("CONFIG_FILE")
	required := path != ""
	if path == "" {
		path = "config.yml"
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && !required {
			data, err = os.ReadFile("internal/config/config.yml")
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("read config file internal/config/config.yml: %w", err)
			}
			path = "internal/config/config.yml"
		} else {
			return fmt.Errorf("read config file %s: %w", path, err)
		}
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("parse config file %s: %w", path, err)
	}

	return nil
}

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
