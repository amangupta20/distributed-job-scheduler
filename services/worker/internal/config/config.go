package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds runtime configuration for the worker.
type Config struct {
	DatabaseURL      string
	WorkerName       string
	WorkerVersion    string
	Concurrency      int
	BatchSize        int
	LeaseSeconds     int
	HeartbeatSeconds int
	PollSeconds      int
	DrainSeconds     int
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

// Load reads configuration from environment variables with sensible defaults.
func Load() *Config {
	return &Config{
		DatabaseURL:      getEnv("WORKER_DATABASE_URL", "postgresql://scheduler:scheduler@postgres:5432/scheduler"),
		WorkerName:       getEnv("WORKER_NAME", "pulsequeue-worker"),
		WorkerVersion:    getEnv("WORKER_VERSION", "0.1.0"),
		Concurrency:      getEnvInt("WORKER_CONCURRENCY", 8),
		BatchSize:        getEnvInt("WORKER_BATCH_SIZE", 8),
		LeaseSeconds:     getEnvInt("WORKER_LEASE_SECONDS", 30),
		HeartbeatSeconds: getEnvInt("WORKER_HEARTBEAT_SECONDS", 10),
		PollSeconds:      getEnvInt("WORKER_POLL_SECONDS", 2),
		DrainSeconds:     getEnvInt("WORKER_DRAIN_SECONDS", 20),
	}
}

func (c *Config) LeaseDuration() time.Duration {
	return time.Duration(c.LeaseSeconds) * time.Second
}

func (c *Config) HeartbeatInterval() time.Duration {
	return time.Duration(c.HeartbeatSeconds) * time.Second
}

func (c *Config) PollInterval() time.Duration {
	return time.Duration(c.PollSeconds) * time.Second
}

func (c *Config) DrainTimeout() time.Duration {
	return time.Duration(c.DrainSeconds) * time.Second
}
