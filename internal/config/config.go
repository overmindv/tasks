package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	defaultServiceName  = "tasks-it"
	defaultHTTPAddress  = ":8080"
	defaultLogLevel     = "info"
	defaultEnvironment  = "local"
	defaultReadTimeout  = 10 * time.Second
	defaultWriteTimeout = 20 * time.Second
)

type Config struct {
	ServiceName  string
	HTTPAddress  string
	DatabaseURL  string
	LogLevel     string
	Environment  string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Load читает, нормализует и проверяет конфигурацию окружения.
func Load() (Config, error) {
	readTimeout, err := envDuration("READ_TIMEOUT", defaultReadTimeout)
	if err != nil {
		return Config{}, err
	}

	writeTimeout, err := envDuration("WRITE_TIMEOUT", defaultWriteTimeout)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		ServiceName:  env("SERVICE_NAME", defaultServiceName),
		HTTPAddress:  env("HTTP_ADDR", defaultHTTPAddress),
		DatabaseURL:  strings.TrimSpace(os.Getenv("DATABASE_URL")),
		LogLevel:     env("LOG_LEVEL", defaultLogLevel),
		Environment:  env("ENV", defaultEnvironment),
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

// Validate проверяет обязательные и ограниченные настройки сервиса.
func (c Config) Validate() error {
	if c.ServiceName == "" {
		return errors.New("SERVICE_NAME не задан")
	}

	if c.HTTPAddress == "" {
		return errors.New("HTTP_ADDR не задан")
	}

	if c.DatabaseURL == "" {
		return errors.New("DATABASE_URL не задан")
	}

	if err := validateDatabaseURL(c.DatabaseURL); err != nil {
		return fmt.Errorf("DATABASE_URL некорректен: %w", err)
	}

	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf(
			"недопустимый LOG_LEVEL %q: ожидается debug, info, warn или error",
			c.LogLevel,
		)
	}

	switch c.Environment {
	case "local", "development", "test", "staging", "production":
	default:
		return fmt.Errorf(
			"недопустимый ENV %q",
			c.Environment,
		)
	}

	if c.ReadTimeout <= 0 {
		return errors.New("READ_TIMEOUT должен быть больше нуля")
	}

	if c.WriteTimeout <= 0 {
		return errors.New("WRITE_TIMEOUT должен быть больше нуля")
	}

	return nil
}

// validateDatabaseURL проверяет схему, host и имя PostgreSQL database.
func validateDatabaseURL(value string) error {
	parsed, err := url.Parse(value)
	if err != nil {
		return err
	}

	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return fmt.Errorf("неподдерживаемая схема %q", parsed.Scheme)
	}

	if parsed.Host == "" {
		return errors.New("не указан host")
	}

	if strings.TrimPrefix(parsed.Path, "/") == "" {
		return errors.New("не указано имя базы данных")
	}

	return nil
}

// env возвращает очищенную переменную окружения или fallback.
func env(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	return value
}

// envDuration разбирает duration из окружения или возвращает fallback.
func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s должен быть duration, например 10s или 1m: %w", key, err)
	}

	return duration, nil
}
