package config

import (
	"errors"
	"net/url"
	"os"
	"strings"
	"time"
)

type Privacy struct {
	ReplayToken, Token, UsersURL   string
	RawRetention, OutcomeRetention time.Duration
}

func LoadPrivacy() (Privacy, error) {
	c := Privacy{ReplayToken: os.Getenv("TASKS_OUTCOME_REPLAY_TOKEN"), Token: os.Getenv("TASKS_PRIVACY_TOKEN"), UsersURL: os.Getenv("TASKS_USERS_URL")}
	var err error
	c.RawRetention, err = envDuration("TASKS_RAW_RETENTION", 0)
	if err != nil {
		return c, err
	}
	c.OutcomeRetention, err = envDuration("TASKS_OUTCOME_RETENTION", 0)
	if err != nil {
		return c, err
	}

	return c, c.Validate()
}
func (c Privacy) Validate() error {
	invalid := errors.New("invalid tasks privacy configuration")
	for _, token := range []string{c.Token, c.ReplayToken} {
		if token != "" && (len(token) < 32 || len(token) > 512 || strings.ContainsFunc(token, func(r rune) bool { return r < 33 || r > 126 })) {
			return invalid
		}
	}
	for _, token := range []string{c.Token, c.ReplayToken} {
		if token == "" {
			continue
		}
		for _, other := range []string{os.Getenv("TASKS_RESOURCE_TOKEN"), os.Getenv("TASK_HUNTER_INGEST_TOKEN"), os.Getenv("TASKS_EXECUTION_METRICS_TOKEN")} {
			if token == other {
				return invalid
			}
		}
	}
	if c.Token != "" && c.Token == c.ReplayToken {
		return invalid
	}
	if (c.Token == "") != (c.UsersURL == "") {
		return invalid
	}
	if c.UsersURL != "" {
		u, err := url.Parse(c.UsersURL)
		if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return invalid
		}
		local := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
		if u.Scheme != "https" && (u.Scheme != "http" || !local) {
			return invalid
		}
	}
	for _, retention := range []time.Duration{c.RawRetention, c.OutcomeRetention} {
		if retention < 0 || retention > 3650*24*time.Hour || (retention > 0 && retention < time.Hour) {
			return invalid
		}
	}

	return nil
}
