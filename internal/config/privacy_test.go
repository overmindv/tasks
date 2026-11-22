package config

import (
	"strings"
	"testing"
	"time"
)

func TestPrivacyConfiguration(t *testing.T) {
	t.Parallel()
	if err := (Privacy{}).Validate(); err != nil {
		t.Fatal(err)
	}
	good := Privacy{Token: strings.Repeat("p", 32), ReplayToken: strings.Repeat("r", 32), UsersURL: "http://127.0.0.1:8090"}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Privacy){func(c *Privacy) { c.UsersURL = "http://users:8090" }, func(c *Privacy) { c.ReplayToken = c.Token }, func(c *Privacy) { c.RawRetention = -time.Hour }, func(c *Privacy) { c.Token = "" }, func(c *Privacy) { c.UsersURL = "https://example.com/?token=bad" }} {
		bad := good
		change(&bad)
		if bad.Validate() == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
}
