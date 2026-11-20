package suitestore

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestImmutableUpload(t *testing.T) {
	t.Parallel()

	var saved []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/private/suites/id/1/suite.json" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if r.Method == http.MethodGet {
			_, _ = w.Write(saved)

			return
		}
		if r.Header.Get("If-None-Match") != "*" {
			t.Error("missing immutable condition")
		}
		if saved != nil {
			w.WriteHeader(412)

			return
		}
		saved, _ = io.ReadAll(r.Body)
	}))
	defer server.Close()
	store, err := New(Config{Endpoint: server.URL, Region: "us-east-1", Bucket: "private", AccessKey: "test", SecretKey: "test", Prefix: "suites"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := store.PutImmutable(context.Background(), "id/1/suite.json", []byte("payload")); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.PutImmutable(context.Background(), "id/1/suite.json", []byte("changed")); err == nil {
		t.Fatal("overwrote immutable object")
	}
}
