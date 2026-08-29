package main

import (
	"os"

	"github.com/overmindv/parker"
	"github.com/overmindv/tasks/internal/app"
)

// main запускает tasks на каркасе parker.
func main() {
	os.Exit(parker.Main(run, parker.WithAppName("tasks")))
}

// run регистрирует бизнес-логику tasks на каркас parker.
func run(application *parker.App) error {
	return app.Build(application)
}
