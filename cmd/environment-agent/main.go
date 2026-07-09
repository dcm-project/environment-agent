package main

import (
	"log/slog"
	"os"
)

func main() {
	os.Exit(run())
}

func run() int {
	slog.Info("Environment Agent starting")
	slog.Info("Environment Agent stopped")
	return 0
}
