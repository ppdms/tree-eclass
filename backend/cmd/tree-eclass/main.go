package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"tree-eclass/internal/app/server"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := server.Run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
