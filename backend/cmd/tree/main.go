package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"tree-eclass/internal/app/workflow"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := workflow.Run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "tree:", err)
		os.Exit(1)
	}
}
