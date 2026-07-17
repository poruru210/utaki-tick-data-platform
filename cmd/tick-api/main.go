package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tick-data-platform/internal/delivery"
	"tick-data-platform/internal/httpapi"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, output, errorsOut io.Writer) int {
	configPath, ok := configPathFor(args, errorsOut)
	if !ok {
		return 2
	}

	apiConfig, err := httpapi.LoadConfig(configPath)
	if err != nil {
		fmt.Fprintln(errorsOut, "API configuration is invalid")
		return 1
	}
	readerConfig, err := delivery.LoadReaderConfig(apiConfig.ReaderConfig)
	if err != nil {
		fmt.Fprintln(errorsOut, "reader configuration is invalid")
		return 1
	}
	reader, err := delivery.NewArchiveReaderV1(context.Background(), readerConfig)
	if err != nil {
		fmt.Fprintln(errorsOut, "archive reader is unavailable")
		return 1
	}
	handler, err := httpapi.NewHandler(reader, apiConfig)
	if err != nil {
		fmt.Fprintln(errorsOut, "API handler configuration is invalid")
		return 1
	}

	requestTimeout, err := apiConfig.Limits.RequestTimeout()
	if err != nil {
		fmt.Fprintln(errorsOut, "API request timeout is invalid")
		return 1
	}
	serverTimeout := requestTimeout
	const serverTimeoutGrace = 5 * time.Second
	if serverTimeout > time.Duration(1<<63-1)-serverTimeoutGrace {
		serverTimeout = time.Duration(1<<63 - 1)
	} else {
		serverTimeout += serverTimeoutGrace
	}
	server := &http.Server{
		Addr:              apiConfig.ListenAddress,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       serverTimeout,
		WriteTimeout:      serverTimeout,
		IdleTimeout:       60 * time.Second,
	}
	fmt.Fprintf(output, "tick-api listening on %s\n", apiConfig.ListenAddress)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintln(errorsOut, "API shutdown failed")
			return 1
		}
		return 0
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return 0
		}
		fmt.Fprintln(errorsOut, "API server failed")
		return 1
	}
}

func configPathFor(args []string, errorsOut io.Writer) (string, bool) {
	flags := flag.NewFlagSet("tick-api", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	config := flags.String("config", "", "API configuration")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *config == "" {
		fmt.Fprintln(errorsOut, "--config is required")
		return "", false
	}
	return *config, true
}
