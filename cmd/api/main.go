package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributedjobplatform/internal/api"
	"distributedjobplatform/internal/observability"
	"distributedjobplatform/internal/storage/postgres"
	"distributedjobplatform/migrations"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Error("DATABASE_URL is required")
		os.Exit(1)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Error("open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)
	startupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(startupCtx); err != nil {
		log.Error("connect database", "error", err)
		os.Exit(1)
	}
	if err := migrations.Apply(startupCtx, db); err != nil {
		log.Error("migrate database", "error", err)
		os.Exit(1)
	}

	metrics := observability.New()
	store := postgres.Store{DB: db, Metrics: metrics}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           (api.Server{Store: store, Ready: db.PingContext, Metrics: metrics, Log: log}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- httpServer.ListenAndServe() }()
	log.Info("API started", "port", port)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	serverFailed := false
	select {
	case sig := <-signals:
		log.Info("shutdown requested", "signal", sig.String())
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP server stopped", "error", err)
			serverFailed = true
		}
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error("HTTP shutdown", "error", err)
	}
	if serverFailed {
		os.Exit(1)
	}
}
