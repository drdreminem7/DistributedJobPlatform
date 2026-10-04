package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributedjobplatform/internal/observability"
	"distributedjobplatform/internal/storage/postgres"
	"distributedjobplatform/internal/worker"
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

	host, err := os.Hostname()
	if err != nil {
		log.Error("read hostname", "error", err)
		os.Exit(1)
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		log.Error("generate worker ID", "error", err)
		os.Exit(1)
	}
	workerID := fmt.Sprintf("%s-%d-%x", host, os.Getpid(), nonce)
	metrics := observability.New()
	store := postgres.Store{DB: db, Metrics: metrics}
	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", metricsAddr)
	if err != nil {
		log.Error("listen for metrics", "error", err)
		os.Exit(1)
	}
	metricsMux := http.NewServeMux()
	metricsHandler := metrics.Handler()
	metricsMux.Handle("GET /metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		snapshot, err := store.MetricSnapshot(ctx)
		if err != nil {
			http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		metrics.SetSnapshot(snapshot)
		metricsHandler.ServeHTTP(w, r)
	}))
	metricsServer := &http.Server{Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := metricsServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server stopped", "error", err)
		}
	}()
	log.Info("worker metrics listening", "address", listener.Addr().String())
	shutdownTimeout := 20 * time.Second
	if raw := os.Getenv("WORKER_SHUTDOWN_TIMEOUT"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			log.Error("WORKER_SHUTDOWN_TIMEOUT must be a positive duration")
			os.Exit(1)
		}
		shutdownTimeout = parsed
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		(worker.Worker{Store: store, Log: log,
			WorkerID: workerID, Metrics: metrics}).Run(ctx)
	}()
	log.Info("worker started", "worker_id", workerID)
	<-ctx.Done()
	log.Info("worker stopping", "worker_id", workerID)
	select {
	case <-done:
		log.Info("worker stopped", "worker_id", workerID)
	case <-time.After(shutdownTimeout):
		log.Error("worker shutdown timed out", "worker_id", workerID)
		os.Exit(1)
	}
	shutdownCtx, cancelMetrics := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelMetrics()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		log.Error("metrics shutdown", "error", err)
	}
}
