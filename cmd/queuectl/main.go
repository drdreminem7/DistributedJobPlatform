package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"time"

	"distributedjobplatform/internal/storage/postgres"
	"distributedjobplatform/migrations"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	queue := flag.String("queue", "", "queue name")
	concurrency := flag.Int("concurrency", 0, "maximum active leases; 0 disables the limit")
	rate := flag.Int("rate", 0, "claims per second; 0 disables the limit")
	burst := flag.Int("burst", 0, "token capacity; 0 disables the limit")
	flag.Parse()
	if *queue == "" || *concurrency < 0 || *rate < 0 || *burst < 0 {
		fmt.Fprintln(os.Stderr, "queue is required and limits cannot be negative")
		os.Exit(2)
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "DATABASE_URL is required")
		os.Exit(2)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := migrations.Apply(ctx, db); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	policy := postgres.QueuePolicy{Queue: *queue}
	if *concurrency > 0 {
		policy.ConcurrencyLimit = concurrency
	}
	if *rate > 0 {
		policy.RatePerSecond = rate
	}
	if *burst > 0 {
		policy.Burst = burst
	}
	if err := (postgres.Store{DB: db}).SetQueuePolicy(ctx, policy); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("updated %s\n", *queue)
}
