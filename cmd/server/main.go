// Command server runs the acceptance-sampling HTTP service.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"sampling-svc/internal/httpapi"
	"sampling-svc/internal/store"
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	dsn := getenv("DATABASE_DSN",
		"postgres://sampling:sampling@localhost:5432/sampling?sslmode=disable")
	addr := getenv("HTTP_ADDR", ":8080")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Wait briefly for Postgres to accept connections on a cold start.
	var st *store.Store
	var err error
	deadline := time.Now().Add(20 * time.Second)
	for {
		st, err = store.Open(ctx, dsn)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			log.Fatalf("connect database: %v", err)
		}
		time.Sleep(time.Second)
	}
	if err := st.Migrate(ctx); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	log.Printf("schema migrated")

	srv := httpapi.NewServer(st)
	log.Printf("listening on %s", addr)
	if err := srv.Start(addr); err != nil {
		log.Fatalf("server: %v", err)
	}
}
