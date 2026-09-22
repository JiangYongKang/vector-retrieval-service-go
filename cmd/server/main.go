// Command server runs the local vector retrieval service.
//
// All data stays local: an append-only WAL and atomic snapshots under
// --data-dir. Start it, then use the HTTP API (see README.md) to write,
// delete, search and rebuild.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/highcumontoa/vector-retrieval-service-go/internal/engine"
	"github.com/highcumontoa/vector-retrieval-service-go/internal/server"
)

func main() {
	var (
		addr          = flag.String("addr", "127.0.0.1:8080", "listen address")
		dataDir       = flag.String("data-dir", "./data", "directory for WAL and snapshots (empty disables persistence)")
		dim           = flag.Int("dim", 8, "mandatory vector dimensionality")
		maxRecords    = flag.Int("max-records", 100000, "maximum live records (capacity limit)")
		maxConcurrent = flag.Int("max-concurrent", 64, "maximum concurrent mutating/rebuild ops (backpressure)")
		maxBatch      = flag.Int("max-batch", 1000, "maximum records per write batch")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eng, err := engine.Open(ctx, engine.Config{
		Dir:           *dataDir,
		Dim:           *dim,
		MaxRecords:    *maxRecords,
		MaxConcurrent: *maxConcurrent,
		MaxBatchSize:  *maxBatch,
	})
	if err != nil {
		log.Fatalf("open engine: %v", err)
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           server.New(eng).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("vector retrieval service listening on %s (data-dir=%q dim=%d)", *addr, *dataDir, *dim)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutdown signal received, draining...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	if err := eng.Close(); err != nil {
		log.Printf("engine close: %v", err)
	}
	log.Println("stopped cleanly")
}
