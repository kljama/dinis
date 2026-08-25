package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"dinis/pkg/server"
	"dinis/pkg/store"
)

const version = "1.0.0"

func main() {
	portFlag := flag.Int("port", 8080, "HTTP Web UI and API listen port")
	hostFlag := flag.String("host", "0.0.0.0", "HTTP listen host address")
	dataFlag := flag.String("data", "data/dinis.json", "Path to persistent JSON database file")
	staticFlag := flag.String("static", "", "Optional path to static web assets directory (overrides embedded assets)")
	versionFlag := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *versionFlag {
		fmt.Printf("DINIS ICMP Network Monitor v%s\n", version)
		os.Exit(0)
	}

	// Resolve data directory
	dataPath, err := filepath.Abs(*dataFlag)
	if err != nil {
		log.Fatalf("Invalid data path: %v", err)
	}

	log.Printf("[DINIS] Starting ICMP Network Monitor v%s", version)
	log.Printf("[DINIS] Data store file: %s", dataPath)

	// Initialize persistent store
	st, err := store.NewStore(dataPath)
	if err != nil {
		log.Fatalf("[DINIS] Failed to initialize storage: %v", err)
	}

	// Initialize coordinator (ICMP Engine, Alerts, CIDR manager)
	coord := server.NewCoordinator(st)
	coord.Start()
	defer coord.Stop()

	// Initialize HTTP server
	srvHandler := server.NewServer(coord, *staticFlag)
	addr := fmt.Sprintf("%s:%d", *hostFlag, *portFlag)
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      srvHandler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Background HTTP server listener
	go func() {
		log.Printf("[DINIS] Web Dashboard listening at http://localhost:%d (or http://%s)", *portFlag, addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[DINIS] HTTP server failed: %v", err)
		}
	}()

	// Graceful shutdown on SIGINT / SIGTERM
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("[DINIS] Shutting down gracefully...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("[DINIS] HTTP shutdown error: %v", err)
	}

	log.Println("[DINIS] Service stopped.")
}
