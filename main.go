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

	"dinis/pkg/influxdb"
	"dinis/pkg/server"
	"dinis/pkg/store"
)

const version = "1.0.0"

func main() {
	portFlag := flag.Int("port", 8080, "HTTP Web UI and API listen port")
	hostFlag := flag.String("host", "0.0.0.0", "HTTP listen host address")
	dataFlag := flag.String("data", "data/dinis.json", "Path to persistent JSON database file")
	staticFlag := flag.String("static", "", "Optional path to static web assets directory (overrides embedded assets)")
	influxURLFlag := flag.String("influxdb-url", "", "InfluxDB 3 Core URL (e.g. http://localhost:8181). Empty disables InfluxDB export.")
	influxBucketFlag := flag.String("influxdb-bucket", "dinis", "InfluxDB bucket/database name")
	influxTokenFlag := flag.String("influxdb-token", "", "InfluxDB auth token (optional)")
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

	// Wire optional InfluxDB exporter
	var influxWriter *influxdb.Writer
	if *influxURLFlag != "" {
		influxWriter = influxdb.NewWriter(influxdb.Config{
			URL:    *influxURLFlag,
			Bucket: *influxBucketFlag,
			Token:  *influxTokenFlag,
		})
		coord.SetProbeExporter(func(ip string, latencyMs float64, success bool, ts time.Time) {
			influxWriter.WriteProbe(ip, "", "", latencyMs, success, ts)
		})
		log.Printf("[DINIS] InfluxDB export enabled -> %s (bucket: %s)", *influxURLFlag, *influxBucketFlag)
	}

	coord.Start()
	defer coord.Stop()
	if influxWriter != nil {
		defer influxWriter.Stop()
	}

	// Initialize HTTP server
	srvHandler := server.NewServer(coord, *staticFlag)
	addr := fmt.Sprintf("%s:%d", *hostFlag, *portFlag)
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           srvHandler,
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       120 * time.Second,
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
