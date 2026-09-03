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
	"strconv"
	"strings"
	"syscall"
	"time"

	"dinis/pkg/influxdb"
	"dinis/pkg/server"
	"dinis/pkg/store"
)

const version = "1.0.0"

func envOrDefault(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return fallback
}

func main() {
	portFlag := flag.Int("port", envIntOrDefault("DINIS_PORT", 8080), "HTTP Web UI and API listen port (DINIS_PORT)")
	hostFlag := flag.String("host", envOrDefault("DINIS_HOST", "0.0.0.0"), "HTTP listen host address (DINIS_HOST)")
	dataFlag := flag.String("data", envOrDefault("DINIS_DATA", "data/dinis.json"), "Path to persistent JSON database file (DINIS_DATA)")
	staticFlag := flag.String("static", envOrDefault("DINIS_STATIC", ""), "Optional path to static web assets directory (DINIS_STATIC)")
	influxURLFlag := flag.String("influxdb-url", envOrDefault("INFLUXDB3_URL", ""), "InfluxDB 3 Core URL (INFLUXDB3_URL). Empty disables InfluxDB export.")
	influxBucketFlag := flag.String("influxdb-bucket", envOrDefault("INFLUXDB3_BUCKET", "dinis"), "InfluxDB bucket/database name (INFLUXDB3_BUCKET)")
	influxTokenFlag := flag.String("influxdb-token", envOrDefault("INFLUXDB3_TOKEN", ""), "InfluxDB auth token (INFLUXDB3_TOKEN)")
	apiTokenFlag := flag.String("api-token", os.Getenv("DINIS_API_TOKEN"), "Optional API authentication token for REST endpoints (DINIS_API_TOKEN)")
	allowedHostsFlag := flag.String("allowed-hosts", os.Getenv("DINIS_ALLOWED_HOSTS"), "Comma-separated list of allowed HTTP Host header values (DINIS_ALLOWED_HOSTS)")
	allowedClientIPsFlag := flag.String("allowed-client-ips", os.Getenv("DINIS_ALLOWED_CLIENT_IPS"), "Comma-separated list of allowed client IPs/CIDRs for Web UI and API access (DINIS_ALLOWED_CLIENT_IPS)")
	trustedProxiesFlag := flag.String("trusted-proxies", os.Getenv("DINIS_TRUSTED_PROXIES"), "Comma-separated list of trusted proxy IPs/CIDRs or preset ('docker'/'private') permitted to provide X-Forwarded-For/Host (DINIS_TRUSTED_PROXIES)")
	allowedOriginsFlag := flag.String("allowed-origins", os.Getenv("DINIS_ALLOWED_ORIGINS"), "Comma-separated list of allowed CORS origins (DINIS_ALLOWED_ORIGINS)")
	maxMetricHostsFlag := flag.Int("max-metric-hosts", envIntOrDefault("DINIS_MAX_METRIC_HOSTS", 10000), "Maximum monitored hosts metric retention capacity before LRU eviction (DINIS_MAX_METRIC_HOSTS)")
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
	defer st.Close()

	if *maxMetricHostsFlag > 0 {
		settings := st.GetSettings()
		if settings.MaxMetricHosts != *maxMetricHostsFlag {
			settings.MaxMetricHosts = *maxMetricHostsFlag
			_ = st.UpdateSettings(settings)
		}
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
		coord.SetProbeExporter(func(ip, alias, subnet string, latencyMs float64, success bool, ts time.Time) {
			influxWriter.WriteProbe(ip, alias, subnet, latencyMs, success, ts)
		})
		log.Printf("[DINIS] InfluxDB export enabled -> %s (bucket: %s)", *influxURLFlag, *influxBucketFlag)
	}

	coord.Start()
	if influxWriter != nil {
		defer influxWriter.Stop()
	}
	defer coord.Stop()

	// Initialize HTTP server
	srvHandler := server.NewServer(coord, *staticFlag)
	if *apiTokenFlag != "" {
		srvHandler.SetAPIToken(*apiTokenFlag)
		log.Println("[DINIS] API token authentication enabled for REST endpoints")
	}
	if *allowedHostsFlag != "" {
		hosts := strings.Split(*allowedHostsFlag, ",")
		for i := range hosts {
			hosts[i] = strings.TrimSpace(hosts[i])
		}
		srvHandler.SetAllowedHosts(hosts)
		log.Printf("[DINIS] Allowed hosts configured: %v", hosts)
	}
	if *allowedOriginsFlag != "" {
		origins := strings.Split(*allowedOriginsFlag, ",")
		for i := range origins {
			origins[i] = strings.TrimSpace(origins[i])
		}
		srvHandler.SetAllowedOrigins(origins)
		log.Printf("[DINIS] Allowed CORS origins configured: %v", origins)
	}
	if *allowedClientIPsFlag != "" {
		ips := strings.Split(*allowedClientIPsFlag, ",")
		for i := range ips {
			ips[i] = strings.TrimSpace(ips[i])
		}
		srvHandler.SetAllowedClientIPs(ips)
		log.Printf("[DINIS] Client IP filter configured: %v", ips)
	}
	if *trustedProxiesFlag != "" {
		proxies := strings.Split(*trustedProxiesFlag, ",")
		for i := range proxies {
			proxies[i] = strings.TrimSpace(proxies[i])
		}
		srvHandler.SetTrustedProxies(proxies)
		log.Printf("[DINIS] Trusted proxies configured: %v", proxies)
	}

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
