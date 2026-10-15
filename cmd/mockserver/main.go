// Command mockserver runs the standalone mock downstream TCP server.
// Usage: go run ./cmd/mockserver [-addr :9090] [-fail 0.0]
package main

import (
	"flag"
	"log"

	"github.com/vjy0/job-queue-service/internal/mockserver"
)

func main() {
	addr     := flag.String("addr", ":9090", "TCP listen address")
	failRate := flag.Float64("fail", 0.0, "fraction of jobs to fail [0.0-1.0]")
	flag.Parse()

	srv, err := mockserver.New(*addr, *failRate)
	if err != nil {
		log.Fatalf("[mockserver] failed to start: %v", err)
	}
	srv.Serve() // blocks until Stop() is called or process exits
}
