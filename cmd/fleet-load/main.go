// Command fleet-load sends steady traffic at a URL and reports what failed.
// The chaos script runs it while it breaks things, to show whether users noticed.
//
//	fleet-load -url http://localhost:18000/echo/hostname -rps 100 -for 30s
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	Requests int64   `json:"requests"`
	OK       int64   `json:"ok"`
	Non2xx   int64   `json:"non2xx"`
	Errors   int64   `json:"errors"`
	P50ms    float64 `json:"p50_ms"`
	P99ms    float64 `json:"p99_ms"`
	MaxMs    float64 `json:"max_ms"`
}

func main() {
	url := flag.String("url", "", "target URL")
	rps := flag.Int("rps", 50, "requests per second")
	dur := flag.Duration("for", 30*time.Second, "how long to run")
	asJSON := flag.Bool("json", false, "print JSON")
	flag.Parse()
	if *url == "" {
		fmt.Fprintln(os.Stderr, "-url is required")
		os.Exit(2)
	}

	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 64}}
	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	var res result
	var mu sync.Mutex
	var lat []float64
	var wg sync.WaitGroup
	tick := time.NewTicker(time.Second / time.Duration(*rps))
	defer tick.Stop()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-tick.C:
			wg.Add(1)
			go func() {
				defer wg.Done()
				start := time.Now()
				resp, err := client.Get(*url)
				ms := float64(time.Since(start).Microseconds()) / 1000
				atomic.AddInt64(&res.Requests, 1)
				if err != nil {
					atomic.AddInt64(&res.Errors, 1)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode/100 == 2 {
					atomic.AddInt64(&res.OK, 1)
				} else {
					atomic.AddInt64(&res.Non2xx, 1)
				}
				mu.Lock()
				lat = append(lat, ms)
				mu.Unlock()
			}()
		}
	}
	wg.Wait()
	sort.Float64s(lat)
	if n := len(lat); n > 0 {
		res.P50ms = lat[n/2]
		res.P99ms = lat[min(n-1, n*99/100)]
		res.MaxMs = lat[n-1]
	}
	if *asJSON {
		_ = json.NewEncoder(os.Stdout).Encode(res)
		return
	}
	fmt.Printf("requests=%d ok=%d non2xx=%d errors=%d p50=%.1fms p99=%.1fms max=%.1fms\n",
		res.Requests, res.OK, res.Non2xx, res.Errors, res.P50ms, res.P99ms, res.MaxMs)
}
