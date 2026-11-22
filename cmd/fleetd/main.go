// Command fleetd runs the control plane: REST API + reconciler.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/api"
	"github.com/Vinaymuniraju/fleet-control-plane/internal/engine"
	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
	"github.com/Vinaymuniraju/fleet-control-plane/internal/reconciler"
	"github.com/Vinaymuniraju/fleet-control-plane/internal/store"
)

func main() {
	addr := flag.String("addr", ":8080", "API listen address")
	kubeconfig := flag.String("kubeconfig", defaultKubeconfig(), "kubeconfig path (empty = in-cluster)")
	resync := flag.Duration("resync", 30*time.Second, "full reconcile interval")
	workers := flag.Int("workers", 4, "parallel reconcile workers")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, log, *addr, *kubeconfig, *resync, *workers); err != nil {
		log.Error("fleetd failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger, addr, kubeconfig string, resync time.Duration, workers int) error {
	cfg, err := kubeConfig(kubeconfig)
	if err != nil {
		return err
	}
	cfg.QPS, cfg.Burst = 50, 100
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	kube := engine.Kube{Client: client}

	st := store.NewMemory()
	engines := engine.Registry{
		model.EngineKong: engine.NewKong(kube),
	}
	rec := reconciler.New(st, engines, reconciler.Options{Workers: workers, Resync: resync, Log: log})
	if err := rec.Watch(ctx, client); err != nil {
		return err
	}
	go rec.Run(ctx)

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.New(st, rec.Enqueue, nil, log).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	log.Info("fleetd listening", "addr", addr, "cluster", cfg.Host)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func kubeConfig(path string) (*rest.Config, error) {
	if path == "" {
		return rest.InClusterConfig()
	}
	return clientcmd.BuildConfigFromFlags("", path)
}

func defaultKubeconfig() string {
	if v := os.Getenv("KUBECONFIG"); v != "" {
		return v
	}
	home, _ := os.UserHomeDir()
	p := filepath.Join(home, ".kube", "config")
	if _, err := os.Stat(p); err == nil {
		return p
	}
	return ""
}
