package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/api"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/config"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/service"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/store"
)

func main() {
	cfg, err := config.Load(os.Getenv("HANDOFF_CONFIG"))
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	var st store.Store
	if cfg.DataDir != "" {
		st, err = store.NewFileStore(cfg.DataDir)
	} else {
		st = store.NewMemStore()
	}
	if err != nil {
		log.Fatalf("init store: %v", err)
	}

	svc := service.New(cfg, st)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := svc.Run(ctx); err != nil {
		log.Fatalf("service run: %v", err)
	}

	srv := &http.Server{Addr: cfg.ServerAddr, Handler: api.NewHandler(svc).Routes()}
	go func() {
		log.Printf("listening on %s", cfg.ServerAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down")
	svc.Stop()
	_ = srv.Shutdown(context.Background())
}
