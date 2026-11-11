package health

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

type Pinger interface {
	Ping(ctx context.Context) error
}

type Handler struct {
	db           Pinger
	readyTimeout time.Duration
}

func NewHandler(db Pinger, readyTimeout time.Duration) http.Handler {
	if readyTimeout <= 0 {
		readyTimeout = time.Second
	}
	h := Handler{
		db:           db,
		readyTimeout: readyTimeout,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.healthz)
	mux.HandleFunc("/readyz", h.readyz)
	return mux
}

func (h Handler) healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (h Handler) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.readyTimeout)
	defer cancel()

	if err := h.db.Ping(ctx); err != nil {
		slog.Warn("readiness check failed", "error", err)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready\n"))
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}
