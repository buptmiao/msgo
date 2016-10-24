package broker

import (
	"net"
	"net/http"
	"time"
)

func ServeHTTP(l net.Listener) {

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", ServeMetrics)
	mux.HandleFunc("/api", ServeAPI)

	srv := &http.Server{
		Handler:        mux,
		ReadTimeout:    2 * time.Second,
		WriteTimeout:   2 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	go func() {
		srv.Serve(l)
		srv.Handler = nil
	}()
}

func ServeMetrics(w http.ResponseWriter, r *http.Request) {

}

func ServeAPI(w http.ResponseWriter, r *http.Request) {

}
