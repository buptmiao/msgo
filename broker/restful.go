package broker

import (
	"net"
	"net/http"
	"time"
)

//ServeHTTP serves the rest requests
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

//ServeMetrics will export metrics, which maybe used by prometheus etc.
func ServeMetrics(w http.ResponseWriter, r *http.Request) {

}

//ServeAPI handle the message service by rest api.
func ServeAPI(w http.ResponseWriter, r *http.Request) {

}
