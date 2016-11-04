package broker

import (
	"github.com/prometheus/client_golang/prometheus"
	"net"
	"net/http"
	"time"
)

//ServeHTTP serves the rest requests
func ServeHTTP(l net.Listener) {

	mux := http.NewServeMux()
	exporter := NewExporter("msgo", GetLocalIP())
	prometheus.Unregister(exporter)
	prometheus.MustRegister(exporter)
	mux.Handle("/metrics", prometheus.Handler())
	mux.HandleFunc("/api", ServeAPI)
	mux.HandleFunc("/", ServeHome)
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

//ServeHome will export metrics, which maybe used by prometheus etc.
func ServeHome(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(`<html>
<head><title>Welcome to Msgo!</title></head>
<body>
<h1>Welcome to Msgo!</h1>
<p><a href='/metrics'>Metrics</a></p>
</body>
</html>
						`))
}

//ServeAPI handle the message service by rest api.
func ServeAPI(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("hello world"))
}
