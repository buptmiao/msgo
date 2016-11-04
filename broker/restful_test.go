package broker_test

import (
	"fmt"
	"github.com/buptmiao/msgo/broker"
	"io/ioutil"
	"net"
	"net/http"
	"testing"
)

func TestServeHTTP(t *testing.T) {
	httpListener, _ := net.Listen("tcp", ":13002")
	broker.ServeHTTP(httpListener)
	for _, url := range []string{fmt.Sprintf("http://%s:13002/", broker.GetLocalIP()),
		fmt.Sprintf("http://%s:13002/metrics", broker.GetLocalIP()),
		fmt.Sprintf("http://%s:13002/api", broker.GetLocalIP())} {

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			panic(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		bytes, _ := ioutil.ReadAll(resp.Body)
		if len(bytes) <= 0 {
			panic("request error")
		}
	}
}
