package broker

import (
	"encoding/binary"
	"fmt"
	"net"
)

func itob(id uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, id)
	return b
}

//PortToLocalAddr convert the port to "host:port"
func PortToLocalAddr(port int) string {
	return fmt.Sprintf(":%d", port)
}

//PanicIfErr will panic if err is not nil
func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

//GetLocalIP will return local IP address
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println(err)
	}
	var ip string = "localhost"
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip = ipnet.IP.String()
			}
		}
	}
	return ip
}
