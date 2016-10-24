package broker

import (
	"encoding/binary"
	"fmt"
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
