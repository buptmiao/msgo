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

func PortToLocalAddr(port int) string {
	return fmt.Sprintf(":%d", port)
}
