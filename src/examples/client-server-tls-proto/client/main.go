package main

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
   "github.com/spikebike/backups-done-right/src/examples/client-server-tls-proto/sum"
	"google.golang.org/protobuf/proto"
	"io"
)

func main() {

	config := &tls.Config{
		InsecureSkipVerify: true, // uncomment this line for local testing without valid SSL certificates
		ServerName: "servername",
	}

	conn, err := tls.Dial("tcp", "localhost:4040", config)
	if err != nil {
		fmt.Printf("client: dial: %s", err)
	}

	if err != nil {
		fmt.Printf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	numbers := &sum.Numbers{
		A: 3,
		B: 5,
	}

	out, err := proto.Marshal(numbers)
	if err != nil {
		fmt.Printf("Failed to encode message: %v", err)
	}

	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[0:], uint32(len(out)))
	conn.Write(buf[0:])
	conn.Write(out)

	_, err = conn.Read(buf[0:])
	if err != nil {
		fmt.Printf("Failed to read response: %v", err)
	}

	length := binary.LittleEndian.Uint32(buf[0:])
	data := make([]byte, length)

	_, err = io.ReadFull(conn, data)
	if err != nil {
		fmt.Printf("Failed to read response: %v", err)
	}

	response := &sum.Sum{}
	if err := proto.Unmarshal(data, response); err != nil {
		fmt.Printf("Failed to parse response: %v", err)
	}

	fmt.Printf("The sum is: %d\n", response.Result)
}
